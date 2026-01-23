import json, os
import re, html as _html
import sys
import xml.etree.ElementTree as ET
import urllib.parse, base64
from datetime import datetime, timedelta, timezone

# --- KBB Offer extractors -------------------------------------------------

_OFFER_URL_RE = re.compile(r'Offer\s*Link:\s*(https?://[^\s;<"]+)', re.I)
_OFFER_AMT_RE = re.compile(r'Offer\s*Amount:\s*\$?\s*([\d,]+(?:\.\d{2})?)', re.I)
_OFFER_ID_RE  = re.compile(r'OfferID=([a-f0-9-]{36})', re.I)

def _scan_text_for_offer(text: str) -> dict:
    if not text:
        return {}
    url = None; amt = None; oid = None
    m = _OFFER_URL_RE.search(text);  url = m.group(1).strip() if m else None
    m = _OFFER_AMT_RE.search(text);  amt = f"${m.group(1)}".replace("$$","$") if m else None
    if url:
        m = _OFFER_ID_RE.search(url); oid = m.group(1) if m else None
    out = {}
    if url: out["offer_url"] = url
    if amt: out["amount_usd"] = amt
    if oid: out["offer_id"] = oid
    return out

def get_kbb_offer_context_simple(opportunity: dict) -> dict:
    """Returns {'offer_url','amount_usd','offer_id','vehicle'} if found."""
    # 1) If we already have a stored value from ADF or past scans, trust it.
    memo = dict(opportunity.get("_kbb_offer_ctx") or {})
    if memo.get("amount_usd") or memo.get("offer_url"):
        return memo

    # 2) Otherwise, scan completed activities/messages for an offer URL/amount.
    completed = (
        opportunity.get("completedActivities")
        or (opportunity.get("activityHistory") or {}).get("completedActivities")
        or []
    )

    texts = []
    for a in completed:
        texts.append((a.get("comments") or "") + " " + (a.get("notes") or ""))
        msg = a.get("message") or {}
        texts.append((msg.get("body") or "") + " " + (msg.get("subject") or ""))

    found = {}
    for t in texts:
        bits = _scan_text_for_offer(t)
        if bits:
            found.update(bits)
            if found.get("offer_url"):
                break

    # 3) Build a vehicle string if we have trade info
    ti = (opportunity.get("tradeIns") or [{}])[0] if (opportunity.get("tradeIns") or []) else {}
    veh = " ".join(
        filter(
            None,
            [
                str(ti.get("year") or "").strip(),
                str(ti.get("make") or "").strip(),
                str(ti.get("model") or "").strip(),
            ],
        )
    ).strip()
    if veh:
        found["vehicle"] = veh

    # 4) Merge with any memo we already had, instead of wiping it.
    if memo:
        memo.update(found)
        found = memo

    opportunity["_kbb_offer_ctx"] = found

    # ✅ Persist KBB memo into its own Airtable column (no opp_json / no patti_json changes)
    try:
        from airtable_store import patch_by_id

        # Avoid repeat PATCHes in the same run
        if opportunity.get("_kbb_offer_ctx_saved") == found:
            return found

        rec_id = opportunity.get("_airtable_rec_id")
        if rec_id and found:
            patch_by_id(rec_id, {
                "kbb_offer_ctx": json.dumps(found, ensure_ascii=False),
            })
            opportunity["_kbb_offer_ctx_saved"] = dict(found)
    except Exception as e:
        import logging
        logging.getLogger("patti.kbb").warning(
            "Failed to persist kbb_offer_ctx opp=%s err=%s",
            opportunity.get("opportunityId") or opportunity.get("id"),
            e,
        )
    return found



def build_kbb_ctx(opportunity: dict) -> dict:
    """Small, consistent payload we can hand to run_gpt."""
    facts = get_kbb_offer_context_simple(opportunity)
    return {
        "offer_valid_days": 7,
        "exclude_sunday": True,
        "offer_url": facts.get("offer_url") or "",
        "amount_usd": facts.get("amount_usd") or "",
        "vehicle": facts.get("vehicle") or ""
    }




def rewrite_sched_cta_for_booked(body_html: str) -> str:
    if not body_html:
        return ""

    replacements = [
        # Anything that invites scheduling before a link
        (
            r"(?is)(let\s+me\s+know.*?(schedule|book|reserve).*?:)",
            "If you need to reschedule your appointment, you can do so here:"
        ),
        (
            r"(?is)(schedule\s+(your\s+)?(appointment|visit).*?:)",
            "If you need to reschedule your appointment, you can do so here:"
        ),
        (
            r"(?is)(please\s+let\s+us\s+know.*?:)",
            "If you need to reschedule your appointment, you can do so here:"
        ),
    ]

    new_html = body_html
    for pattern, repl in replacements:
        new_html = re.sub(pattern, repl, new_html)

    return new_html

def _fmt_utc_range(start_iso_utc: str, minutes: int = 30):
    # Input: 'YYYY-MM-DDTHH:MM:SSZ' or ISO with tz
    dt = datetime.fromisoformat(start_iso_utc.replace("Z","+00:00")).astimezone(timezone.utc)
    end = dt + timedelta(minutes=minutes)
    # Google/Outlook/Yahoo want basic format: YYYYMMDDTHHMMSSZ
    def z(s: datetime): return s.strftime("%Y%m%dT%H%M%SZ")
    return z(dt), z(end)

def build_calendar_links(summary: str, description: str, location: str, start_iso_utc: str, duration_min: int = 30):
    s, e = _fmt_utc_range(start_iso_utc, duration_min)
    q = urllib.parse.quote

    google = (
        "https://calendar.google.com/calendar/render?action=TEMPLATE"
        f"&text={q(summary)}"
        f"&dates={s}/{e}"
        f"&details={q(description)}"
        f"&location={q(location)}"
    )
    outlook = (
        "https://outlook.live.com/calendar/0/deeplink/compose?"
        "path=/calendar/action/compose&rru=addevent"
        f"&subject={q(summary)}"
        f"&startdt={s}"
        f"&enddt={e}"
        f"&body={q(description)}"
        f"&location={q(location)}"
    )
    yahoo = (
        "https://calendar.yahoo.com/?v=60&view=d&type=20"
        f"&title={q(summary)}"
        f"&st={s}"
        f"&et={e}"
        f"&desc={q(description)}"
        f"&in_loc={q(location)}"
    )
    return {"google": google, "outlook": outlook, "yahoo": yahoo}

def build_ics_text(uid: str, summary: str, description: str, location: str, start_iso_utc: str, duration_min: int = 30, organizer_email: str = ""):
    dt = datetime.fromisoformat(start_iso_utc.replace("Z","+00:00")).astimezone(timezone.utc)
    end = dt + timedelta(minutes=duration_min)
    def z(s: datetime): return s.strftime("%Y%m%dT%H%M%SZ")
    org = f"ORGANIZER:MAILTO:{organizer_email}\n" if organizer_email else ""
    return (
        "BEGIN:VCALENDAR\n"
        "PRODID:-//Patterson Auto Group//Patti//EN\n"
        "VERSION:2.0\n"
        "CALSCALE:GREGORIAN\n"
        "METHOD:REQUEST\n"
        "BEGIN:VEVENT\n"
        f"UID:{uid}\n"
        f"DTSTAMP:{z(datetime.utcnow().replace(tzinfo=timezone.utc))}\n"
        f"DTSTART:{z(dt)}\n"
        f"DTEND:{z(end)}\n"
        f"SUMMARY:{summary}\n"
        f"DESCRIPTION:{description}\n"
        f"LOCATION:{location}\n"
        f"{org}"
        "END:VEVENT\n"
        "END:VCALENDAR\n"
    )

def parse_date(date_str):
    """
    Robust date parser for activity sorting.
    If date_str is None/blank/unparseable, return a far-future datetime so it sorts last.
    """
    if not date_str:
        return datetime.max.replace(tzinfo=timezone.utc)

    # If Fortellis ever returns a datetime object directly
    if isinstance(date_str, datetime):
        return date_str if date_str.tzinfo else date_str.replace(tzinfo=timezone.utc)

    # Normalize to string
    s = str(date_str).strip()
    if not s:
        return datetime.max.replace(tzinfo=timezone.utc)

    # Common formats you may already have here — keep yours, just wrap safely
    fmts = [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%Y",
    ]

    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        except Exception:
            pass

    # Last resort: sort it last
    return datetime.max.replace(tzinfo=timezone.utc)


def sortActivities(activities, field="completedDate"):
    activities = activities or []
    return sorted(
        activities,
        key=lambda x: parse_date((x or {}).get(field))
    )



def findActivityByType(activities, typeNo):
    for act in activities:
        if act['activityType'] == typeNo:
            return act
    return None

def getFirstActivity(activities):
    
    if len(activities) == 0:
        return None
    
    act = None
    try:    
        act = findActivityByType(activities, 13)
    except:
        pass

    if not act:
        activities = sortActivities(activities)
        act = activities[0]
    
    return act

 

def get_names_in_dir(path):
    return os.listdir(path)

def adf_to_dict(xml_string):
    """Convert ADF XML string into a nested Python dict."""
    
    def element_to_dict(elem):
        node = {}
        
        # Include element attributes if any
        if elem.attrib:
            node["@attributes"] = elem.attrib
        
        # Process child elements
        children = list(elem)
        if children:
            child_dict = {}
            for child in children:
                child_data = element_to_dict(child)
                tag = child.tag

                # Handle repeated tags (e.g., multiple <phone>)
                if tag in child_dict:
                    if isinstance(child_dict[tag], list):
                        child_dict[tag].append(child_data)
                    else:
                        child_dict[tag] = [child_dict[tag], child_data]
                else:
                    child_dict[tag] = child_data

            node.update(child_dict)
        else:
            # If no children, set text value (strip spaces)
            text = (elem.text or "").strip()
            if text:
                node["#text"] = text

        return node

    # Parse the XML
    root = ET.fromstring(xml_string)

    # Convert entire XML structure
    return {root.tag: element_to_dict(root)}

def getInqueryUsingAdf(adfDict: dict):
    inqueryTextBody = adfDict.get('adf', {}) \
        .get('prospect', {}).get('customer', {}) \
        .get('comments', {}).get('#text', None)
    if not inqueryTextBody:
        inqueryTextBody = adfDict.get('ProcessSalesLead', {}) \
            .get('ProcessSalesLeadDataArea', {}).get('SalesLead', {}) \
            .get('SalesLeadHeader', {}).get('CustomerComments', {}) \
            .get('#text', None)
        
    return inqueryTextBody




def getBugLine():
    frame = sys._getframe(1)
    filePath = frame.f_code.co_filename
    if '\\' in filePath:
        fileName = filePath.split('\\')[-1]
    else:
        fileName = filePath.split('/')[-1]
        pass

    lineNo = frame.f_lineno

    return fileName, lineNo

def _html_to_text(h: str) -> str:
    if not h: return ""
    # line breaks
    h = re.sub(r'(?i)<br\s*/?>', '\n', h)
    h = re.sub(r'(?is)<p[^>]*>', '', h)
    h = re.sub(r'(?i)</p>', '\n\n', h)
    # strip tags
    h = re.sub(r'(?is)<[^>]+>', '', h)
    # unescape entities
    return _html.unescape(h).strip()





def wJson(jsonFile, filePath):
    def default(o):
        if isinstance(o, datetime):
            return o.isoformat()  # preserves exact timestamp for ES
        return o
    with open(filePath, 'w', encoding='utf-8') as jsonWriter:
        json.dump(jsonFile, jsonWriter, ensure_ascii=False, indent=4, default=default)

def rJson(filePath):
    with open(filePath, encoding='utf-8') as jsonReader:
        return json.load(jsonReader)
    
def rCsvToDict(filePath):
    newData = []
    with open(filePath, encoding='utf-8') as csvReader:
        csvData = csvReader.readlines()
        dictKeys = csvData[0].replace("\n", "").split(',')
        for data in csvData[1:]:
            data = data.replace("\n", "")
            dictValues = data.split(',')
            tmpDict = {key : dictValues[i] for i, key in enumerate(dictKeys)}
            newData.append(tmpDict)
    return newData

    
def newFolderCreate(folder_name,dPath):
    complete_path = os.path.join(dPath, folder_name)
    if not (os.path.exists(complete_path) and os.path.isdir(complete_path)):
        new_directory = os.path.join(dPath, folder_name)
        os.makedirs(new_directory)



if __name__ == "__main__":
    data = rJson('jsons/newOPPs/6f3636eb-3eac-f011-814f-00505690ec8c.json')
    
    print(getInqueryUsingAdf(data['firstActivity']['adfDict']))
    pass
