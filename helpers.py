import json, os
import re, html as _html
import sys
import xml.etree.ElementTree as ET
from datetime import datetime

import urllib.parse, base64
from datetime import datetime, timedelta, timezone

_SCHED_PARA = re.compile(r"(?is)<p[^>]*>.*?(LegacySalesApptSchLink|schedule (an )?appointment|schedule your visit|reserve your time).*?</p>")

def strip_sched_cta(body_html: str, *, add_resched: bool = True) -> str:
    """Remove any scheduling CTA and (optionally) add a reschedule sentence."""
    if not body_html:
        body_html = ""
    body = _SCHED_PARA.sub("", body_html).strip()
    if add_resched and "<{LegacySalesApptSchLink}>" not in body:
        resched = (
            '<p>If you need to change your time, you can reschedule here: '
            '<{LegacySalesApptSchLink}></p>'
        )
        # insert before footer if present
        idx = body.lower().rfind("</table>")  # naive: many footers end with a table
        if idx != -1:
            return body[:idx] + resched + body[idx:]
        return body + resched
    return body


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
    # Try with microseconds first, fallback if not present
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            pass
    raise ValueError(f"Invalid date format: {date_str}")

def sortActivities(activities, field = "completedDate"):
    activities = sorted(
        activities,
        key=lambda x: parse_date(x[field])
    )
    return activities

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
