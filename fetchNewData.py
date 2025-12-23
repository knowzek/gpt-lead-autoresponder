import os, json, re, xml.etree.ElementTree as ET, email
from imapclient import IMAPClient
import logging
from datetime import datetime as _dt, timedelta as _td, timezone as _tz
from rooftops import get_rooftop_info
from gpt import run_gpt
from emailer import send_email
import requests

import html as _html

from helpers import rJson, wJson, _html_to_text
from dotenv import load_dotenv
load_dotenv()

from constants import *
from airtable_store import upsert_lead, find_by_opp_id, _safe_json_dumps


DRY_RUN = int(os.getenv("DRY_RUN", "1"))  # 1 = DO NOT write to CRM, 0 = allow writes

from fortellis import (
    SUB_MAP,
    get_token,
    get_recent_opportunities,   
    get_customer_by_url,
    get_activities
)

# Accept both classic ICO and ServiceDrive variants
_KBB_SOURCES = {
    "kbb instant cash offer",
    "kbb servicedrive",
}

def _is_exact_kbb_source(val) -> bool:
    return (val or "").strip().lower() in _KBB_SOURCES

def _is_kbb_ico_new_active(doc: dict) -> bool:
    """True if this opp matches KBB ICO: Source=KBB Instant Cash Offer, Status=Active, SubStatus=New, upType=Campaign."""
    def _v(key):
        return (str(doc.get(key) or "").strip().lower())
    return (
        _v("source") == "kbb instant cash offer" and
        _v("status") == "active" and
        _v("upType") == "campaign"
    )

def _is_assigned_to_kristin_doc(doc: dict) -> bool:
    for m in (doc.get("salesTeam") or []):
        fn = (m.get("firstName") or "").strip().lower()
        ln = (m.get("lastName") or "").strip().lower()
        em = (m.get("email") or "").strip().lower()
        if (fn == "kristin" and ln == "nowzek") or em in {"knowzek@pattersonautos.com","knowzek@gmail.com"}:
            return True
    return False

# ── Logging (compact) ────────────────────────────────────────────────
LOG_LEVEL = os.getenv("APP_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("patti")

inquiry_text = None  # ensure defined

# === Email fetcher & parsers (quiet logging) =========================
def fetch_adf_xml_from_gmail(email_address, app_password, sender_filters=None):
    if sender_filters is None:
        sender_filters = [
            "notify@eleadnotify.com",
            "Sales@tustinhyundai.edealerhub.com",
            "sales@missionviejokia.edealerhub.com",
        ]
    results = []
    with IMAPClient("imap.gmail.com", ssl=True) as client:
        client.login(email_address, app_password)
        client.select_folder("INBOX")
        messages = client.search(["UNSEEN"])
        if not messages:
            log.info("No new lead emails found.")
            return []
        for uid, message_data in client.fetch(messages, ["RFC822"]).items():
            msg = email.message_from_bytes(message_data[b"RFC822"])
            from_header = msg.get("From", "").lower()
            if any(sender.lower() in from_header for sender in sender_filters):
                for part in msg.walk():
                    if part.get_content_type() in ["text/plain", "text/html"]:
                        body = part.get_payload(decode=True).decode(errors="ignore")
                        results.append((body.strip(), from_header, uid))
                        break
        for _, _, uid in results:
            client.add_flags(uid, ["\\Seen"])
    return results

def parse_plaintext_lead(body):
    try:
        vehicle_match = re.search(r"Vehicle:\s([^\n<]+)", body)
        name_match    = re.search(r"Name:\s([^\n<]+)", body)
        phone_match   = re.search(r"Phone:\s([^\n<]+)", body)
        email_match   = re.search(r"E-?Mail:\s([^\s<]+)", body)
        comment_match = re.search(r"Comments:\s(.*?)<", body)
        vehicle_parts = vehicle_match.group(1).split() if vehicle_match else []
        year  = vehicle_parts[0] if len(vehicle_parts) > 0 else ""
        make  = vehicle_parts[1] if len(vehicle_parts) > 1 else ""
        model = " ".join(vehicle_parts[2:]) if len(vehicle_parts) > 2 else ""
        return {
            "activityId": "email-lead",
            "opportunityId": "email-opportunity",
            "source": "Email",
            "customerId": "email-customer",
            "links": [],
            "email_first": name_match.group(1).split()[0] if name_match else "Guest",
            "email_last": " ".join(name_match.group(1).split()[1:]) if name_match else "",
            "email_address": email_match.group(1) if email_match else "",
            "email_phone": phone_match.group(1) if phone_match else "",
            "vehicle": {"year": year, "make": make, "model": model, "trim": ""},
            "notes": (comment_match.group(1).strip() if comment_match else ""),
        }
    except Exception as e:
        log.warning("Failed to parse plain text lead: %s", e)
        return None

def extract_adf_comment(adf_xml: str) -> str:
    try:
        root = ET.fromstring(adf_xml)
        comment_el = root.find(".//customer/comments")
        if comment_el is not None and comment_el.text:
            return comment_el.text.strip()
    except Exception as e:
        log.warning("Failed to parse ADF XML: %s", e)
    return ""




# === Pull opportunity leads ======================================================

all_items = []
per_rooftop_counts = {sub_id: 0 for sub_id in SUB_MAP.values()}

# TEMP: only fetch opps for these subscriptions while testing
ALLOWED_SUBSCRIPTIONS = {
    "7a05ce2c-cf00-4748-b841-45b3442665a7",
    "c27d7f4f-4a4c-45c8-8154-a5de48421fc3",
}

for subscription_id in SUB_MAP.values():   # iterate real Subscription-Ids
    if subscription_id not in ALLOWED_SUBSCRIPTIONS:
        continue

    token = get_token(subscription_id) 

    # Opportunities delta (the base you confirmed in Postman)
    opp_data = get_recent_opportunities(token, subscription_id,
                                        since_minutes=WINDOW_MIN,
                                        page_size=PAGE_SIZE)
    
    opp_items = (opp_data or {}).get("items", []) or []
    log.info("API reported opportunity totalItems for %s: %s",
             subscription_id, (opp_data or {}).get("totalItems", "N/A"))
    
    # before the for-loop (keep these)
    raw_count = len(opp_items)
    items = []
    
    for op in opp_items:
        up_type = (op.get("upType") or "").lower()
        is_kbb = _is_exact_kbb_source(op.get("source"))

        # Keep normal upType gate for non-KBB; allow KBB ICO through regardless
        if (not is_kbb) and (up_type not in ELIGIBLE_UPTYPES):
            continue
    
        # Previously only Kristin-assigned; now also allow exact KBB ICO
        if (not _is_assigned_to_kristin_doc(op)) and (not is_kbb):
            continue
    
        # one canonical id everywhere
        opp_id = op.get("opportunityId") or op.get("id")
        if not opp_id:
            continue
    
        # base doc
        now_iso = _dt.now(_tz.utc).isoformat()
        customerID = (op.get("customer") or {}).get("id")
    
        # ensure we have a dict to populate
        docToIndex = {}

        docToIndex["opportunityId"] = opp_id
        docToIndex["id"] = opp_id                     # keep both, your processor checks either
        docToIndex["source"] = op.get("source")       # <-- REQUIRED for KBB routing flags
        docToIndex["_subscription_id"] = subscription_id  # used later when fetching activities, etc.
    
        # always present for processor
        docToIndex.setdefault("messages", [])
        docToIndex.setdefault("checkedDict", {
            "patti_already_contacted": False,
            "last_msg_by": None,
            "is_sales_contacted": False
        })
        docToIndex.setdefault("isActive", True)
        docToIndex.setdefault("status", op.get("status") or "Active")
        docToIndex.setdefault("subStatus", op.get("subStatus") or "New")
        docToIndex.setdefault("substatus", docToIndex["subStatus"])  # alias
        docToIndex.setdefault("upType", op.get("upType"))
        docToIndex.setdefault("uptype", op.get("upType"))            # alias
        docToIndex["updated_at"] = now_iso
        docToIndex.setdefault("created_at", now_iso)
        docToIndex.setdefault("tradeIns", op.get("tradeIns") or [])
        docToIndex.setdefault("salesTeam", op.get("salesTeam") or [])
        docToIndex.setdefault("soughtVehicles", op.get("soughtVehicles") or [])
    
        # KBB: exact match only
        if is_kbb:
            docToIndex["followUP_date"] = now_iso  # due now (so Day 0 runs)
            docToIndex.setdefault("_kbb_state", {
                "mode": "cadence",
                "last_template_day_sent": None,
                "last_template_sent_at": None,
                "last_customer_msg_at": None,
                "last_agent_msg_at": None,
                "nudge_count": 0,
                "last_inbound_activity_id": None,
                "last_appt_activity_id": None,
                "appt_due_utc": None,
                "appt_due_local": None
            })
        else:
            docToIndex["followUP_date"] = (_dt.now(_tz.utc) + _td(days=1)).isoformat()
    
        # ---- Airtable upsert ----
        existing = find_by_opp_id(opp_id)
        created_now = existing is None
        
        # write the base opp blob + index fields Airtable needs
        upsert_lead(opp_id, {
            "subscription_id": subscription_id,
            "source": docToIndex.get("source") or "",
            "is_active": bool(docToIndex.get("isActive", True)),
            "follow_up_at": docToIndex.get("followUP_date"),
            "mode": (docToIndex.get("_kbb_state") or {}).get("mode", ""),
            "opp_json": _safe_json_dumps(docToIndex),
        })
        
        # If created now, optionally hydrate customer+activities then upsert again
        if created_now:
            if customerID:
                try:
                    richer = get_customer_by_url(f"{CUSTOMER_URL}/{customerID}", token, subscription_id) or {}
                    if richer:
                        docToIndex["customer"] = richer
                except Exception as e:
                    log.warning("customer hydrate failed opp_id=%s err=%s", opp_id, e)
        
            completedActivities = []
            try:
                acts = get_activities(opp_id, customerID, token, subscription_id) or {}
                completedActivities = acts.get("items") or acts.get("activities") or []
            except Exception as e:
                log.warning("get_activities failed opp_id=%s err=%s", opp_id, e)
        
            # mirror your init_doc behavior by mutating docToIndex
            docToIndex["completedActivities"] = completedActivities
            docToIndex["scheduledActivities"] = []
            docToIndex["messages"] = []
            docToIndex["alreadyProcessedActivities"] = {}
            docToIndex["checkedDict"] = {
                "is_sales_contacted": False,
                "patti_already_contacted": False,
                "last_msg_by": None,
            }
        
            upsert_lead(opp_id, {
                "subscription_id": subscription_id,
                "source": docToIndex.get("source") or "",
                "is_active": bool(docToIndex.get("isActive", True)),
                "follow_up_at": docToIndex.get("followUP_date"),
                "mode": (docToIndex.get("_kbb_state") or {}).get("mode", ""),
                "opp_json": _safe_json_dumps(docToIndex),
            })

        # keep what we’re sending for logging/return
        items.append(docToIndex)

        # exit()
    
    eligible_count = len(items)
    
    # stamp + tally + aggregate

    all_items.extend(items)
    per_rooftop_counts[subscription_id] += eligible_count
    
    # logs: show both API total and eligible after filter
    log.info("Eligible opportunities (upType in %s) for %s: %d/%d",
             ",".join(sorted(ELIGIBLE_UPTYPES)), subscription_id, eligible_count, raw_count)

# per-rooftop + total logs (opportunity counts)
for dk in sorted(per_rooftop_counts):
    log.info("Opportunities fetched for %s: %d", dk, per_rooftop_counts[dk])
log.info("Total opportunities fetched: %d", len(all_items))

if not all_items:
    log.info("No opportunities. Exiting.")
    raise SystemExit(0)
