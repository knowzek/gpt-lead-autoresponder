import os, json, re, xml.etree.ElementTree as ET, email
from imapclient import IMAPClient
import logging
from datetime import datetime as _dt, timedelta as _td, timezone as _tz
from rooftops import get_rooftop_info
from gpt import run_gpt
from emailer import send_email
import requests
from es_resilient import es_index_with_retry, es_update_with_retry, es_head_exists_with_retry

import re, html as _html

from helpers import rJson, wJson, _html_to_text
from dotenv import load_dotenv
load_dotenv()

from constants import *
from esQuerys import esClient, isIdExist



log = logging.getLogger(__name__)

DRY_RUN = int(os.getenv("DRY_RUN", "1"))  # 1 = DO NOT write to CRM, 0 = allow writes

from fortellis import (
    SUB_MAP,
    get_token,
    get_recent_opportunities,   
    get_customer_by_url,
    get_activities
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

for subscription_id in SUB_MAP.values():   # iterate real Subscription-Ids
    # remove later
    if subscription_id != "7a05ce2c-cf00-4748-b841-45b3442665a7":
        continue

    token = get_token(subscription_id) 

    # Opportunities delta (the base you confirmed in Postman)
    opp_data  = get_recent_opportunities(token, subscription_id,
                                         since_minutes=WINDOW_MIN,
                                         page_size=PAGE_SIZE)

    opp_items = (opp_data or {}).get("items", []) or []
    log.info("API reported opportunity totalItems for %s: %s",
             subscription_id, (opp_data or {}).get("totalItems", "N/A"))

    # Normalize opportunities → your downstream “lead-like” shape
    raw_count = len(opp_items)
    items = []
    
    for op in opp_items:

        up_type = (op.get("upType") or "").lower()
        if up_type not in ELIGIBLE_UPTYPES:
            continue  # skip showroom/phone/etc.

        if not _is_assigned_to_kristin_doc(op):
            continue  # ✅ indent this line

        isExist = es_head_exists_with_retry(
            esClient, index="opportunities", id=opp_id, default=False
        )

        customerID = op.get("customer", {}).get("id", None)
        if customerID and not isExist:
            customerData = get_customer_by_url(f"{CUSTOMER_URL}/{customerID}", token, subscription_id)
        else:
            customerData = op.get("customer", {})

        if not isExist:
            try:
                activities = get_activities(op.get("id"), customerID, token, subscription_id)
            except:
                activities = {}
        
        currDate = _dt.now()
        nextDate = currDate + _td(days=1)

        docToIndex = {
            "_subscription_id": subscription_id,
            "opportunityId": op.get("id"),
            "activityId": None,                   # may be None
            "links": op.get("links", []),
            "source": op.get("source"),
            "upType": op.get("upType"),           # <-- keep for later logs/debug
            # carry common fields you already read later:
            "soughtVehicles": op.get("soughtVehicles"),
            "salesTeam": op.get("salesTeam"),
            "tradeIns": op.get("tradeIns"),
            "createdBy": op.get("createdBy"),
            "updated_at": currDate
        }

        if not isExist:
            docToIndex['customer'] = customerData
            docToIndex['created_at'] = currDate
            docToIndex['isActive'] = True
            # TODO: need to check activities if it will update here
            docToIndex['scheduledActivities'] = activities.get("scheduledActivities", [])
            docToIndex['completedActivities'] = activities.get("completedActivities", [])
            docToIndex['followUP_date'] = nextDate
            docToIndex['followUP_count'] = 0
            docToIndex['messages'] = []
            docToIndex['alreadyProcessedActivities'] = {}
            docToIndex['checkedDict'] = {
                "is_sales_contacted": False,
                "patti_already_contacted": False,
                "last_msg_by": None
            }
            es_index_with_retry(esClient, index="opportunities", id=op.get("id"), document=docToIndex)
        else:
            es_update_with_retry(esClient, index="opportunities", id=op.get("id"), doc=docToIndex)


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
