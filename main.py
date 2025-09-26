import os, json, re, xml.etree.ElementTree as ET, email
from imapclient import IMAPClient
import logging
from datetime import datetime as _dt, timedelta as _td, timezone as _tz
from rooftops import get_rooftop_info


from fortellis import (
    SUB_MAP,
    get_token,
    get_recent_opportunities,   
    get_opportunity,
    get_customer_by_url,
    get_activity_by_url,
    get_activity_by_id_v1,
    send_opportunity_email_activity,
    add_opportunity_comment,
    add_vehicle_sought,
    schedule_activity,
    complete_activity,
)

from gpt import run_gpt
from emailer import send_email


# ── Logging (compact) ────────────────────────────────────────────────
LOG_LEVEL = os.getenv("APP_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("patti")

# === Modes & Safety ==================================================
USE_EMAIL_MODE = False                              # legacy inbox mode off by default
SAFE_MODE = os.getenv("PATTI_SAFE_MODE", "1") in ("1","true","True")  # blocks real customer emails

TEST_FROM = os.getenv("FORTELLIS_TEST_FROM", "sales@claycooleygenesisofmesquite.edealerhub.com")
TEST_TO   = os.getenv("FORTELLIS_TEST_TO",   "rishabhrajendraprasad.shukla@cdk.com")

MICKEY_EMAIL = os.getenv("MICKEY_EMAIL", "knowzek@gmail.com")  # proof recipient
ELIGIBLE_UPTYPES = {s.strip().lower() for s in os.getenv("ELIGIBLE_UPTYPES", "internet").split(",")}

inquiry_text = None  # ensure defined

# === Dealership name → dealer_key (keys must match your SUB_MAP env) ==
DEALERSHIP_TO_KEY = {
    "Tustin Mazda": "tustin-mazda",
    "Huntington Beach Mazda": "huntington-beach-mazda",
    "Tustin Hyundai": "tustin-hyundai",
    "Mission Viejo Kia": "mission-viejo-kia",
}

# === SRP URL bases ===================================================
DEALERSHIP_URL_MAP = {
    "Tustin Mazda": "https://www.tustinmazda.com/used-inventory/",
    "Huntington Beach Mazda": "https://www.huntingtonbeachmazda.com/used-inventory/",
    "Tustin Hyundai": "https://www.tustinhyundai.com/used-inventory/",
    "Mission Viejo Kia": "https://www.missionviejokia.com/used-inventory/",
    "Patterson Auto Group": "https://www.pattersonautos.com/used-inventory/",
}

# === Salesperson & dealership inference (unchanged, trimmed) =========
SALES_PERSON_MAP = {
    "Madeleine": "Madeleine Demo",
    "Pavan": "Pavan Singh",
    "Joe B": "Joe B",
    "Desk Manager 1": "Jim Feinstein",
    "Bloskie, Terry": "Terry Bloskie",
    "Test606, CLB": "Roozbeh",
}
DEALERSHIP_MAP = {
    "Podium": "Tustin Hyundai",
    "Podium Webchat": "Tustin Hyundai",
    "CarNow": "Mission Viejo Kia",
    "Madeleine": "Tustin Mazda",
    "Pavan": "Tustin Hyundai",
    "Joe B": "Huntington Beach Mazda",
    "Bloskie, Terry": "Tustin Hyundai",
    "Test606, CLB": "Tustin Hyundai",
    "Desk Manager 1": "Mission Viejo Kia",
}

CONTACT_INFO_MAP = {
    "Tustin Hyundai":    "Tustin Hyundai, 16 Auto Center Dr, Tustin, CA 92782 | (714) 838-4554 | https://www.tustinhyundai.com/",
    "Mission Viejo Kia": "Mission Viejo Kia, 24041 El Toro Rd, Lake Forest, CA 92630 | (949) 768-7900 | https://www.missionviejokia.com/",
    "Tustin Mazda":      "Tustin Mazda, 28 Auto Center Dr, Tustin, CA 92782 | (714) 258-2300 | https://www.tustinmazda.com/",
    "Huntington Beach Mazda": "Huntington Beach Mazda, 16800 Beach Blvd, Huntington Beach, CA 92647 | (714) 847-7686 | https://www.huntingtonbeachmazda.com/",
    "Patterson Auto Group":   "Patterson Auto Group, 123 Main St, Irvine, CA 92618 | (949) 555-0100 | https://www.pattersonautos.com/",
}

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

# === Start ===========================================================
log.info("Starting GPT lead autoresponder (SAFE_MODE=%s, EMAIL_MODE=%s)",
         os.getenv("PATTI_SAFE_MODE","1"), str(USE_EMAIL_MODE))

# === Pull opportunity leads ======================================================

all_items = []
per_rooftop_counts = {dk: 0 for dk in SUB_MAP}

WINDOW_MIN = int(os.getenv("DELTA_WINDOW_MINUTES", "30"))  
PAGE_SIZE  = int(os.getenv("DELTA_PAGE_SIZE", "500"))

for dealer_key in SUB_MAP.keys():
    token = get_token(dealer_key)

    # Opportunities delta (the base you confirmed in Postman)
    opp_data  = get_recent_opportunities(token, dealer_key,
                                         since_minutes=WINDOW_MIN,
                                         page_size=PAGE_SIZE)
    opp_items = (opp_data or {}).get("items", []) or []
    log.info("API reported opportunity totalItems for %s: %s",
             dealer_key, (opp_data or {}).get("totalItems", "N/A"))

    # Normalize opportunities → your downstream “lead-like” shape
    raw_count = len(opp_items)
    items = []
    
    for op in opp_items:
        up_type = (op.get("upType") or "").lower()
        if up_type not in ELIGIBLE_UPTYPES:
            continue  # skip showroom/phone/etc.
    
        items.append({
            "_dealer_key": dealer_key,
            "opportunityId": op.get("id"),
            "activityId": None,                   # may be None
            "links": op.get("links", []),
            "source": op.get("source"),
            "upType": op.get("upType"),           # <-- keep for later logs/debug
            # carry common fields you already read later:
            "soughtVehicles": op.get("soughtVehicles"),
            "salesTeam": op.get("salesTeam"),
            "customer": op.get("customer"),
            "tradeIns": op.get("tradeIns"),
            "createdBy": op.get("createdBy"),
        })
    
    eligible_count = len(items)
    
    # stamp + tally + aggregate
    for it in items:
        it["_dealer_key"] = dealer_key
    all_items.extend(items)
    per_rooftop_counts[dealer_key] += eligible_count
    
    # logs: show both API total and eligible after filter
    log.info("Eligible opportunities (upType in %s) for %s: %d/%d",
             ",".join(sorted(ELIGIBLE_UPTYPES)), dealer_key, eligible_count, raw_count)


# per-rooftop + total logs (opportunity counts)
for dk in sorted(per_rooftop_counts):
    log.info("Opportunities fetched for %s: %d", dk, per_rooftop_counts[dk])
log.info("Total opportunities fetched: %d", len(all_items))

if not all_items:
    log.info("No opportunities. Exiting.")
    raise SystemExit(0)

# pick the first item for processing
lead = all_items[0]  # keep variable name 'lead' to minimize downstream edits
activity_id = lead.get("activityId")
opportunity_id = lead.get("opportunityId")
dealer_key = lead.get("_dealer_key")
if dealer_key not in SUB_MAP:
    raise KeyError(f"Missing/unknown dealer_key on lead: {dealer_key}. Valid: {list(SUB_MAP.keys())}")
token = get_token(dealer_key)

log.info("Evaluating lead activity=%s opportunity=%s dealer_key=%s", activity_id, opportunity_id, dealer_key)

# === Pull the opportunity & context =================================
if USE_EMAIL_MODE:
    opportunity = {
        "salesTeam": [{"firstName": "Pavan", "lastName": "Singh"}],
        "source": lead.get("source", "Email"),
        "subSource": "",
        "soughtVehicles": [lead.get("vehicle", {})],
        "customer": {"id": "email"},
        "tradeIns": [],
        "createdBy": "Patti Assistant",
    }
    inquiry_text = lead.get("notes", "")
else:
    token = get_token(dealer_key)
    opportunity = get_opportunity(opportunity_id, token, dealer_key)

    # --- Fetch customer email/name if available ---
    # Always define a default so later code doesn't NameError
    customer_email = (lead.get("email_address") or "").strip()
    
    try:
        customer_url = next(
            (l["href"] for l in opportunity.get("customer", {}).get("links", [])
             if l.get("rel") in ("self", "Fetch Customer", "Get Customer")),
            None
        )
        if customer_url:
            customer_data = get_customer_by_url(customer_url, token, dealer_key)
            emails = customer_data.get("emails") or []
    
            # Prefer primary email if flagged, else first non-empty
            email_obj = next((e for e in emails if e.get("isPrimary")), (emails[0] if emails else {}))
            lead["email_address"] = (email_obj.get("address") or "").strip()
    
            # First name
            lead["customer_first"] = (customer_data.get("firstName") or "").strip()
    
    except Exception as e:
        log.warning("Failed to fetch customer info: %s", e)
        # keep any preexisting lead["email_address"] if set; otherwise blank
        lead["email_address"] = (lead.get("email_address") or "").strip()
    
    # Finalize a guaranteed-defined variable
    customer_email = (lead.get("email_address") or "").strip()


    # Inquiry text via activity record

    try:
        activity_url = None
        for link in lead.get("links", []):
            if "activity" in (link.get("title") or "").lower():
                activity_url = link.get("href"); break
    
        activity_id = lead.get("activityId")  # may be None for opp-only
        activity_data = {}
    
        if activity_url:
            activity_data = get_activity_by_url(activity_url, token, dealer_key)
        elif activity_id:
            activity_data = get_activity_by_id_v1(activity_id, token, dealer_key)
        else:
            log.info("No activity link/id on opportunity %s; skipping activity fetch.", opportunity_id)
    
        inquiry_text = (activity_data.get("notes", "") or "")
        if not inquiry_text and "message" in activity_data:
            inquiry_text = extract_adf_comment(activity_data["message"].get("body", ""))
    except Exception as e:
        log.warning("Failed to fetch activity: %s", e)
        inquiry_text = ""


# === Salesperson / dealership mapping ================================
salesperson_obj = opportunity.get("salesTeam", [{}])[0]
first_name = (salesperson_obj.get("firstName") or "").strip()
last_name  = (salesperson_obj.get("lastName") or "").strip()
full_name  = (first_name + " " + last_name).strip()
created_by = opportunity.get("createdBy", "")

salesperson = (
    SALES_PERSON_MAP.get(first_name)
    or SALES_PERSON_MAP.get(full_name)
    or SALES_PERSON_MAP.get(created_by)
    or full_name
    or "our team"
)

source = opportunity.get("source", "")
sub_source = opportunity.get("subSource", "")

dealership = (
    DEALERSHIP_MAP.get(first_name)
    or DEALERSHIP_MAP.get(full_name)
    or DEALERSHIP_MAP.get(source)
    or DEALERSHIP_MAP.get(sub_source)
    or "Patterson Auto Group"
)

# ensure dealer_key aligns with name (only if it maps to a real rooftop key)
mapped = DEALERSHIP_TO_KEY.get(dealership)
if mapped and mapped in SUB_MAP and mapped != dealer_key:
    log.info("Switching dealer_key from %s to %s based on dealership='%s'",
             dealer_key, mapped, dealership)
    dealer_key = mapped

contact_info = CONTACT_INFO_MAP.get(dealership, CONTACT_INFO_MAP["Patterson Auto Group"])

# === Vehicle & SRP link =============================================
vehicle = (opportunity.get("soughtVehicles") or [{}])[0]
make  = vehicle.get("make", "")
model = vehicle.get("model", "")
year  = vehicle.get("yearFrom", "")
trim  = vehicle.get("trim", "")
stock = vehicle.get("stockNumber", "")

vehicle_str = f"{year} {make} {model} {trim}".strip() or "one of our vehicles"
base_url = DEALERSHIP_URL_MAP.get(dealership)
if base_url and (make and model):
    vehicle_str = f'<a href="{base_url}?make={make}&model={model}">{vehicle_str}</a>'

trade_ins = opportunity.get("tradeIns", [])
trade_in = (trade_ins[0].get("make") if trade_ins else "") or ""

customer_name = lead.get("customer_first") or "there"

# === Compose with GPT ===============================================
fallback_mode = not inquiry_text or inquiry_text.strip().lower() in ["", "request a quote", "interested", "info", "information", "looking"]

if fallback_mode:
    prompt = f"""
Your job is to write personalized, dealership-branded emails from Patti, a friendly virtual assistant.
The guest submitted a lead through {source}. They’re interested in: {vehicle_str}. Salesperson: {salesperson}
They didn’t leave a detailed message.

Please write a warm, professional email reply that:
- Begin with exactly `Hi {customer_name},`
- Start with 1–2 appealing vehicle features or dealership Why Buys
- Welcome the guest and highlight our helpfulness
- Invite specific questions or preferences
- Mention the salesperson by name

Dealership Contact Info: {contact_info}
"""
else:
    prompt = f"""
Your job is to write personalized, dealership-branded emails from Patti, a friendly virtual assistant.

When writing:
- Begin with exactly `Hi {customer_name},`
- Lead with value (features / Why Buy)
- If a specific vehicle is mentioned, answer directly and link if possible
- If a specific question exists, answer it first
- Include the salesperson’s name
- Keep it warm, clear, and human

Guest inquiry:
\"\"\"{inquiry_text}\"\"\"

Dealership Contact Info: {contact_info}
"""

# --- Rooftop resolution (must be before run_gpt) ---
# dealer_key here should be the same value you pass into _headers(dealer_key, token)
rt = get_rooftop_info(dealer_key)                   # or pass the Subscription-Id if that’s what you have here
rooftop_name   = rt.get("name")   or "Patterson Auto Group"
rooftop_sender = rt.get("sender") or TEST_FROM     # fallback to your default sender
# rooftop_addr = rt.get("address")                  # optional; run_gpt can append it in the signature

# Generate subject/body with rooftop branding
response  = run_gpt(prompt, customer_name, rooftop_name)
subject   = response["subject"]
body_html = response["body"]


# === Send YOU a copy (proof), not the customer =======================
send_email(to=[MICKEY_EMAIL], subject=subject, body=response["body"])
log.info("Reply email sent to %s", MICKEY_EMAIL)

# === Log to Fortellis (SAFE first) ==================================

post_results = {}

token = get_token(dealer_key)

# 1) Comment (always safe)
try:
    comment_text = "Patti generated a reply (safe mode). Email content stored in comments."
    if not SAFE_MODE:
        comment_text = "Patti generated and sent an intro email (test)."
    r = add_opportunity_comment(token, dealer_key, opportunity_id, comment_text)
    post_results["opportunities_addComment"] = r
    log.info("Added CRM comment: status=%s", r.get("status", "N/A"))
except Exception as e:
    log.error("Add comment failed: %s", e)
    post_results["opportunities_addComment"] = {"error": str(e)}

# 2) Email activity (only if NOT safe; still forced to TEST_TO)
try:
    if not SAFE_MODE:
        # choose recipient correctly
        recipients_list = [TEST_TO]  # test-only recipient in NOT SAFE runs
        act = send_opportunity_email_activity(
            token=token,
            dealer_key=dealer_key,
            opportunity_id=opportunity_id,
            sender=rooftop_sender,
            recipients=recipients_list,         # <-- use test recipient list
            carbon_copies=[],
            subject=subject,
            body_html=body_html,
            rooftop_name=rooftop_name,
        )
        post_results["opportunities_sendEmail"] = act
        log.info("Logged sendEmail activity (test recipient): status=%s", act.get("status", "N/A"))
    else:
        post_results["opportunities_sendEmail"] = {"skipped": "SAFE_MODE"}
        log.info("sendEmail skipped (SAFE_MODE)")
except Exception as e:
    log.error("sendEmail failed: %s", e)
    post_results["opportunities_sendEmail"] = {"error": str(e)}

# 3) Vehicle sought (demo data)
try:
    vs = add_vehicle_sought(
        token, dealer_key, opportunity_id,
        is_new=True, year_from=2023, year_to=2025,
        make=make or "Kia", model=model or "Telluride",
        trim=trim or "SX-Prestige", stock_number=stock or "DEMO-123",
        is_primary=True,
    )
    post_results["opportunities_addVehicleSought"] = vs
    log.info("Added vehicle sought: status=%s", vs.get("status", "N/A"))
except Exception as e:
    log.error("Add vehicle sought failed: %s", e)
    post_results["opportunities_addVehicleSought"] = {"error": str(e)}

# 4) Schedule & Complete follow-up activity
try:
    due_dt_iso = (_dt.now(_tz.utc) + _td(minutes=10)).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    sched = schedule_activity(
        token, dealer_key, opportunity_id,
        due_dt_iso_utc=due_dt_iso, activity_name="Send Email/Letter",
        activity_type=14, comments="Patti demo—schedule a follow-up in ~10 minutes.",
    )
    post_results["activities_schedule"] = sched
    activity_id_new = sched.get("id") or sched.get("activityId")
    log.info("Schedule activity: status=%s", sched.get("status", "N/A"))

    if activity_id_new:
        completed_dt_iso = _dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        comp = complete_activity(
            token, dealer_key, opportunity_id,
            due_dt_iso_utc=due_dt_iso, completed_dt_iso_utc=completed_dt_iso,
            activity_name="Send Email/Letter", activity_type=14,
            comments="Patti demo—completed as proof.", activity_id=activity_id_new,
        )
    else:
        comp = {"skipped": "no activityId from schedule"}
    post_results["activities_complete"] = comp
    log.info("Complete activity: status=%s", comp.get("status", "N/A"))
except Exception as e:
    log.error("Schedule/Complete failed: %s", e)
    post_results["activities_schedule/complete"] = {"error": str(e)}
