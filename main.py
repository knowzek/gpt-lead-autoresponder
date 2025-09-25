import os, json, re, xml.etree.ElementTree as ET, email
from imapclient import IMAPClient
import logging

from fortellis import (
    SUB_MAP,                    # mapping {dealer_key: subscription_id}
    get_token,                  # get_token(dealer_key)
    get_recent_leads,           # get_recent_leads(token, dealer_key, ...)
    get_opportunity,            # get_opportunity(opportunity_id, token, dealer_key)
    get_customer_by_url,        # get_customer_by_url(url, token, dealer_key)
    get_activity_by_url,        # get_activity_by_url(url, token, dealer_key)
    get_activity_by_id_v1,      # get_activity_by_id_v1(activity_id, token, dealer_key)
    send_opportunity_email_activity,
    add_opportunity_comment,
    add_vehicle_sought,
    schedule_activity,          # schedule_activity(token, dealer_key, ...)
    complete_activity,          # complete_activity(token, dealer_key, ...)
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
inquiry_text = None  # ensure defined

# === Dealership name → dealer_key (keys must match your SUB_MAP env) ==
DEALERSHIP_TO_KEY = {
    "Tustin Mazda": "tustin-mazda",
    "Huntington Beach Mazda": "hbm-mazda",
    "Tustin Hyundai": "tustin-hyundai",
    "Mission Viejo Kia": "mission-viejo-kia",
    "Patterson Auto Group": "patterson-auto-group",
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

# === Pull leads ======================================================
all_leads = []
if USE_EMAIL_MODE:
    raw_items = fetch_adf_xml_from_gmail(os.getenv("GMAIL_USER"), os.getenv("GMAIL_APP_PASSWORD"))
    # (omitted: same parsing+dealership inference as before)
else:
    # Loop over each dealership subscription and pull recent leads
    for dealer_key in SUB_MAP.keys():
        token = get_token(dealer_key)
        leads = get_recent_leads(token, dealer_key, since_minutes=30)

        items = leads.get("items", [])
        for ld in items:
            if isinstance(ld, dict):
                ld["_dealer_key"] = dealer_key
        all_leads.extend(items) 

log.info("Total leads fetched: %d", len(all_leads))
if not all_leads:
    log.info("No leads. Exiting.")
    raise SystemExit(0)

# For now, process the first lead
lead = all_leads[0]

activity_id = lead.get("activityId")
opportunity_id = lead.get("opportunityId")
dealer_key = lead.get("_dealer_key") or "patterson-auto-group"  # safety default

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

    # Fetch customer email/name if available
    try:
        customer_url = next(
            (l["href"] for l in opportunity.get("customer", {}).get("links", [])
             if l.get("rel") in ("self", "Fetch Customer", "Get Customer")),
            None
        )
        if customer_url:
            customer_data = get_customer_by_url(customer_url, token, dealer_key)
            emails = customer_data.get("emails", [])
            lead["email_address"] = emails[0].get("address", "").strip() if emails else ""
            lead["customer_first"] = (customer_data.get("firstName", "") or "").strip()
    except Exception as e:
        log.warning("Failed to fetch customer info: %s", e)
        lead["email_address"] = ""

    # Inquiry text via activity record
    try:
        activity_url = None
        for link in lead.get("links", []):
            if "activity" in link.get("title", "").lower():
                activity_url = link.get("href"); break
        if activity_url:
            activity_data = get_activity_by_url(activity_url, token, dealer_key)
        else:
            activity_data = get_activity_by_id_v1(activity_id, token, dealer_key)
        inquiry_text = activity_data.get("notes", "") or ""
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

# ensure dealer_key aligns with name (if we got name differently)
dealer_key = DEALERSHIP_TO_KEY.get(dealership, dealer_key)

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

response = run_gpt(prompt, customer_name)
log.info("Composed reply (subject=%s)", response['subject'][:80])

subject = response["subject"].strip()
if subject == "Your vehicle inquiry with Patterson Auto Group":
    subject = f"Your vehicle inquiry with {dealership}"

# === Send YOU a copy (proof), not the customer =======================
send_email(to=[MICKEY_EMAIL], subject=subject, body=response["body"])
log.info("Proof email sent to %s", MICKEY_EMAIL)

# === Log to Fortellis (SAFE first) ==================================
from datetime import datetime as _dt, timedelta as _td
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
        sender_address = TEST_FROM
        recipients_list = [TEST_TO]  # never the real customer in tests
        act = send_opportunity_email_activity(
            token, dealer_key, opportunity_id, sender_address,
            recipients_list, [], f"[PATTI TEST] {subject}",
            response["body"].replace("\n", "<br/>"),
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
    due_dt_iso = (_dt.utcnow() + _td(minutes=10)).replace(microsecond=0).isoformat() + "Z"
    sched = schedule_activity(
        token, dealer_key, opportunity_id,
        due_dt_iso_utc=due_dt_iso, activity_name="Send Email/Letter",
        activity_type=14, comments="Patti demo—schedule a follow-up in ~10 minutes.",
    )
    post_results["activities_schedule"] = sched
    activity_id_new = sched.get("id") or sched.get("activityId")
    log.info("Schedule activity: status=%s", sched.get("status", "N/A"))

    if activity_id_new:
        completed_dt_iso = _dt.utcnow().replace(microsecond=0).isoformat() + "Z"
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

# === Email the proof bundle (concise) ================================
from datetime import datetime as _dtnow
ts_utc = _dtnow.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")

def _get_status(val):
    return val.get("status", "N/A") if isinstance(val, dict) else "N/A"

status_lines = [
    f"addComment: {_get_status(post_results.get('opportunities_addComment', {}))}",
    f"sendEmail: {_get_status(post_results.get('opportunities_sendEmail', {}))}",
    f"addVehicleSought: {_get_status(post_results.get('opportunities_addVehicleSought', {}))}",
    f"schedule: {_get_status(post_results.get('activities_schedule', {}))}",
    f"complete: {_get_status(post_results.get('activities_complete', {}))}",
]

email_body = "\n".join([
    "Fortellis Demo Proof (summary)",
    f"Timestamp (UTC): {ts_utc}",
    f"Dealer Key: {dealer_key}",
    f"Opportunity Id: {opportunity_id}",
    f"Lead Activity Id: {activity_id or 'N/A'}",
    "",
    "=== HTTP Statuses ===",
    *status_lines,
])

send_email(to=[MICKEY_EMAIL], subject=f"Patti Fortellis Demo Proof — Opp {opportunity_id}", body=email_body)
log.info("Proof summary emailed. Run complete for opportunity=%s activity=%s dealer=%s",
         opportunity_id, activity_id, dealer_key)
