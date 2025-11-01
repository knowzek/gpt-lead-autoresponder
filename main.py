import os, json, re, xml.etree.ElementTree as ET, email
from imapclient import IMAPClient
import logging
from datetime import datetime as _dt, timedelta as _td, timezone as _tz
from rooftops import get_rooftop_info
from gpt import run_gpt
from emailer import send_email
import requests
from inventory_matcher import recommend_from_xml
from fortellis import get_vehicle_inventory_xml  # we’ll add this helper next
import re, html as _html
from kbb_ico import process_kbb_ico_lead

log = logging.getLogger(__name__)

DRY_RUN = int(os.getenv("DRY_RUN", "1"))  # 1 = DO NOT write to CRM, 0 = allow writes

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
    search_activities_by_opportunity,  # <-- add this
)

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

# Cache Fortellis tokens per Subscription-Id so we don’t re-auth every lead
_token_cache = {}
def _get_token_cached(subscription_id: str):
    tok = _token_cache.get(subscription_id)
    if not tok:
        tok = get_token(subscription_id)
        _token_cache[subscription_id] = tok
    return tok

def maybe_call(fn, *args, **kwargs):
    if DRY_RUN:
        # keep payloads visible in logs for testing
        try:
            preview = { "args": args, "kwargs": kwargs }
        except Exception:
            preview = "...payload not serializable..."
        log.info(f"[DRY_RUN] Skipping {fn.__name__} with {preview}")
        return {"ok": True, "dry_run": True}
    return fn(*args, **kwargs)


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

MICKEY_EMAIL = os.getenv("MICKEY_EMAIL")  # proof recipient
ELIGIBLE_UPTYPES = {s.strip().lower() for s in os.getenv("ELIGIBLE_UPTYPES", "internet").split(",")}
PROOF_RECIPIENTS = [
    "knowzek@gmail.com",
    "mickeyt@the-dms.com",
    "dev.almousa@gmail.com"
]

# Preferred activity type + rooftop-safe fallback
ACTIVITY_COMBOS = [
    ("Send Email", 3),          # try first
    ("Send Email/Letter", 14),  # fallback
]

# Per-run cache: sub_id -> (name, type) that succeeded
GOOD_ACTIVITY_COMBO: dict[str, tuple[str, int]] = {}

PATTI_FIRST_REPLY_SENTINEL = "[patti:first-reply]"

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
per_rooftop_counts = {sub_id: 0 for sub_id in SUB_MAP.values()}

WINDOW_MIN = int(os.getenv("DELTA_WINDOW_MINUTES", "30"))  
PAGE_SIZE  = int(os.getenv("DELTA_PAGE_SIZE", "500"))

for subscription_id in SUB_MAP.values():   # iterate real Subscription-Ids
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
    
        items.append({
            "_subscription_id": subscription_id,
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

# pick the first item for processing
lead = all_items[0]
activity_id = lead.get("activityId")
opportunity_id = lead.get("opportunityId")
subscription_id = lead.get("_subscription_id")
if not subscription_id:
    raise KeyError("Lead missing _subscription_id")
token = get_token(subscription_id)

log.info("Evaluating lead activity=%s opportunity=%s subscription_id=%s",
         activity_id, opportunity_id, subscription_id)


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
    # ---- Idempotency: has Patti already first-replied to this opportunity? ----
    already_contacted = False
    try:
        recent_acts = search_activities_by_opportunity(
            opportunity_id, token, subscription_id, page=1, page_size=50
        )
        for act in recent_acts:
            comments = (act.get("comments") or "")
            name     = (act.get("activityName") or "").strip().lower()
            a_type   = act.get("activityType")

            # 1) Our sentinel in any prior comment?
            if PATTI_FIRST_REPLY_SENTINEL in comments:
                already_contacted = True
                break

            # 2) Any prior email activity on the opp (conservative)
            if ("send email" in name) or (a_type == 3):
                already_contacted = True
                break
    except Exception as e:
        log.warning("Activity search failed (continuing defensively): %s", e)

    if already_contacted:
        log.info("Skipping first reply: prior contact detected for opportunity %s", opportunity_id)
        raise SystemExit(0)   # if later you loop multiple leads, change this to:  continue
        
    opportunity = get_opportunity(opportunity_id, token, subscription_id)

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
            customer_data = get_customer_by_url(customer_url, token, subscription_id)
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
            activity_data = get_activity_by_url(activity_url, token, subscription_id)
        elif activity_id:
            activity_data = get_activity_by_id_v1(activity_id, token, subscription_id)
        else:
            log.info("No activity link/id on opportunity %s; skipping activity fetch.", opportunity_id)
    
        inquiry_text = (activity_data.get("notes", "") or "")
        if not inquiry_text and "message" in activity_data:
            inquiry_text = extract_adf_comment(activity_data["message"].get("body", ""))

    except Exception as e:
        log.warning("Failed to fetch activity: %s", e)
        inquiry_text = ""

# === Persona routing: KBB ICO vs General ==============================
src = (opportunity.get("source") or lead.get("source") or "").lower()
is_kbb_ico = (
    src.startswith("kbb ico")
    or "kbb instant cash offer" in src
    or "kelley blue book" in src
)

lead_age_days = 0
created_raw = opportunity.get("createdDate") or lead.get("createdDate")
try:
    if created_raw:
        created_dt = _dt.fromisoformat(created_raw.replace("Z", "+00:00"))
        lead_age_days = (_dt.now(_tz.utc) - created_dt).days
except Exception:
    pass


mode = "kbb_ico" if is_kbb_ico else "general"
opportunity["mode"] = mode
opportunity["lead_age_days"] = lead_age_days

log.info("Persona route: mode=%s lead_age_days=%s src=%s", mode, lead_age_days, src[:120])


# --- Rooftop resolution (from Subscription-Id) ---
rt = get_rooftop_info(subscription_id)
rooftop_name   = rt.get("name")   or "Patterson Auto Group"
rooftop_sender = rt.get("sender") or TEST_FROM
rooftop_addr   = rt.get("address") or ""
log.info("Resolved rooftop: sub_id=%s name=%s", subscription_id, rooftop_name)



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

# === Skip MVK Bucket leads: forward internally via sendEmail =========
if "mvk bucket" in (source or "").lower() or "mvk bucket" in (sub_source or "").lower():
    rt = get_rooftop_info(subscription_id)
    rooftop_name   = rt.get("name")   or rooftop_name
    rooftop_sender = rt.get("sender") or rooftop_sender

    MVK_FORWARD_MAP = {
        "Mission Viejo Kia": "knowzek@gmail.com",
        "Tustin Mazda": "knowzek@gmail.com",
        "Tustin Hyundai": "knowzek@gmail.com",
        "Huntington Beach Mazda": "knowzek@gmail.com",
        "Tustin Kia": "knowzek@gmail.com",
    }
    fwd_to = MVK_FORWARD_MAP.get(rooftop_name, MICKEY_EMAIL)

    try:
        subj = f"[MVK BUCKET] {rooftop_name} — Opportunity {opportunity_id}"
        body = (
            f"This lead was identified as an MVK Bucket Lead and was NOT handled by Patti.\n\n"
            f"Opportunity ID: {opportunity_id}\n"
            f"Source: {source}\nSubSource: {sub_source}\n"
            f"Rooftop: {rooftop_name}\n\n"
            "Please follow up directly."
        )
        _ = maybe_call(send_opportunity_email_activity,
            token=token,
            dealer_key=subscription_id,
            opportunity_id=opportunity_id,
            sender=rooftop_sender,
            recipients=[fwd_to],              # 👈 internal-only
            carbon_copies=[MICKEY_EMAIL],     # optional: keep Mickey in the loop
            subject=subj,
            body_html=body.replace("\n", "<br>"),
            rooftop_name=rooftop_name,
        )
        log.info("Forwarded MVK bucket lead via sendEmail API to %s", fwd_to)
    except Exception as e:
        log.error("MVK forward failed: %s", e)

    raise SystemExit(0) 

dealership = (
    DEALERSHIP_MAP.get(first_name)
    or DEALERSHIP_MAP.get(full_name)
    or DEALERSHIP_MAP.get(source)
    or DEALERSHIP_MAP.get(sub_source)
    or rooftop_name
)

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

# --- KBB ICO short-circuit -------------------------------------------
if opportunity.get("mode") == "kbb_ico":
    process_kbb_ico_lead(
        opportunity=opportunity,
        lead_age_days=opportunity.get("lead_age_days", 0),
        rooftop_name=rooftop_name,
        inquiry_text=inquiry_text,
        token=token,
        subscription_id=subscription_id,
        SAFE_MODE=SAFE_MODE,
    )

    raise SystemExit(0)


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

Do not include any signature, dealership contact block, address, phone number, or URL in your reply; I will append it.

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

Do not include any signature, dealership contact block, address, phone number, or URL in your reply; I will append it.
"""
# === Inventory recommendations =====================================

# Get live inventory XML
try:
    inventory_xml = get_vehicle_inventory_xml("Patterson2", "FjX^PGwk63", "ZE", "ZE7")
except Exception as e:
    log.warning(f"❌ Could not retrieve inventory XML: {e}")
    inventory_xml = None

# 🔁 Use the same inquiry text you already computed.
# If it's empty (fallback mode), feed a lightweight hint from the parsed vehicle fields.
if inquiry_text and inquiry_text.strip():
    customer_email_text = inquiry_text
else:
    # minimal hint so the matcher can still try (e.g., "Honda Pilot 2021 SUV")
    hint_bits = [str(year or "").strip(), (make or "").strip(), (model or "").strip(), (trim or "").strip()]
    customer_email_text = " ".join([b for b in hint_bits if b]) or "SUV car"

recommendation_text = ""
if inventory_xml and customer_email_text:
    try:
        recommendation_text = recommend_from_xml(inventory_xml, customer_email_text).strip()
        if recommendation_text:
            prompt += f"\n\nInventory suggestions to include:\n{recommendation_text}\n"
            log.info("✅ Added inventory suggestions to prompt.")
    except Exception as e:
        log.warning(f"Recommendation failed: {e}")

# === Generate subject/body with rooftop branding ====================
response  = run_gpt(prompt, customer_name, rooftop_name)
subject   = response["subject"]
body_html = response["body"]

body_html = re.sub(
    r"(?is)(?:\n\s*)?patti\s*(?:\r?\n)+virtual assistant.*?$",
    "",
    body_html
)

# === Send an SMTP proof directly (no CRM write) =====================
SEND_SMTP_PROOF = int(os.getenv("SEND_SMTP_PROOF", "1"))  # 1=send, 0=skip during testing

PROOF_FROM = os.getenv("PROOF_FROM", rooftop_sender)  # use rooftop sender as default
PROOF_TO   = [os.getenv("PROOF_TO", "knowzek@gmail.com")]  # 👈 set your address here (or via env)
PROOF_CC   = [os.getenv("MICKEY_EMAIL", "knowzek@gmail.com")]  # Mickey

proof_subject = f"[Patti Proof] {subject}"
proof_text = _html_to_text(
    body_html
    + "<br><br><hr><p><em>Internal proof only — not sent to the customer.</em></p>"
    + f"<p style='color:#888;font-size:12px'>{contact_info}</p>"
)

if SEND_SMTP_PROOF:
    if DRY_RUN:
        # Allow SMTP even during DRY_RUN (since DRY_RUN only blocks CRM writes)
        # If you want DRY_RUN to block SMTP as well, wrap this in another if.
        pass
    try:
        send_email(
            to=PROOF_TO + [e for e in PROOF_CC if e],
            subject=proof_subject,
            body=proof_text,
        )
        log.info("✅ SMTP proof sent to %s (cc %s)", PROOF_TO, PROOF_CC)
    except Exception as e:
        log.error("❌ SMTP proof send failed: %s", e)
else:
    log.info("SMTP proof sending disabled (SEND_SMTP_PROOF=0)")

# Append dynamic appointment link token literally
# (not an f-string, so the braces are safe; '{{...}}' also works if you prefer)


# === Send a proof to Mickey via Fortellis sendEmail (never to customer) ===
# Rooftop context is already resolved above
try:
    proof_subject = f"{subject}"
    proof_body = (
        body_html.replace("\n", "<br>")
        + "<br><br><hr><p><em>Note: QA-only email sent to Mickey via CRM sendEmail; "
          "customer was NOT emailed.</em></p>"
    )

    _ = maybe_call(send_opportunity_email_activity,
        token=token,
        dealer_key=subscription_id,       # you pass Subscription-Id here (your code already does this)
        opportunity_id=opportunity_id,
        sender=rooftop_sender,            # from rooftops.py mapping
        recipients=PROOF_RECIPIENTS,       # 👈 proof only
        carbon_copies=[],                 # or keep empty in production
        subject=proof_subject,
        body_html=proof_body,
        rooftop_name=rooftop_name,
    )
    log.info("Proof sent to Mickey via sendEmail API")
except Exception as e:
    log.error("sendEmail proof to Mickey failed: %s", e)


# === Log to Fortellis (SAFE first) ==================================

post_results = {}

# 1) Comment (always safe)
try:
    if SAFE_MODE:
        comment_text = f"{PATTI_FIRST_REPLY_SENTINEL} Patti generated a reply (safe mode). Email content stored in comments."
    else:
        comment_text = f"{PATTI_FIRST_REPLY_SENTINEL} Patti generated and sent an intro email (test)."

    r = maybe_call(add_opportunity_comment, token, subscription_id, opportunity_id, comment_text)
    post_results["opportunities_addComment"] = r
    log.info("Added CRM comment (dry_run=%s)", r.get("dry_run", False))
except Exception as e:
    log.error("Add comment failed: %s", e)
    post_results["opportunities_addComment"] = {"error": str(e)}

# 2) Email activity — disabled for customer sends (we only send proof to Mickey above)
post_results["opportunities_sendEmail"] = {"note": "customer send disabled; proof sent to Mickey via sendEmail"}
log.info("Customer send disabled by config; proof already sent to Mickey")


# 3) Vehicle sought (demo data)
try:
    vs = maybe_call(add_vehicle_sought,
        token, subscription_id, opportunity_id,
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

# 4) Schedule & Complete follow-up activity (two-step rooftop-aware fallback)
try:
    due_dt_iso = (_dt.now(_tz.utc) + _td(minutes=10)).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    used_name: str | None = None
    used_type: int | None = None
    last_err = None

    def _schedule_with(name: str, atype: int):
        return maybe_call(schedule_activity,
            token, subscription_id, opportunity_id,
            due_dt_iso_utc=due_dt_iso,
            activity_name=name,
            activity_type=atype,
            comments="Patti demo—schedule a follow-up in ~10 minutes.",
        )

    # If we already learned a good combo for this rooftop in this run, use it straight away.
    if subscription_id in GOOD_ACTIVITY_COMBO:
        used_name, used_type = GOOD_ACTIVITY_COMBO[subscription_id]
        sched = _schedule_with(used_name, used_type)
        post_results["activities_schedule"] = sched
        log.info("Schedule activity (cached) succeeded with combo name=%r type=%r", used_name, used_type)
    else:
        for cand_name, cand_type in ACTIVITY_COMBOS:
            try:
                sched = _schedule_with(cand_name, cand_type)
                used_name, used_type = cand_name, cand_type
                GOOD_ACTIVITY_COMBO[subscription_id] = (used_name, used_type)
                post_results["activities_schedule"] = sched
                log.info("Schedule activity succeeded with combo name=%r type=%r", used_name, used_type)
                break
            except requests.exceptions.HTTPError as e:
                code = getattr(getattr(e, "response", None), "status_code", None)
                body = (getattr(getattr(e, "response", None), "text", "") or "")[:200]
                log.warning("Schedule failed (name=%r,type=%r): status=%s body=%s", cand_name, cand_type, code, body)
                last_err = e
                continue

        if not used_name:
            raise last_err or RuntimeError("No valid activityType/name combination found for this rooftop.")

    activity_id_new = (post_results["activities_schedule"].get("id")
                       or post_results["activities_schedule"].get("activityId"))

    if activity_id_new:
        completed_dt_iso = _dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        comp = maybe_call(complete_activity,
            token, subscription_id, opportunity_id,
            due_dt_iso_utc=due_dt_iso, completed_dt_iso_utc=completed_dt_iso,
            activity_name=used_name,        # use the exact combo that worked
            activity_type=used_type,
            comments="Patti demo—completed as proof.",
            activity_id=activity_id_new,
        )
    else:
        comp = {"skipped": "no activityId from schedule"}

    post_results["activities_complete"] = comp
    log.info("Complete activity: status=%s", comp.get("status", "N/A"))

except Exception as e:
    log.error("Schedule/Complete failed after fallback: %s", str(e)[:300])
    post_results["activities_schedule/complete"] = {"error": str(e)}

