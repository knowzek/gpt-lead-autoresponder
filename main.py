import os
import json
import pprint
import re
import xml.etree.ElementTree as ET
from fortellis import (
    get_token,
    get_recent_leads,
    get_opportunity,
    get_customer_by_url,
    get_activity_by_url,
    get_activity_by_id_v1
)

from gpt import run_gpt
from emailer import send_email
USE_EMAIL_MODE = True  # Set to False to use Fortellis API
from imapclient import IMAPClient
import email

def fetch_adf_xml_from_gmail(email_address, app_password, sender_filters=None):
    if sender_filters is None:
        sender_filters = [
            "notify@eleadnotify.com",
            "Sales@tustinhyundai.edealerhub.com",
            "sales@missionviejokia.edealerhub.com"
        ]

    with IMAPClient("imap.gmail.com", ssl=True) as client:
        client.login(email_address, app_password)
        client.select_folder("INBOX")

        messages = client.search(["UNSEEN"])

        if not messages:
            print("📭 No new lead emails found.")
            return None, None

        for uid, message_data in client.fetch(messages, ["RFC822"]).items():
            msg = email.message_from_bytes(message_data[b"RFC822"])
            from_header = msg.get("From", "").lower()
            print("📤 Email from:", from_header)
            if any(sender.lower() in from_header for sender in sender_filters):
                print("✉️ Subject:", msg.get("Subject"))

                for part in msg.walk():
                    content_type = part.get_content_type()
                    if content_type in ["text/plain", "text/html"]:
                        body = part.get_payload(decode=True).decode(errors="ignore")
                        print("📨 Raw email preview:\n", body[:500])
                        return body.strip(), from_header


    print("⚠️ No ADF/XML found in email body.")
    return None, None


def parse_plaintext_lead(body):
    try:
        # Rough match for vehicle block
        vehicle_match = re.search(r"Vehicle:\s+([^\n<]+)", body)
        name_match = re.search(r"Name:\s+([^\n<]+)", body)
        phone_match = re.search(r"Phone:\s+([^\n<]+)", body)
        email_match = re.search(r"E-?Mail:\s+([^\s<]+)", body)
        comment_match = re.search(r"Comments:\s+(.*?)<", body)

        vehicle_parts = vehicle_match.group(1).split() if vehicle_match else []
        year = vehicle_parts[0] if len(vehicle_parts) > 0 else ""
        make = vehicle_parts[1] if len(vehicle_parts) > 1 else ""
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
            "vehicle": {
                "year": year,
                "make": make,
                "model": model,
                "trim": ""  # You can refine this later if needed
            },
            "notes": comment_match.group(1).strip() if comment_match else ""
        }
    except Exception as e:
        print(f"❌ Failed to parse plain text lead: {e}")
        return None


DEALERSHIP_URL_MAP = {
    "Tustin Mazda": "https://www.tustinmazda.com/",
    "Huntington Beach Mazda": "https://www.huntingtonbeachmazda.com/",
    "Tustin Hyundai": "https://www.tustinhyundai.com/",
    "Mission Viejo Kia": "https://www.missionviejokia.com/",
    "Patterson Auto Group": "https://www.pattersonautos.com/"
}

def infer_dealership(salesperson_obj, source):
    # Map test/demo names to real dealerships
    demo_name = f"{salesperson_obj.get('firstName', '')} {salesperson_obj.get('lastName', '')}".strip()
    test_name_map = {
        "Test606 CLB": "Tustin Mazda",
        "Bloskie Terry": "Tustin Hyundai",
        "Demo User": "Huntington Beach Mazda",
        "Desk Manager 1": "Mission Viejo Kia"
    }

    source_map = {
        "Podium": "Tustin Mazda",
        "CarNow": "Mission Viejo Kia",
        "AutoTrader": "Huntington Beach Mazda"
    }

    # Try test name first
    if demo_name in test_name_map:
        return test_name_map[demo_name]

    # Fallback to source
    return source_map.get(source, "Patterson Auto Group")

def extract_adf_comment(adf_xml: str) -> str:
    try:
        root = ET.fromstring(adf_xml)
        comment_el = root.find(".//customer/comments")
        if comment_el is not None and comment_el.text:
            return comment_el.text.strip()
    except Exception as e:
        print(f"⚠️ Failed to parse ADF XML: {e}")
    return ""

# from state_store import was_processed, mark_processed

MICKEY_EMAIL = os.getenv("MICKEY_EMAIL")

# 🧭 Salesperson → Standardized Full Name
SALES_PERSON_MAP = {
    "Madeleine": "Madeleine Demo",
    "Pavan": "Pavan Singh",
    "Joe B": "Joe B",  # Already full
    "Desk Manager 1": "Jim Feinstein",  # Replace with a known team member style if needed
    "Bloskie, Terry": "Terry Bloskie",  # Fix CRM name style
    "Test606, CLB": "Roozbeh",          # Assign to a known persona
}

# 🏢 Source/Subsource/Salesperson → Dealership
DEALERSHIP_MAP = {
    "Podium": "Tustin Hyundai",
    "Podium Webchat": "Tustin Hyundai",
    "CarNow": "Mission Viejo Kia",
    "Madeleine": "Tustin Mazda",
    "Pavan": "Tustin Hyundai",
    "Joe B": "Huntington Beach Mazda",
    "Bloskie, Terry": "Tustin Hyundai",
    "Test606, CLB": "Tustin Hyundai",
    "Desk Manager 1": "Mission Viejo Kia"
}

# 🌐 Dealership → SRP URL base
DEALERSHIP_URL_MAP = {
    "Tustin Mazda": "https://www.tustinmazda.com/used-inventory/",
    "Huntington Beach Mazda": "https://www.huntingtonbeachmazda.com/used-inventory/",
    "Tustin Hyundai": "https://www.tustinhyundai.com/used-inventory/",
    "Mission Viejo Kia": "https://www.missionviejokia.com/used-inventory/"
}

print("▶️ Starting GPT lead autoresponder...")

if USE_EMAIL_MODE:
    print("📥 Email mode enabled — pulling latest email...")
    email_body, from_header = fetch_adf_xml_from_gmail(
        os.getenv("GMAIL_USER"),
        os.getenv("GMAIL_APP_PASSWORD")
    )
    if not email_body:
        print("❌ No email lead found.")
        exit()
    
    print("📨 Raw email preview:\n", email_body[:500])


    if "<?xml" in email_body:
        parsed_lead = parse_adf_xml_to_lead(email_body)
    else:
        parsed_lead = parse_plaintext_lead(email_body)

    if not parsed_lead:
        print("❌ Failed to parse lead from email body.")
        exit()

        # ─── override dealership by sender domain ───
    hdr = from_header.lower()
    if "missionviejokia" in hdr:
        email_dealership = "Mission Viejo Kia"
    elif "tustinhyundai" in hdr:
        email_dealership = "Tustin Hyundai"
    elif "huntingtonbeachmazda" in hdr:
        email_dealership = "Huntington Beach Mazda"
    elif "tustinmazda" in hdr:
        email_dealership = "Tustin Mazda"
    else:
        email_dealership = "Patterson Auto Group"
    # ─────────────────────────────────────────────

    leads = [parsed_lead]

else:
    token = get_token()
    leads = get_recent_leads(token)


print(f"📬 Found {len(leads)} leads from Fortellis")

filtered_leads = leads[:5]

pprint.pprint(leads[0])  # Debug: show first lead structure

for lead in filtered_leads:
    print("🔍 Raw lead:", json.dumps(lead, indent=2))
    activity_id = lead.get("activityId")
    if not activity_id:
        print("⚠️ No activityId found, skipping lead.")
        continue

    opportunity_id = lead.get("opportunityId")
    print(f"➡️ Evaluating lead: {activity_id} → Opportunity: {opportunity_id}")

    if USE_EMAIL_MODE:
        # Fabricate a fake opportunity object using parsed email values
        opportunity = {
            "salesTeam": [{"firstName": "Pavan", "lastName": "Singh"}],  # Default fallback
            "source": parsed_lead.get("source", "Email"),
            "subSource": "",
            "soughtVehicles": [parsed_lead.get("vehicle", {})],
            "customer": {"id": "email"},
            "tradeIns": [],
            "createdBy": "Patti Assistant"
        }
        inquiry_text = parsed_lead.get("notes", "")
    else:
        opportunity = get_opportunity(opportunity_id, token)
        print("📄 Opportunity data:", json.dumps(opportunity, indent=2))
    
    if not USE_EMAIL_MODE:
        # 🔍 Fetch inquiry notes from activity link or fallback to ID
        activity_url = None
        for link in lead.get("links", []):
            if "activity" in link.get("title", "").lower():
                activity_url = link.get("href")
                break
    
        if activity_url:
            try:
                activity_data = get_activity_by_url(activity_url, token)
                print("🧾 Raw activity data:", json.dumps(activity_data, indent=2))  # <-- Add this
                inquiry_text = activity_data.get("notes", "") or ""
    
                # 👇 fallback to parsing ADF XML if 'notes' is empty
                if not inquiry_text and "message" in activity_data:
                    inquiry_text = extract_adf_comment(activity_data["message"].get("body", ""))
    
                print(f"📩 Inquiry text: {inquiry_text}")
            except Exception as e:
                print(f"⚠️ Failed to fetch activity by URL: {e}")
        else:
            print(f"⚠️ No activity link found for lead {activity_id}, trying fallback...")
            try:
                activity_data = get_activity_by_id_v1(activity_id, token)
                inquiry_text = activity_data.get("notes", "") or ""
    
                # 👇 fallback to parsing ADF XML if 'notes' is empty
                if not inquiry_text and "message" in activity_data:
                    inquiry_text = extract_adf_comment(activity_data["message"].get("body", ""))
                print(f"📩 Inquiry text (fallback by ID): {inquiry_text}")
            except Exception as e:
                print(f"❌ Fallback failed: Could not fetch activity by ID: {e}")
                continue

    # ✅ Final fallback: retry get_activity_by_id in case URL lookup failed earlier
    if not USE_EMAIL_MODE and not inquiry_text:
        try:
            activity_data = get_activity_by_id_v1(activity_id, token)

            print("🧾 Raw activity data (by ID):", json.dumps(activity_data, indent=2))  # <== ADD THIS LINE
            inquiry_text = activity_data.get("notes", "") or ""

            # 👇 fallback to parsing ADF XML if 'notes' is empty
            if not inquiry_text and "message" in activity_data:
                inquiry_text = extract_adf_comment(activity_data["message"].get("body", ""))
            print(f"📩 Inquiry text (fallback by ID): {inquiry_text}")
        except Exception as e:
            print(f"⚠️ Final fallback failed: {e}")
    # 🛑 Fallback trigger if inquiry is blank or generic
    fallback_mode = False
    if not inquiry_text or inquiry_text.strip().lower() in ["", "request a quote", "interested", "info", "information", "looking"]:
        fallback_mode = True


    # 🔍 Salesperson and Dealership Mapping

    salesperson_obj = opportunity.get("salesTeam", [{}])[0]
    first_name = salesperson_obj.get("firstName", "").strip()
    last_name = salesperson_obj.get("lastName", "").strip()
    full_name = f"{first_name} {last_name}".strip()
    created_by = opportunity.get("createdBy", "")  # fallback if needed
    
    # Map salesperson to known persona name for Patti
    salesperson = (
        SALES_PERSON_MAP.get(first_name)
        or SALES_PERSON_MAP.get(full_name)
        or SALES_PERSON_MAP.get(created_by)
        or full_name
        or "our team"
    )
    
    # Determine lead source/subSource
    source = opportunity.get("source", "")
    sub_source = opportunity.get("subSource", "")
    position_name = salesperson_obj.get("positionName", "")
    
    if USE_EMAIL_MODE:
        dealership = email_dealership
    else:
        dealership = (
            DEALERSHIP_MAP.get(first_name)
            or DEALERSHIP_MAP.get(full_name)
            or DEALERSHIP_MAP.get(source)
            or DEALERSHIP_MAP.get(sub_source)
            or DEALERSHIP_MAP.get(created_by)
            or "Patterson Auto Group"
        )

    CONTACT_INFO_MAP = {
        "Tustin Hyundai":    "Tustin Hyundai, 16 Auto Center Dr, Tustin, CA 92782 | (714) 838-4554 | https://www.tustinhyundai.com/",
        "Mission Viejo Kia": "Mission Viejo Kia, 24041 El Toro Rd, Lake Forest, CA 92630 | (949) 768-7900 | https://www.missionviejokia.com/",
        "Tustin Mazda":      "Tustin Mazda, 28 Auto Center Dr, Tustin, CA 92782 | (714) 258-2300 | https://www.tustinmazda.com/",
        "Huntington Beach Mazda": "Huntington Beach Mazda, 16800 Beach Blvd, Huntington Beach, CA 92647 | (714) 847-7686 | https://www.huntingtonbeachmazda.com/",
        "Patterson Auto Group":   "Patterson Auto Group, 123 Main St, Irvine, CA 92618 | (949) 555-0100 | https://www.pattersonautos.com/"
    }
    contact_info = CONTACT_INFO_MAP.get(
        dealership,
        CONTACT_INFO_MAP["Patterson Auto Group"]
    )

    # Set base_url for VDP/SRP linking
    base_url = DEALERSHIP_URL_MAP.get(dealership)

    # 📦 Vehicle info
    vehicle = opportunity.get("soughtVehicles", [{}])[0]
    make = vehicle.get("make", "")
    model = vehicle.get("model", "")
    year = vehicle.get("yearFrom", "")
    trim = vehicle.get("trim", "")
    stock = vehicle.get("stockNumber", "")
    vehicle_str = f"{year} {make} {model} {trim}".strip()

    # 🔁 Fallback: parse ADF XML if vehicle is blank (only in API mode)
    if not USE_EMAIL_MODE and not any([year, make, model]):
        try:
            xml = activity_data.get("message", {}).get("body", "")

            root = ET.fromstring(xml)
            v = root.find(".//vehicle")
            if v is not None:
                year = v.findtext("year", "").strip()
                make = v.findtext("make", "").strip()
                model = v.findtext("model", "").strip()
                trim = v.findtext("trim", "").strip()
            else:
                print("⚠️ No <vehicle> element found in ADF XML.")

        except Exception as e:
            print(f"⚠️ Failed to parse fallback vehicle info from ADF XML: {e}")

    # Link model text to SRP if dealership known
    base_url = DEALERSHIP_URL_MAP.get(dealership)
    if any([year, make, model]):
        if base_url and make and model:
            search_slug = f"?make={make}&model={model}"
            vehicle_str = f'<a href="{base_url}{search_slug}">{vehicle_str}</a>'
    else:
        vehicle_str = "one of our vehicles"

    trade_ins = opportunity.get("tradeIns", [])
    trade_in = trade_ins[0].get("make", "") if trade_ins else ""

    trade_text = f"They may also be trading in a {trade_in}." if trade_in else ""

    # 👤 Customer name
    if USE_EMAIL_MODE:
        # in email mode, we know the guest’s first name already
        customer_name = parsed_lead.get("email_first", "there")
    else:
        customer = opportunity.get("customer", {})
        customer_url = ""
        for link in customer.get("links", []):
            if link.get("rel") == "self":
                customer_url = link.get("href")
                break

        customer_name = "there"
        if customer_url:
            try:
                customer_data = get_customer_by_url(customer_url, token)
                first_name = customer_data.get("firstName", "").strip()
                if first_name and first_name.lower() not in ["mobile", "test", "unknown"]:
                    customer_name = first_name
            except Exception as e:
                print(f"⚠️ Failed to fetch customer name: {e}")

    # 🧪 Debug info for GPT
    debug_block = f"""
    ---
    🧪 # DEBUG CONTEXT
    Customer Name: {customer_name}
    Lead Source: {source}
    Dealership: {dealership}
    Vehicle: {vehicle_str}
    Trade-In: {trade_in or 'N/A'}
    Stock #: {stock or 'N/A'}
    Salesperson: {salesperson}
    Activity ID: {activity_id}
    Opportunity ID: {opportunity_id}
    """

    if fallback_mode:
        prompt = f"""
        Your job is to write personalized, dealership-branded emails from Patti, a friendly virtual assistant.
    
        The guest submitted a lead through {source}.
        They’re interested in: {vehicle_str}.
        Salesperson: {salesperson}
        {trade_text}
    
        They didn’t leave a detailed message.
    
        Please write a warm, professional email reply that:
        - Begin your reply with exactly `Hi {customer_name},` where `{customer_name}` is the lead’s first name as extracted from the email
        - Starts with 1–2 appealing vehicle features or dealership Why Buys (if available)
        - Welcomes the guest and highlights your team's helpfulness
        - Encourages them to share any specific questions or preferences
        - Mentions the salesperson by name
    
        {debug_block}
        Dealership Contact Info: {contact_info}
        """
    else:
        prompt = f"""
        Your job is to write personalized, dealership-branded emails from Patti, a friendly virtual assistant.
    
        When writing:
        - Begin your reply with exactly `Hi {customer_name},` where `{customer_name}` is the lead’s first name as extracted from the email
        - Lead with VALUE: if you have Why Buy info or vehicle features, make that the first thing Patti shares
        - If the customer mentioned a vehicle, answer them confidently and link to that inventory if possible
        - If there’s a specific question, answer it first before offering general help
        - Do NOT ask the customer to “let us know what you’re interested in” if the vehicle is already clear
        - Always include the salesperson’s name and invite them to reach out
        - Keep it warm, clear, and helpful — no robotic filler
    
        This guest submitted a lead through {source}.
        They’re interested in: {vehicle_str}.
        Salesperson: {salesperson}
        {trade_text}
        
        Here’s what the guest asked or submitted:
        "{inquiry_text}"
        
        Please write a warm, professional email reply from Patti. Be sure to apply Patti’s voice, formatting, and rules.

        {debug_block}
        Dealership Contact Info: {contact_info}
        
        """

    response = run_gpt(prompt, customer_name)
    print(f"💬 GPT response: {response['body'][:100]}...")

    send_email(
        to=["knowzek@gmail.com", "knowzek@gmail.com"],
        subject=response["subject"],
        body=response["body"]
    )
    print(f"📧 Email sent to Mickey for lead {activity_id}")

    # mark_processed(opportunity_id)
    # print(f"✅ Marked lead {activity_id} as processed")

print("🏁 Done.")
