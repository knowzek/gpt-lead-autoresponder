import os
import json
import pprint
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


import xml.etree.ElementTree as ET

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

token = get_token()
leads = get_recent_leads(token)

print(f"📬 Found {len(leads)} leads from Fortellis")

filtered_leads = leads[:25]

pprint.pprint(leads[0])  # Debug: show first lead structure

for lead in filtered_leads:
    print("🔍 Raw lead:", json.dumps(lead, indent=2))
    activity_id = lead.get("activityId")
    if not activity_id:
        print("⚠️ No activityId found, skipping lead.")
        continue

    opportunity_id = lead.get("opportunityId")
    print(f"➡️ Evaluating lead: {activity_id} → Opportunity: {opportunity_id}")

    opportunity = get_opportunity(opportunity_id, token)
    print("📄 Opportunity data:", json.dumps(opportunity, indent=2))

    # 🔍 Fetch inquiry notes from activity link or fallback to ID
    inquiry_text = ""
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
    if not inquiry_text:
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
    
    # Map to a known dealership from test data
    dealership = (
        DEALERSHIP_MAP.get(first_name)
        or DEALERSHIP_MAP.get(full_name)
        or DEALERSHIP_MAP.get(source)
        or DEALERSHIP_MAP.get(sub_source)
        or DEALERSHIP_MAP.get(created_by)
        or "Patterson Auto Group"
    )
    
    # Set base_url for VDP/SRP linking
    base_url = DEALERSHIP_URL_MAP.get(dealership)

    vehicle = opportunity.get("soughtVehicles", [{}])[0]
    year = vehicle.get("yearFrom", "")
    make = vehicle.get("make", "")
    model = vehicle.get("model", "")
    trim = vehicle.get("trim", "")
    stock = vehicle.get("stockNumber", "")

    vehicle_str = f"{year} {make} {model} {trim}".strip()
    if any([year, make, model]):
        if base_url and make and model:
            search_slug = f"?make={make}&model={model}"
            vehicle_str = f'<a href="{base_url}{search_slug}">{vehicle_str}</a>'
    else:
        vehicle_str = "one of our vehicles"


    trade_in = opportunity.get("tradeIns", [{}])[0].get("make", "")
    trade_text = f"They may also be trading in a {trade_in}." if trade_in else ""

    # 👤 Customer name
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
            print("📄 Customer data:", json.dumps(customer_data, indent=2))
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

    prompt = f"""
    You are Patti, the virtual assistant for Patterson Auto Group — including Tustin Mazda, Huntington Beach Mazda, Mission Viejo Kia, and Tustin Hyundai. You respond to guests in a warm, helpful, and professional tone that follows all compliance and brand guidelines.
    
    This guest submitted a lead through {source}.
    They’re interested in: {vehicle_str}.
    Salesperson: {salesperson}
    {trade_text}
    
    Here’s what the guest asked or submitted:
    "{inquiry_text}"
    
    Please write a warm, professional email reply from Patti. Be sure to apply Patti’s voice, formatting, and rules.
    
    {debug_block}
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
