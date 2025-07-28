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
# from state_store import was_processed, mark_processed

MICKEY_EMAIL = os.getenv("MICKEY_EMAIL")

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



    # 📦 Vehicle info
    vehicle = opportunity.get("soughtVehicles", [{}])[0]
    make = vehicle.get("make", "")
    model = vehicle.get("model", "")
    year = vehicle.get("yearFrom", "")
    trim = vehicle.get("trim", "")
    stock = vehicle.get("stockNumber", "")
    vehicle_str = f"{year} {make} {model} {trim}".strip()
    if not any([year, make, model, trim]):
        vehicle_str = "one of our vehicles"

    trade_in = opportunity.get("tradeIns", [{}])[0].get("make", "")
    trade_text = f"They may also be trading in a {trade_in}." if trade_in else ""

    salesperson_obj = opportunity.get("salesTeam", [{}])[0]
    salesperson = salesperson_obj.get("firstName", "our team")

    store_map = {
        "Tustin Mazda": "Tustin Mazda",
        "Huntington Beach Mazda": "Huntington Beach Mazda",
        "Tustin Hyundai": "Tustin Hyundai",
        "Mission Viejo Kia": "Mission Viejo Kia"
    }
    source = opportunity.get("source", "Internet")
    position_name = salesperson_obj.get("positionName", "")
    dealership = (
        store_map.get(position_name)
        or store_map.get(source)
        or "Patterson Auto Group"
    )

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
    You are Patti, the virtual assistant for Patterson Auto Group.

    This guest submitted a lead through {source}.
    They’re interested in: {vehicle_str}.
    Salesperson: {salesperson}
    {trade_text}

    Here’s what the guest asked or submitted:
    "{inquiry_text}"

    Please write a warm, professional reply. If you can’t tell which dealership this is for, follow your fallback behavior for Unknown Store. If an appointment is mentioned, include it per your system rules.

    Use Patti’s tone, logic, and formatting per your system instructions.

    ### Debug info for testing:
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
