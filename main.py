import os
from fortellis import get_token, get_recent_leads, get_opportunity, get_customer_by_url
from gpt import run_gpt
from emailer import send_email
from state_store import was_processed, mark_processed
import json

MICKEY_EMAIL = os.getenv("MICKEY_EMAIL")

print("▶️ Starting GPT lead autoresponder...")

token = get_token()
leads = get_recent_leads(token)

print(f"📬 Found {len(leads)} leads from Fortellis")

# Limit to 5 leads max per run
leads = leads[:3]

for lead in leads:
    activity_id = lead.get("activityId")
#    if was_processed(activity_id):
#        print(f"⏭️ Skipping previously processed lead: {activity_id}")
#       continue

    print(f"➡️ Processing new lead: {activity_id}")

    opportunity_id = lead.get("opportunityId")
    opportunity = get_opportunity(opportunity_id, token)
    print("📄 Opportunity data:", json.dumps(opportunity, indent=2))
    
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
    
    # Extract salesperson info
    salesperson_obj = opportunity.get("salesTeam", [{}])[0]
    salesperson = salesperson_obj.get("firstName", "our team")
    
    # Try to guess the dealership from lead source or position name
    store_map = {
        "Tustin Mazda": "Tustin Mazda",
        "Huntington Beach Mazda": "Huntington Beach Mazda",
        "Tustin Hyundai": "Tustin Hyundai",
        "Mission Viejo Kia": "Mission Viejo Kia"
    }
    
    source = opportunity.get("source", "Internet")
    position_name = salesperson_obj.get("positionName", "")
    
    # First try position name, then fallback to source, then default
    dealership = (
        store_map.get(position_name)
        or store_map.get(source)
        or "Patterson Auto Group"
    )
    
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
    
    # Add lead debug details to bottom of email for testing
    debug_block = f"""
    
    ---
    
    🧪 # DEBUG CONTEXT
    Lead Source: {source}
    Dealership: {dealership}
    Vehicle: {vehicle_str}
    Trade-In: {trade_in or 'N/A'}
    Stock #: {stock or 'N/A'}
    Salesperson: {salesperson}
    """
    
    prompt = f"""
    A new lead came in from {source}. They're interested in a {vehicle_str}.
    {trade_text}
    Write a warm, professional email introducing {salesperson} from {dealership} and following up on their interest.
    
    ### Requirements:
    - Begin the email with: Hi [Guest's Name],
    - Include the customer's vehicle of interest: {vehicle_str}
    - Mention trade-in info if available: {trade_text or 'No trade-in info'}
    - Mention the salesperson by name: {salesperson}
    - Sign the email from Patti at {dealership}
    - Use a warm, helpful tone that reflects our assistant Patti
    - At the end, include this debug info as-is - this is required for debug and testing to compare the raw json results from debug_block to the information in the email:
    {debug_block}

    """

    response = run_gpt(prompt, customer_name)
    print(f"💬 GPT response: {response['body'][:100]}...")
    
    send_email(
        to=MICKEY_EMAIL,
        subject=response["subject"],
        body=response["body"]
    )
    print(f"📧 Email sent to Mickey for lead {activity_id}")
    
    mark_processed(activity_id)
    print(f"✅ Marked lead {activity_id} as processed")

print("🏁 Done.")
