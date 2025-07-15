import os
from fortellis import get_token, get_recent_leads, get_opportunity
from gpt import run_gpt
from emailer import send_email
from state_store import was_processed, mark_processed

MICKEY_EMAIL = os.getenv("MICKEY_EMAIL")

print("▶️ Starting GPT lead autoresponder...")

token = get_token()
leads = get_recent_leads(token)

print(f"📬 Found {len(leads)} leads from Fortellis")

# Limit to 5 leads max per run
leads = leads[:5]

for lead in leads:
    activity_id = lead.get("activityId")
    if was_processed(activity_id):
        print(f"⏭️ Skipping previously processed lead: {activity_id}")
        continue

    print(f"➡️ Processing new lead: {activity_id}")

    opportunity_id = lead.get("opportunityId")
    opportunity = get_opportunity(opportunity_id, token)
    
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
    customer_name = customer.get("firstName", "there")
    
    # Add lead debug details to bottom of email for testing
    debug_block = f"""
    
    ---
    
    🧪 # DEBUG CONTEXT
    Lead Source: {source}
    Vehicle: {vehicle_str}
    Trade-In: {trade_in or 'N/A'}
    Stock #: {stock or 'N/A'}
    Salesperson: {salesperson}
    """
    
    prompt = f"""
    A new lead came in from {source}. They're interested in a {vehicle_str}.
    {trade_text}
    Write a warm, professional email introducing {salesperson} from {dealership} and following up on their interest.
    Include a subject line. Do not include hyperlinks unless a real URL is provided.
    Use the tone and formatting of our assistant Patti.
    
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
