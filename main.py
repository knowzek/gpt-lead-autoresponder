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
    make = vehicle.get("make", "a vehicle")
    model = vehicle.get("model", "")
    year = vehicle.get("yearFrom", "")
    trim = vehicle.get("trim", "")
    source = opportunity.get("source", "Internet")
    salesperson = opportunity.get("salesTeam", [{}])[0].get("firstName", "our team")
    
    customer = opportunity.get("customer", {})
    customer_name = customer.get("firstName", "there")
    
    prompt = f"""
    A new lead came in from {source}. They're interested in a {year} {make} {model} {trim}.
    They may also trade in a {opportunity.get('tradeIns', [{}])[0].get('make', '')}.
    Write a friendly follow-up email introducing {salesperson} from our dealership.
    Include a subject line, and use the tone and format of our assistant Patti.
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
