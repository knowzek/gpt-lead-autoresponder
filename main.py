from fortellis import get_recent_leads
from gpt import generate_response
from emailer import send_email
from state_store import load_state, save_state

print("▶️ Starting GPT lead autoresponder...")

# Load the last processed lead time
last_seen_time = load_state()
print(f"ℹ️ Last processed lead timestamp: {last_seen_time}")

# Fetch leads from Fortellis
leads = get_recent_leads(since_minutes=15)
print(f"📬 Found {len(leads)} leads from Fortellis")

# Loop through leads
for lead in leads:
    activity_id = lead.get("activityId")
    created_date = lead.get("createdDate")

    print(f"➡️ Processing lead: {activity_id} created at {created_date}")

    # Skip already processed leads
    if last_seen_time and created_date <= last_seen_time:
        print(f"⏭️ Skipping already processed lead: {activity_id}")
        continue

    response = generate_response(lead)
    print(f"💬 GPT response: {response[:100]}...")

    send_email(lead, response)
    print(f"📧 Email sent to Mickey for lead {activity_id}")

    # Update state
    save_state(created_date)
    print(f"✅ Updated state to {created_date}")

print("🏁 Done.")
