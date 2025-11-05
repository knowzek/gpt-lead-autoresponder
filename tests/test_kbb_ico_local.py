# tests/test_kbb_ico_local.py
import os
os.environ.setdefault("SAFE_MODE", "1")
os.environ.setdefault("ALLOW_TEXTING", "0")
os.environ.setdefault("TEST_TO", "qa@example.com")

from datetime import datetime, timezone, timedelta
from types import SimpleNamespace

# --- Mocks for Fortellis calls ---------------------------------------
sent = SimpleNamespace(emails=[], comments=[], tasks=[], searches=[])
def add_opportunity_comment(token, dealer_key, opportunity_id, text):
    sent.comments.append({"opportunity_id":opportunity_id, "text":text})

def send_opportunity_email_activity(token, dealer_key, opportunity_id, sender, recipients, carbon_copies, subject, body_html, rooftop_name):
    sent.emails.append({
        "opportunity_id":opportunity_id,
        "recipients":recipients, "subject":subject,
        "body":body_html, "rooftop":rooftop_name
    })

def schedule_activity(token, dealer_key, opportunity_id, due_dt_iso_utc, activity_name, activity_type, comments):
    sent.tasks.append({"opportunity_id":opportunity_id,"name":activity_name,"type":activity_type,"comments":comments})

def search_activities_by_opportunity(opportunity_id, token, dealer_key, page=1, page_size=50):
    # This mock returns whatever "inbound" activities we push into the opportunity below.
    return sent.searches

# --- Wire mocks into your module under test ---------------------------
import kbb_ico as uut
uut.add_opportunity_comment = add_opportunity_comment
uut.send_opportunity_email_activity = send_opportunity_email_activity
uut.schedule_activity = schedule_activity
uut.search_activities_by_opportunity = search_activities_by_opportunity

# --- Minimal templates/cadence for the test --------------------------
import kbb_templates
import kbb_cadence
kbb_templates.TEMPLATES[0] = "<p>Hi <{CustFirstName}>, Day0 ICO — <{TradeYear}> <{TradeModel}></p>"
kbb_cadence.CADENCE = {
    0: {"email_template_day": 0, "subject": "ICO Day 00", "create_phone_task": True, "create_text_task": False}
}

# --- Fake opportunity/lead -------------------------------------------
now_iso = datetime.now(timezone.utc).isoformat()
opportunity = {
    "id": "OPP123",
    "createdDate": now_iso,
    "source": "KBB ICO",
    "customer": {"firstName": "Ashley", "emails": [{"address":"ashley@tester.com","isPrimary":True}]},
    "tradeIns": [{"year": 2016, "model": "Civic"}],
    # In-memory comments list so _load_state_from_comments can see state
    "completedActivitiesTesting": []
}
lead = {"customer_first": "Ashley", "email_address": "ashley@tester.com"}

# --- Run 1: No reply yet => should send Day 00 template ---------------
def test_drip_day0():
    sent.emails.clear(); sent.comments.clear(); sent.tasks.clear(); sent.searches.clear()
    uut.process_kbb_ico_lead(
        opportunity=opportunity,
        lead_age_days=0,
        rooftop_name="Mission Viejo Kia",
        inquiry_text="",
        token="T", subscription_id="S",
        SAFE_MODE=True,   # routes to TEST_TO
    )
    assert len(sent.emails) == 1, "Expected Day 00 email"
    print("DRIP EMAIL:", sent.emails[-1])

# --- Run 2: Simulate inbound customer reply => should GPT-reply -------
def test_convo_reply():
    sent.emails.clear(); sent.comments.clear(); sent.tasks.clear(); sent.searches.clear()
    # Add a fake inbound activity to mock a customer reply
    sent.searches.append({
        "activityType": "message",
        "direction": "inbound",
        "createdDate": datetime.now(timezone.utc).isoformat(),
        "activityName": "Customer replied"
    })

    uut.process_kbb_ico_lead(
        opportunity=opportunity,
        lead_age_days=0,                  # day calc now irrelevant; we’re in convo
        rooftop_name="Mission Viejo Kia",
        inquiry_text="Can I come Saturday morning?",
        token="T", subscription_id="S",
        SAFE_MODE=True,
    )
    assert len(sent.emails) == 1, "Expected GPT convo email"
    print("CONVO EMAIL:", sent.emails[-1])

if __name__ == "__main__":
    test_drip_day0()
    test_convo_reply()
    print("COMMENTS:", sent.comments)
    print("TASKS:", sent.tasks)
