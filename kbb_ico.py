# kbb_ico.py
from datetime import datetime as _dt, timezone as _tz
from kbb_templates import TEMPLATES, fill_merge_fields
from kbb_cadence import events_for_day
from fortellis import (
    add_opportunity_comment,
    send_opportunity_email_activity,
    schedule_activity,
)
from config import SAFE_MODE, TEST_TO  # or wherever these live


def customer_has_replied(opportunity):
    """Check if the lead already replied — prevents duplicate drip emails."""
    acts = opportunity.get("completedActivitiesTesting") or opportunity.get("completedActivities") or []
    for act in acts:
        if (act.get("activityType") == "message") and act.get("direction", "").lower() == "inbound":
            return True
        if "reply" in (act.get("activityName") or "").lower():
            return True
    return False


def process_kbb_ico_lead(opportunity, lead_age_days, rooftop_name, inquiry_text,
                         token, subscription_id, SAFE_MODE=False):
    """Send the correct ICO cadence email for today and schedule next steps."""
    salesperson = ""
    rooftop_addr = ""
    try:
        from rooftops import ROOFTOP_INFO
        rooftop_addr = (ROOFTOP_INFO.get(rooftop_name, {}) or {}).get("address", "")
    except Exception:
        pass

    plan = events_for_day(lead_age_days)
    if not plan:
        return

    if customer_has_replied(opportunity):
        print(f"Skipping drip: customer already replied for {rooftop_name}")
        return

    html = TEMPLATES[plan["email_template_day"]]
    ctx = {
        "DealershipName": rooftop_name,
        "SalesPersonName": salesperson,
        "SalespersonPhone": "",
        "SalespersonEmailAddress": "",
        "CustFirstName": opportunity.get("customer", {}).get("firstName") or "there",
        "TradeYear": str((opportunity.get("tradeIns") or [{}])[0].get("year", "")),
        "TradeModel": (opportunity.get("tradeIns") or [{}])[0].get("model", ""),
        "DealershipAddress": rooftop_addr,
    }
    body_html = fill_merge_fields(html, ctx)
    subject = plan["subject"]

    # 1️⃣  Log comment
    add_opportunity_comment(
        token, subscription_id, opportunity.get("id"), f"KBB ICO Day {lead_age_days}: queued email."
    )

    # 2️⃣  Send the email
    recipients = [opportunity.get("customer", {}).get("emailAddress")]
    if SAFE_MODE:
        recipients = [TEST_TO]

    send_opportunity_email_activity(
        token, subscription_id, opportunity.get("id"),
        sender=None, recipients=recipients,
        carbon_copies=[], subject=subject, body_html=body_html,
        rooftop_name=rooftop_name,
    )

    # 3️⃣  Schedule phone/text tasks
    schedule_activity(
        token, subscription_id, opportunity.get("id"),
        due_dt_iso_utc=_dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        activity_name="KBB ICO: Phone Task", activity_type=14,
        comments=f"Auto-scheduled per ICO Day {lead_age_days} workflow.",
    )
