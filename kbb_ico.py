# kbb_ico.py
from datetime import datetime as _dt, timezone as _tz, timedelta as _td
from kbb_templates import TEMPLATES, fill_merge_fields
from kbb_cadence import events_for_day
from fortellis import (
    add_opportunity_comment,
    send_opportunity_email_activity,
    schedule_activity,
)
from config import TEST_TO 
from fortellis import search_activities_by_opportunity

import json, re
STATE_TAG = "[PATTI_KBB_STATE]"  # marker to find the state comment quickly

import os
ALLOW_TEXTING = os.getenv("ALLOW_TEXTING","0").lower() in ("1","true","yes")

def _ico_offer_expired(created_iso: str, exclude_sunday: bool = True) -> bool:
    if not created_iso:
        return False
    try:
        created = _dt.fromisoformat(created_iso.replace("Z","+00:00")).astimezone(_tz.utc)
    except Exception:
        return False
    days = 7
    if exclude_sunday:
        # count 7 calendar days; Sunday still exists but your email copy says “excluding Sunday”
        pass
    return _dt.now(_tz.utc) > (created + _td(days=days))


def _load_state_from_comments(opportunity) -> dict:
    comments = opportunity.get("messages") or opportunity.get("completedActivitiesTesting") or []
    # Look for our tagged comment body
    for c in comments:
        txt = (c.get("comments") or c.get("notes") or "")
        if STATE_TAG in txt:
            try:
                return json.loads(re.sub(r".*?\[PATTI_KBB_STATE\]\s*", "", txt, flags=re.S))
            except Exception:
                pass
    # default
    return {"mode": "cadence", "last_customer_msg_at": None, "last_agent_msg_at": None}

def _save_state_comment(token, subscription_id, opportunity_id, state: dict):
    payload = f"{STATE_TAG} {json.dumps(state, ensure_ascii=False)}"
    add_opportunity_comment(token, subscription_id, opportunity_id, payload)

def customer_has_replied(opportunity_id, token, subscription_id) -> tuple[bool, str]:
    """Returns (has_replied, last_customer_ts_iso)."""
    acts = search_activities_by_opportunity(opportunity_id, token, subscription_id, page=1, page_size=50)
    last_ts = None
    for a in acts:
        name = (a.get("activityName") or "").lower()
        if (a.get("activityType") in (3, "message") or "message" in name):
            direction = (a.get("direction") or "").lower()
            created_by = (a.get("createdBy") or "").lower()
            if direction in ("inbound", "from customer") or created_by not in ("patti","dealer","sales","system"):
                last_ts = a.get("createdDate") or a.get("createdOn") or a.get("modifiedDate")
                return True, last_ts
    return False, last_ts

def process_kbb_ico_lead(opportunity, lead_age_days, rooftop_name, inquiry_text,
                         token, subscription_id, SAFE_MODE=False):
    opp_id = opportunity.get("id")
    created_iso = opportunity.get("createdDate") or opportunity.get("created_on")

    # Load state and see if customer replied
    state = _load_state_from_comments(opportunity)
    has_reply, last_cust_ts = customer_has_replied(opp_id, token, subscription_id)

    if has_reply:
        # flip to convo mode & persist
        state["mode"] = "convo"
        state["last_customer_msg_at"] = last_cust_ts
        _save_state_comment(token, subscription_id, opp_id, state)

        # Compose a natural reply with GPT (ICO persona)
        from gpt import run_gpt  # local import to avoid circulars
        prompt = (
            f"Lead context:\n"
            f"- Rooftop: {rooftop_name}\n"
            f"- Offer valid window: 7 days excluding Sunday.\n\n"
            f"Customer message:\n\"\"\"{inquiry_text}\"\"\"\n\n"
            "Write a short, natural reply that first acknowledges what they asked, then proposes "
            "2 appointment windows and what to bring (title, ID, keys). No signature block."
        )
        reply = run_gpt(
            prompt,
            customer_name=(opportunity.get('customer',{}) or {}).get('firstName') or "there",
            rooftop_name=rooftop_name,
            prevMessages=True,
            persona="kbb_ico",
            kbb_ctx={"offer_valid_days":7, "exclude_sunday":True},
        )

        # Send reply
        # --- Determine recipient safely --------------------------------------
        cust = (opportunity.get("customer", {}) or {})
        email = cust.get("emailAddress")
        if not email:
            emails = cust.get("emails") or []
            email = (emails[0] or {}).get("address") if emails else None
        if not email:
            email = (opportunity.get("_lead", {}) or {}).get("email_address")  # if you stash it
        recipients = [email] if (email and not SAFE_MODE) else [TEST_TO]
        # ---------------------------------------------------------------------

        send_opportunity_email_activity(
            token, subscription_id, opp_id,
            sender=None, recipients=recipients, carbon_copies=[],
            subject=reply["subject"], body_html=reply["body"], rooftop_name=rooftop_name
        )
        # update agent timestamp
        state["last_agent_msg_at"] = _dt.now(_tz.utc).isoformat()
        _save_state_comment(token, subscription_id, opp_id, state)
        return

    # Still in cadence mode (no reply)
    state["mode"] = "cadence"
    _save_state_comment(token, subscription_id, opp_id, state)

    # Offer-window override (if expired jump to Day 08/09 track)
    expired = _ico_offer_expired(created_iso, exclude_sunday=True)
    effective_day = lead_age_days
    if expired and lead_age_days < 8:
        effective_day = 8  # or 9 based on your PDF plan

    plan = events_for_day(effective_day)
    if not plan:  # nothing to send today
        return

    # Render template
    html = TEMPLATES[plan["email_template_day"]]
    from rooftops import ROOFTOP_INFO
    rooftop_addr = (ROOFTOP_INFO.get(rooftop_name, {}) or {}).get("address", "")
    ctx = {
        "DealershipName": rooftop_name,
        "SalesPersonName": "",
        "SalespersonPhone": "",
        "SalespersonEmailAddress": "",
        "CustFirstName": (opportunity.get("customer",{}) or {}).get("firstName") or "there",
        "TradeYear": str((opportunity.get("tradeIns") or [{}])[0].get("year","")),
        "TradeModel": (opportunity.get("tradeIns") or [{}])[0].get("model",""),
        "DealershipAddress": rooftop_addr,
    }
    body_html = fill_merge_fields(html, ctx)
    subject = plan["subject"]

    # Log + send
    add_opportunity_comment(token, subscription_id, opp_id, f"KBB ICO Day {effective_day}: queued email.")
    recipients = [ (opportunity.get("customer",{}) or {}).get("emailAddress") ]
    if SAFE_MODE: recipients = [TEST_TO]
    send_opportunity_email_activity(
        token, subscription_id, opp_id,
        sender=None, recipients=recipients, carbon_copies=[],
        subject=subject, body_html=body_html, rooftop_name=rooftop_name
    )


    # Phone/Text tasks (TCPA guard)
    if ALLOW_TEXTING and plan.get("create_text_task", False) and _customer_has_text_consent(opportunity):
        schedule_activity(
            token, subscription_id, opp_id,
            due_dt_iso_utc=_dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            activity_name="KBB ICO: Text Task", activity_type=15,
            comments=f"Auto-scheduled per ICO Day {effective_day}."
        )
    if plan.get("create_phone_task", True):
        schedule_activity(
            token, subscription_id, opp_id,
            due_dt_iso_utc=_dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            activity_name="KBB ICO: Phone Task", activity_type=14,
            comments=f"Auto-scheduled per ICO Day {effective_day}."
        )

def _customer_has_text_consent(opportunity) -> bool:
    # TODO: look at your CRM/TCPA field once available
    return bool((opportunity.get("customer",{}) or {}).get("tcpConsent", False))

