# kbb_ico.py
from datetime import datetime as _dt, timezone as _tz, timedelta as _td
from kbb_templates import TEMPLATES, fill_merge_fields
from kbb_cadence import events_for_day
from fortellis import (
    add_opportunity_comment,
    send_opportunity_email_activity,
    schedule_activity,
)

from fortellis import search_activities_by_opportunity

import json, re
STATE_TAG = "[PATTI_KBB_STATE]"  # marker to find the state comment quickly

import os
TEST_TO = os.getenv("TEST_TO", "pattiautoresponder@gmail.com")
import logging
log = logging.getLogger(__name__)

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
    if not opportunity_id:
        log.warning("skip state comment: missing opportunity_id")
        return
    payload = f"{STATE_TAG} {json.dumps(state, ensure_ascii=False)}"
    add_opportunity_comment(token, subscription_id, opportunity_id, payload)


def customer_has_replied(opportunity: dict, token: str, subscription_id: str) -> tuple[bool, str | None]:
    """Returns (has_replied, last_customer_ts_iso)."""

    # Derive IDs from the opportunity dict (don’t use any external 'hit' var)
    opportunity_id = opportunity.get("opportunityId") or opportunity.get("id")
    customer = (opportunity.get("customer") or {})
    customer_id = customer.get("id")

    # Loud guard: if we somehow lack IDs, fail fast so we don’t send bad requests
    if not opportunity_id:
        log.error("customer_has_replied: missing opportunity_id")
        return False, None
    if not customer_id:
        log.warning("customer_has_replied: missing customer_id (some tenants require it)")

    log.info("KBB ICO: activity search opp=%s cust=%s sub=%s", opportunity_id, customer_id, subscription_id)

    # Fetch recent activities (your tenant requires customerId)
    acts = search_activities_by_opportunity(
        opportunity_id=opportunity_id,
        token=token,
        dealer_key=subscription_id,
        page=1,
        page_size=50,
        customer_id=customer_id,
    )

    last_ts = None
    for a in acts:
        name = (a.get("activityName") or "").lower()
        # treat messages/inbound as customer replies
        if (a.get("activityType") in (3, "message") or "message" in name):
            direction = (a.get("direction") or "").lower()
            created_by = (a.get("createdBy") or "").lower()
            if direction in ("inbound", "from customer") or created_by not in ("patti", "dealer", "sales", "system"):
                last_ts = a.get("createdDate") or a.get("createdOn") or a.get("modifiedDate")
                return True, last_ts

    return False, last_ts


def process_kbb_ico_lead(opportunity, lead_age_days, rooftop_name, inquiry_text,
                         token, subscription_id, SAFE_MODE=False, rooftop_sender=None):
    opp_id = opportunity.get("opportunityId") or opportunity.get("id")
    created_iso = opportunity.get("createdDate") or opportunity.get("created_on")

    # Load state and see if customer replied
    state = _load_state_from_comments(opportunity)
    has_reply, last_cust_ts = customer_has_replied(opportunity, token, subscription_id)


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
            sender=rooftop_sender,  # now defined
            recipients=recipients, carbon_copies=[],
            subject=subject, body_html=body_html, rooftop_name=rooftop_name
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

    # Compute plan for this day
    plan = events_for_day(effective_day)
    if not plan:  # nothing to send today
        return
    
    # --- Load template safely -------------------------------------------------
    tpl_key = plan.get("email_template_day")  # e.g., 0 / 1 / 2 (match your TEMPLATES keys)
    html = TEMPLATES.get(tpl_key)
    if not html:
        log.warning("KBB ICO: missing template for day key=%r", tpl_key)
        return
    
    # --- Rooftop address ------------------------------------------------------
    from rooftops import ROOFTOP_INFO
    rooftop_addr = ((ROOFTOP_INFO.get(rooftop_name, {}) or {}).get("address") or "")
    
    # --- Salesperson (primary) ------------------------------------------------
    sales_team = (opportunity.get("salesTeam") or [])
    sp = next((m for m in sales_team if m.get("isPrimary")), (sales_team[0] if sales_team else {}))
    salesperson_name  = " ".join(filter(None, [sp.get("firstName", ""), sp.get("lastName", "")])).strip()
    salesperson_phone = (sp.get("phone") or sp.get("mobile") or "")
    salesperson_email = (sp.get("email") or "")
    
    # --- Customer basics ------------------------------------------------------
    cust = (opportunity.get("customer") or {})
    cust_first = (cust.get("firstName") or opportunity.get("customer_first") or "there")
    
    # --- Trade info (first trade only) ---------------------------------------
    ti = (opportunity.get("tradeIns") or [{}])[0] if (opportunity.get("tradeIns") or []) else {}
    trade_year  = str(ti.get("year") or "")
    trade_make  = str(ti.get("make") or "")
    trade_model = str(ti.get("model") or "")
    
    # --- Merge fields for template -------------------------------------------
    ctx = {
        "DealershipName": rooftop_name,
        "SalesPersonName": salesperson_name,
        "SalespersonPhone": salesperson_phone,
        "SalespersonEmailAddress": salesperson_email,
        "CustFirstName": cust_first,
        "TradeYear": trade_year,
        "TradeMake": trade_make,
        "TradeModel": trade_model,
        "DealershipAddress": rooftop_addr,
    }
    body_html = fill_merge_fields(html, ctx)
    
    # --- Subject --------------------------------------------------------------
    subject = plan.get("subject") or f"{rooftop_name} — Your Instant Cash Offer"
    
    # --- Recipient resolution (SAFE_MODE honored) -----------------------------
    # Prefer customer.emails[] primary/preferred; fallback to single emailAddress if present
    email_addr = ""
    emails = cust.get("emails") or []
    if emails:
        prim = next((e for e in emails if e.get("isPrimary") or e.get("isPreferred")), None)
        email_addr = (prim or emails[0]).get("address") or ""
    if not email_addr:
        email_addr = cust.get("emailAddress") or ""
    
    recipients = [email_addr] if (email_addr and not SAFE_MODE) else [TEST_TO]
    
    # --- Log + send -----------------------------------------------------------
    add_opportunity_comment(
        token, subscription_id, opp_id,
        f"KBB ICO Day {effective_day}: sending template {tpl_key} to "
        f"{('TEST_TO' if SAFE_MODE else email_addr)}."
    )
    send_opportunity_email_activity(
        token, subscription_id, opp_id,
        sender=rooftop_sender,  # if you have this resolved earlier; else None
        recipients=recipients, carbon_copies=[],
        subject=subject, body_html=body_html, rooftop_name=rooftop_name
    )
    
    # --- Phone/Text tasks (TCPA guard) ---------------------------------------
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

