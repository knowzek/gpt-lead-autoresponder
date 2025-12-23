# kbb_adf_ingestion.py
import re
import logging

from fortellis import get_token, get_recent_opportunities, get_opportunity, get_customer_by_url, find_recent_kbb_opportunity_by_email, find_best_kbb_opp_for_email
from constants import CUSTOMER_URL 
from airtable_store import upsert_lead, find_by_opp_id, _safe_json_dumps, find_by_customer_email, opp_from_record, save_opp

from datetime import datetime as _dt, timedelta as _td, timezone as _tz
from email_ingestion import clean_html  
import json
import requests


# TEMP: while testing, only these rooftops
ALLOWED_SUBSCRIPTIONS = {
    "7a05ce2c-cf00-4748-b841-45b3442665a7",
    "c27d7f4f-4a4c-45c8-8154-a5de48421fc3",
}


# Fortellis "Complete Activity" endpoint
FORTELLIS_COMPLETE_ACTIVITY_URL = "https://api.fortellis.io/sales/v1/elead/activities/complete"

# Based on your map screenshot: "send email": 3, "note": 37
ACTIVITY_TYPE_SEND_EMAIL = 3
ACTIVITY_TYPE_NOTE = 37

def log_completed_activity_email(*, token: str, subscription_id: str, opportunity_id: str, customer_id: str | None,
                                 subject: str, body_text: str, from_email: str, to_email: str,
                                 use_send_email_type: bool = True) -> None:
    """
    Logs an outbound email to Fortellis as a completed activity.
    IMPORTANT: This does NOT send an email. It only logs.
    """
    activity_type_id = ACTIVITY_TYPE_SEND_EMAIL if use_send_email_type else ACTIVITY_TYPE_NOTE

    payload = {
        "activityTypeId": activity_type_id,
        "opportunityId": opportunity_id,
        "customerId": customer_id,
        "completedDateTime": _dt.now(_tz.utc).isoformat(),
        "comments": f"PATTI sent Day 1 email via Outlook.\nFrom: {from_email}\nTo: {to_email}\nSubject: {subject}",
        "message": {
            "subject": subject,
            "body": body_text,
        },
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": subscription_id,
        "Request-Id": f"patti-kbb-day1-{opportunity_id}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    try:
        r = requests.post(FORTELLIS_COMPLETE_ACTIVITY_URL, headers=headers, json=payload, timeout=30)
        if r.status_code >= 400:
            # Don't fail the webhook if CRM logging fails
            # (email already went out; we can retry later)
            print("WARN: Fortellis activity log failed:", r.status_code, r.text[:300])
    except Exception as e:
        print("WARN: Fortellis activity log exception:", e)

log = logging.getLogger("patti.kbb_adf")

# Match $12,345 or ¤12,345 but NOT things like "+$1,025"
_KBB_AMT_RE = re.compile(r"(?<!\+)(?:\$|¤)[0-9][0-9,]*")

def _extract_kbb_amount(text: str) -> str | None:
    """
    Try to pull out the most relevant KBB offer amount from the ADF email body.

    Priority:
      1) "Counter Offer Amount: $XX,XXX"
      2) "Offer Amount: $XX,XXX" or "Offer Amount: ¤XX,XXX"
      3) "Instant Cash Offer $XX,XXX"
      4) First generic currency-looking amount as a fallback.

    We also normalize '¤' → '$' for display.
    """
    if not text:
        return None

    lower = text.lower()

    def _find_after(label: str) -> str | None:
        idx = lower.find(label.lower())
        if idx == -1:
            return None
        # Look in a short window after the label
        segment = text[idx: idx + 200]
        m = _KBB_AMT_RE.search(segment)
        if not m:
            return None
        raw = m.group(0)
        # Normalize ¤ to $
        return raw.replace("¤", "$")

    # 1) Counter-offer first, if present
    amt = _find_after("Counter Offer Amount")
    if amt:
        return amt

    # 2) Standard offer amount next
    amt = _find_after("Offer Amount")
    if amt:
        return amt

    # 3) Instant Cash Offer line
    amt = _find_after("Instant Cash Offer")
    if amt:
        return amt

    # 4) Generic fallback: first amount anywhere
    m = _KBB_AMT_RE.search(text)
    if not m:
        return None
    raw = m.group(0)
    return raw.replace("¤", "$")



def _extract_shopper_email(body_text: str) -> str | None:
    """
    Parse the ADF notification email text to find shopper's email.
    Example lines:
      Email: knowzek@gmail.com
    """
    if not body_text:
        return None

    m = re.search(r"Email:\s*([^\s]+@[^\s]+)", body_text, re.IGNORECASE)
    if not m:
        return None
    return m.group(1).strip().lower()

def _find_kbb_opp_by_email_via_fortellis(shopper_email: str) -> tuple[str, str] | None:
    """
    Returns (subscription_id, opportunity_id) for the most recent KBB opp whose customer email matches.
    Strategy: searchDelta last N minutes, then hydrate customer for KBB candidates and compare email.
    """
    from fortellis import SUB_MAP

    # search window: choose something generous but not crazy
    LOOKBACK_MIN = 24 * 60  # 24h

    # SUB_MAP may be {dealer_key: subscription_id}; we only need subscription_ids
    sub_ids = list(SUB_MAP.values()) if isinstance(SUB_MAP, dict) else []

    shopper_email = (shopper_email or "").strip().lower()
    if not shopper_email:
        return None

    for subscription_id in sub_ids:
        try:
            tok = get_token(subscription_id)
            data = get_recent_opportunities(tok, subscription_id, since_minutes=LOOKBACK_MIN, page_size=100)
            items = (data or {}).get("items") or []

            # only KBB candidates
            for op in items:
                src = (op.get("source") or "").strip().lower()
                if not src.startswith("kbb"):
                    continue

                opp_id = op.get("opportunityId") or op.get("id")
                cust_id = (op.get("customer") or {}).get("id")
                if not (opp_id and cust_id):
                    continue

                # hydrate customer and compare email
                try:
                    cust = get_customer_by_url(f"{CUSTOMER_URL}/{cust_id}", tok, subscription_id) or {}
                    emails = cust.get("emails") or []
                    for e in emails:
                        addr = (e.get("address") or "").strip().lower()
                        if addr and addr == shopper_email:
                            return (subscription_id, opp_id)
                except Exception:
                    continue

        except Exception:
            continue

    return None


def process_kbb_adf_notification(inbound: dict) -> None:
    subject = inbound.get("subject") or ""
    body_html = inbound.get("body_html") or ""
    body_text = inbound.get("body_text") or clean_html(body_html)

    log.info("KBB ADF: raw body_text sample: %r", (body_text or "")[:500])

    shopper_email = _extract_shopper_email(body_text)
    if not shopper_email:
        log.warning("KBB ADF inbound had no shopper email; subject=%s", subject)
        return

    # -----------------------------
    # 1) Fortellis lookup (NOT Airtable)
    # -----------------------------

    opp_id = None
    subscription_id = None
    customer_id = None
    reason = None
    
    for sub_id in ALLOWED_SUBSCRIPTIONS:
        try:
            tok = get_token(sub_id)
            found_opp_id, found_cust_id, why = find_best_kbb_opp_for_email(
                shopper_email=shopper_email,
                token=tok,
                subscription_id=sub_id,
            )
            if found_opp_id:
                opp_id = found_opp_id
                customer_id = found_cust_id
                subscription_id = sub_id
                reason = why
                break
        except Exception as e:
            log.warning("KBB ADF: lookup failed sub=%s err=%s", sub_id, e)
    
    if not opp_id or not subscription_id:
        log.warning("KBB ADF: No Fortellis KBB opp found for shopper email %s", shopper_email)
        return
    
    log.info("KBB ADF: matched opp=%s sub=%s customer=%s (%s)", opp_id, subscription_id, customer_id, reason)


    # -----------------------------
    # 2) Hydrate full opp + customer (THIS FIXES missing email)
    # -----------------------------
    token = get_token(subscription_id)
    fresh_opp = get_opportunity(opp_id, token, subscription_id) or {}

    # hydrate customer FULL record (emails live here)
    customer = fresh_opp.get("customer") or {}
    customer_id = customer.get("id")

    if customer_id:
        try:
            customer_full = get_customer_by_url(f"{CUSTOMER_URL}/{customer_id}", token, subscription_id) or {}
            if customer_full:
                customer = customer_full
        except Exception as e:
            log.warning("KBB ADF: customer hydrate failed opp=%s cust_id=%s err=%s", opp_id, customer_id, e)

    # Build opp_json
    now_iso = _dt.now(_tz.utc).isoformat()

    opportunity = dict(fresh_opp)
    opportunity["opportunityId"] = opportunity.get("opportunityId") or opportunity.get("id") or opp_id
    opportunity["id"] = opportunity.get("id") or opportunity["opportunityId"]
    opportunity["_subscription_id"] = subscription_id

    # ensure customer is stored (with emails)
    if customer:
        opportunity["customer"] = customer

    # safety defaults processNewData expects
    opportunity.setdefault("messages", [])
    opportunity.setdefault("checkedDict", {
        "patti_already_contacted": False,
        "last_msg_by": None,
        "is_sales_contacted": False,
    })
    opportunity.setdefault("isActive", True)
    opportunity.setdefault("status", opportunity.get("status") or "Active")
    opportunity.setdefault("subStatus", opportunity.get("subStatus") or opportunity.get("substatus") or "New")
    opportunity.setdefault("upType", opportunity.get("upType") or opportunity.get("uptype") or "Campaign")

    # mark that ADF arrived (lets cron guard against accidental blasts)
    opportunity["_kbb_adf_seen_at"] = now_iso

    # IMPORTANT: queue for cron (this ONE opp only)
    opportunity["followUP_date"] = now_iso

    # Treat this as customer-initiated inbound so cadence can start cleanly
    checked = opportunity.setdefault("checkedDict", {})
    checked.setdefault("last_msg_by", "customer")


    # -----------------------------
    # 3) Store offer amount (optional) — make ADF authoritative
    # -----------------------------
    combined_body = "\n".join([body_text or "", body_html or ""])
    amt = _extract_kbb_amount(combined_body)
    log.info("KBB ADF: _extract_kbb_amount -> %r", amt)

    if amt:
        ctx = dict(opportunity.get("_kbb_offer_ctx") or {})
        ctx["amount_usd"] = amt  # overwrite ok
        opportunity["_kbb_offer_ctx"] = ctx

    # Make sure KBB state exists for cadence logic
    opportunity.setdefault("_kbb_state", {
        "mode": "cadence",
        "last_template_day_sent": None,
        "last_template_sent_at": None,
        "last_customer_msg_at": None,
        "last_agent_msg_at": None,
        "nudge_count": 0,
        "last_inbound_activity_id": None,
        "last_appt_activity_id": None,
        "appt_due_utc": None,
        "appt_due_local": None,
    })

    # -----------------------------
    # 4) TEST GATE (ONLY controls "active", not record creation)
    #    If not test, DO NOT set follow_up_at=now (prevents Due Now noise)
    # -----------------------------
    TEST_OPP_ID = "050a81e9-78d4-f011-814f-00505690ec8c"
    is_test = (opportunity.get("opportunityId") == TEST_OPP_ID) or (opportunity.get("id") == TEST_OPP_ID)

    follow_up_at = now_iso
    is_active = True

    if not is_test:
        # keep record for visibility, but do NOT wake cron while testing
        is_active = False
        follow_up_at = (_dt.now(_tz.utc) + _td(days=365)).isoformat()
        opportunity["isActive"] = False
        opportunity.setdefault("checkedDict", {})["exit_type"] = "not_test_opp"
        opportunity["followUP_date"] = follow_up_at

    # -----------------------------
    # 5) Upsert Airtable record (create OR update)
    # -----------------------------
    from airtable_store import upsert_lead, _safe_json_dumps
    
    rec = upsert_lead(opp_id, {
        "subscription_id": subscription_id,
        "source": opportunity.get("source") or "",
        "is_active": bool(is_active),
        "follow_up_at": follow_up_at,
        "mode": (opportunity.get("_kbb_state") or {}).get("mode", ""),
        "opp_json": _safe_json_dumps(opportunity),
    })
    
    # Attach Airtable record id so future save_opp() calls work
    opportunity["_airtable_rec_id"] = rec.get("id")
    
    log.info(
        "KBB ADF: upserted Airtable opp=%s rec_id=%s follow_up_at=%s is_active=%s",
        opp_id,
        opportunity["_airtable_rec_id"],
        follow_up_at,
        is_active,
    )

    # -----------------------------
    # 6) IMMEDIATE Day 1 send (webhook-driven)
    # -----------------------------
    # Only send if:
    # - record is active
    # - we haven't already sent Day 1
    # - customer email exists and is mailable
    kbb_state = opportunity.get("_kbb_state") or {}
    already_sent_day1 = bool(kbb_state.get("last_template_day_sent"))  # any value means we already sent something

    cust = opportunity.get("customer") or {}
    emails = cust.get("emails") or []
    cust_email_obj = next((e for e in emails if (e.get("address") or "").strip()), None)
    cust_email = (cust_email_obj.get("address") or "").strip() if cust_email_obj else None
    do_not_email = bool(cust_email_obj.get("doNotEmail")) if cust_email_obj else False

    if is_active and (not already_sent_day1) and cust_email and (not do_not_email):
        try:
            # --- Render Day 1 content ---
            # REPLACE THIS with your existing Day 1 template renderer.
            # It should return: subject, html_body, text_body
            subject_out, html_out, text_out = render_kbb_day1_email(opportunity)

            # Send via Outlook (Patti email), NOT Fortellis
            # Assumes your send_email handles the proper From based on rooftop/subscription_id
            send_email(
                to_email=cust_email,
                subject=subject_out,
                html_body=html_out,
                text_body=text_out,
                rooftop_key=subscription_id,   # if your send_email expects dealer key; adjust if needed
            )

            now2 = _dt.now(_tz.utc).isoformat()

            # Update state so cron will NOT resend Day 1
            opportunity.setdefault("checkedDict", {})["patti_already_contacted"] = True
            opportunity["checkedDict"]["last_msg_by"] = "patti"

            kbb_state = opportunity.setdefault("_kbb_state", {})
            kbb_state["mode"] = kbb_state.get("mode") or "cadence"
            kbb_state["last_template_day_sent"] = 1
            kbb_state["last_template_sent_at"] = now2
            kbb_state["last_agent_msg_at"] = now2
            kbb_state["nudge_count"] = kbb_state.get("nudge_count") or 0

            # Persist updated opp_json back to Airtable (NOW that _airtable_rec_id is attached)
            from airtable_store import save_opp
            save_opp(opportunity)

            # Log to Fortellis as "Send Email" completed activity (does NOT send)
            # If you decide this might affect response-time metrics incorrectly,
            # flip use_send_email_type=False to log as NOTE instead.
            from_email_for_log = "patti@pattersonautos.com"  # replace if you derive per rooftop
            log_completed_activity_email(
                token=token,
                subscription_id=subscription_id,
                opportunity_id=opp_id,
                customer_id=(cust.get("id") or customer_id),
                subject=subject_out,
                body_text=text_out or "",
                from_email=from_email_for_log,
                to_email=cust_email,
                use_send_email_type=True,
            )

            log.info("KBB ADF: Day 1 sent immediately to %s for opp=%s", cust_email, opp_id)

        except Exception as e:
            # Do NOT crash webhook; just log and let cron pick it up later
            log.exception("KBB ADF: immediate Day 1 send failed opp=%s err=%s", opp_id, e)
    else:
        log.info(
            "KBB ADF: skip immediate Day 1 send (is_active=%s already_sent=%s cust_email=%s do_not_email=%s)",
            is_active, already_sent_day1, bool(cust_email), do_not_email
        )


    # quick sanity log showing customer email we stored
    cust_email = None
    try:
        emails = (opportunity.get("customer") or {}).get("emails") or []
        cust_email = (emails[0].get("address") if emails else None)
    except Exception:
        cust_email = None

    log.info(
        "KBB ADF: upserted Airtable opp=%s sub=%s shopper_email=%s customer_email=%s follow_up_at=%s is_test=%s is_active=%s",
        opp_id, subscription_id, shopper_email, cust_email, follow_up_at, is_test, is_active
    )
    return
