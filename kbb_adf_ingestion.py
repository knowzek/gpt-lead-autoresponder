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

import requests
from datetime import datetime, timezone

FORTELLIS_BASE = "https://api.fortellis.io"
COMPLETE_ACTIVITY_URL = f"{FORTELLIS_BASE}/sales/v1/elead/activities/complete"

def complete_send_email_activity(*, token: str, subscription_id: str,
                                 opportunity_id: str, customer_id: str,
                                 subject: str, body_text: str,
                                 from_email: str | None = None) -> dict:
    """
    Logs a completed 'Send Email' activity in Fortellis WITHOUT sending an email.
    This should stop the dealership response-time clock (per your requirement).
    """

    now = datetime.now(timezone.utc).isoformat()

    payload = {
        "opportunityId": opportunity_id,
        "customerId": customer_id,
        "activityTypeId": 3,  # "send email"
        "completedDate": now,
        "comments": f"Patti sent Day 1 ICO email via Outlook. Subject: {subject}",
        # Some tenants accept message object; safe to include (Fortellis may ignore unknown fields)
        "message": {
            "subject": subject,
            "body": body_text,
        }
    }

    # Optional: capture sender used
    if from_email:
        payload["comments"] += f" | From: {from_email}"

    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": subscription_id,
        "Request-Id": f"patti-{opportunity_id}-{int(datetime.now().timestamp())}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    r = requests.post(COMPLETE_ACTIVITY_URL, headers=headers, json=payload, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"Complete Activity failed {r.status_code}: {r.text[:800]}")
    return r.json()


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



EMAIL_RE = re.compile(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", re.I)

def _extract_shopper_email(body_text: str) -> str | None:
    """
    Parse KBB ADF notification text to find shopper email.

    Handles:
      - Email: foo@bar.com
      - email appearing on its own line (common in KBB alerts)
    """
    if not body_text:
        return None

    # 1) Preferred: explicit Email: label
    m = re.search(r"(?im)^\s*email\s*:\s*([^\s<]+@[^\s<]+)\s*$", body_text)
    if m:
        return m.group(1).strip().lower()

    # 2) Fallback: any standalone email in body
    matches = EMAIL_RE.findall(body_text)
    if not matches:
        return None

    # Filter out system / sender addresses
    block = {
        "reply@messages.kbb.com",
        "noreply@kbb.com",
        "noreplylead@carfax.com",
        "patti@pattersonautos.com",
    }

    for email in matches:
        e = email.strip().lower()
        if e in block:
            continue
        if "kbb" in e or "pattersonautos.com" in e:
            continue
        return e

    # Last-resort fallback
    return matches[0].strip().lower()


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

    follow_up_at = now_iso
    opportunity["isActive"] = True
    opportunity["followUP_date"] = follow_up_at

    # -----------------------------
    # 4.5) Compute is_active once (used by Airtable + logs)
    # -----------------------------
    status = (opportunity.get("status") or "Active").strip().lower()
    substatus = (opportunity.get("subStatus") or opportunity.get("substatus") or "New").strip().lower()

    # Treat Lost/Inactive as not active; otherwise active
    is_active = (status == "active") and (substatus not in {"lost", "inactive", "closed"})


    # -----------------------------
    # 5) Upsert Airtable record (create OR update)
    # -----------------------------
    from airtable_store import upsert_lead, _safe_json_dumps
    
    rec = upsert_lead(opp_id, {
        "subscription_id": subscription_id,
        "source": opportunity.get("source") or "",
        "is_active": True,
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
    # 6) IMMEDIATE Day 1 send (ADF-triggered)
    # -----------------------------
    try:
        from kbb_ico import process_kbb_ico_lead
        from rooftops import get_rooftop_info
        from airtable_store import save_opp
    
        # Only send immediately if we haven't already sent Day 1
        state = opportunity.setdefault("_kbb_state", {})
        already_sent_day = state.get("last_template_day_sent")
        if already_sent_day:
            log.info(
                "KBB ADF: skipping immediate send (already sent day=%s) opp=%s",
                already_sent_day, opp_id
            )
            return
    
        rt = get_rooftop_info(subscription_id) or {}
        rooftop_name   = rt.get("name") or rt.get("rooftop_name") or "Rooftop"
        rooftop_sender = rt.get("sender") or rt.get("patti_email") or None
    
        tok = get_token(subscription_id)
    
        log.info("KBB ADF: triggering immediate Day 1 send opp=%s sub=%s", opp_id, subscription_id)
    
        # Make Day 1 eligible
        state.setdefault("mode", "cadence")
        state.setdefault("last_template_day_sent", None)
        state.setdefault("nudge_count", 0)
    
        state, action_taken = process_kbb_ico_lead(
            opportunity=opportunity,
            lead_age_days=0,          # ADF just arrived
            rooftop_name=rooftop_name,
            inquiry_text="",          # Day 1 is a kickoff, not a reply
            token=tok,
            subscription_id=subscription_id,
            SAFE_MODE=False,          # allow sending
            rooftop_sender=rooftop_sender,
        )
    
        if action_taken:
            # Make future runs idempotent
            opportunity["_kbb_state"] = state
            checked = opportunity.setdefault("checkedDict", {})
            checked["patti_already_contacted"] = True
            checked["last_msg_by"] = "patti"
    
            # ✅ schedule next nudge + persist (THIS updates Airtable follow_up_at)
            next_follow = (_dt.now(_tz.utc) + _td(days=1)).isoformat()
            opportunity["followUP_date"] = next_follow
    
            save_opp(opportunity)
    
            log.info("KBB ADF: Day 1 sent; scheduled next follow_up_at=%s opp=%s", next_follow, opp_id)
    
    except Exception as e:
        log.exception("KBB ADF: immediate Day 1 send failed opp=%s err=%s", opp_id, e)

    # quick sanity log showing customer email we stored
    cust_email = None
    try:
        emails = (opportunity.get("customer") or {}).get("emails") or []
        cust_email = (emails[0].get("address") if emails else None)
    except Exception:
        cust_email = None

    log.info(
        "KBB ADF: upserted Airtable opp=%s sub=%s shopper_email=%s customer_email=%s follow_up_at=%s is_active=%s",
        opp_id, subscription_id, shopper_email, cust_email, follow_up_at, is_active
    )

    return
