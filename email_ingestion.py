#email_ingestion.py
import os
import re
import logging
from datetime import datetime as _dt, timezone as _tz
import json

from rooftops import get_rooftop_info
from fortellis import (
    get_token,
    add_opportunity_comment,
    get_opportunity,
    search_customers_by_email,
    get_opps_by_customer_id,
)

from kbb_ico import _top_reply_only
from airtable_store import (
    find_by_opp_id,
    find_by_customer_email,
    opp_from_record,
    save_opp,
    upsert_lead,
    _safe_json_dumps,
)


log = logging.getLogger("patti.email_ingestion")

# For now we only want this running on your single test opp
TEST_OPP_ID = "050a81e9-78d4-f011-814f-00505690ec8c"

DEFAULT_SUBSCRIPTION_ID = os.getenv("DEFAULT_SUBSCRIPTION_ID")  # set this to Tustin Kia's subscription id

def _resolve_subscription_id(inbound: dict, headers: dict) -> str | None:
    # If your Flow sends it, use it.
    sid = (
        inbound.get("subscription_id")
        or inbound.get("SubscriptionId")
        or headers.get("X-Subscription-ID")
    )
    if sid:
        return sid

    # Otherwise fall back to deployment default (Patti@ = Tustin Kia)
    return DEFAULT_SUBSCRIPTION_ID

def _find_best_active_opp_for_email(*, shopper_email: str, token: str, subscription_id: str) -> str | None:
    target = (shopper_email or "").strip().lower()
    if not target:
        return None

    customers = search_customers_by_email(target, token, subscription_id, page_size=10) or []
    if not customers:
        return None

    candidates = []

    for c in customers:
        cid = c.get("id") or c.get("customerId")
        if not cid:
            continue

        opps = get_opps_by_customer_id(cid, token, subscription_id, page_size=100) or []
        for o in opps:
            status = (o.get("status") or "").strip().lower()
            if status != "active":
                continue

            opp_id = o.get("id") or o.get("opportunityId")
            if not opp_id:
                continue

            # pick most recently touched
            dt_str = (
                o.get("updatedAt")
                or o.get("updated_at")
                or o.get("createdAt")
                or o.get("created_at")
                or ""
            )
            candidates.append((str(dt_str), opp_id))

    if not candidates:
        return None

    candidates.sort(reverse=True)
    return candidates[0][1]

def _safe_mode_from(inbound: dict) -> bool:
    # Prefer the PA flag
    if inbound.get("test_mode") is True:
        return True

    # Fall back to Render env vars
    return (os.getenv("PATTI_SAFE_MODE", "0") == "1") or (os.getenv("SAFE_MODE", "0") == "1")


def clean_html(html: str) -> str:
    """Strip HTML tags and reduce to plain text."""
    text = re.sub(r"(?is)<[^>]+>", " ", html or "")
    return re.sub(r"\s+", " ", text).strip()

def _extract_email(addr: str) -> str:
    """
    Given "Kristin <foo@bar.com>" or just "foo@bar.com" return lowercase email.
    """
    if not addr:
        return ""
    m = re.search(r"<([^>]+)>", addr)
    email = m.group(1) if m else addr
    return email.strip().lower()


def _compute_lead_age_days(opportunity: dict) -> int:
    """
    Copy of the lead_age_days logic from processNewData.py
    so kbb_ico sees the same value.
    """
    lead_age_days = 0
    created_raw = (
        opportunity.get("dateIn")
        or opportunity.get("createdDate")
        or opportunity.get("created_at")
        or (opportunity.get("firstActivity") or {}).get("completedDate")
    )
    try:
        if created_raw:
            created_dt = _dt.fromisoformat(str(created_raw).replace("Z", "+00:00"))
            lead_age_days = (_dt.now(_tz.utc) - created_dt).days
    except Exception:
        pass
    return lead_age_days


def _find_opportunity_by_sender(sender_email: str):
    """
    Find opportunity in Airtable by matching the sender email against
    opp_json.customer.emails[].address (or a stored customer_email column if you have one).
    """
    if not sender_email:
        return None, None

    rec = find_by_customer_email(sender_email)  # you’ll add this in airtable_store.py
    if not rec:
        return None, None

    opp = opp_from_record(rec)
    return opp.get("opportunityId") or opp.get("id"), opp



def is_test_opp(opp: dict, opp_id: str | None) -> bool:
    if opp_id and opp_id == TEST_OPP_ID:
        return True
    if opp and opp.get("opportunityId") == TEST_OPP_ID:
        return True
    if opp and opp.get("id") == TEST_OPP_ID:
        return True
    return False


def process_inbound_email(inbound: dict) -> None:
    """
    Entry point called from web_app.py when Power Automate POSTs a
    "new email" JSON payload.

    Goal:
      - Resolve the opportunity
      - Append this message to opportunity["messages"]
      - Call process_kbb_ico_lead so the existing KBB brain decides
        what (if anything) to send next.
    """
    sender_raw = (inbound.get("from") or "").strip()
    subject = inbound.get("subject") or ""
    
    body_html = inbound.get("body_html") or ""
    raw_text = inbound.get("body_text") or clean_html(body_html)

    # Start with raw text as a fallback
    body_text = raw_text

    # 1️⃣ Try KBB's HTML reply-stripper first (when we actually have HTML)
    if body_html:
        try:
            top_html = _top_reply_only(body_html) or ""
            stripped = clean_html(top_html)
            # Only use it if we got something non-empty back
            if stripped:
                body_text = stripped
        except Exception:
            # If anything weird happens, just stick with raw_text
            pass

    # 2️⃣ Plain-text reply stripping for Outlook-style separators
    body_text = (body_text or "").strip()

    # Cut off everything after common reply delimiters so KBB only sees
    # the *new* line like "What was the kbb estimate?"
    for sep in [
        "\r\n________________________________",
        "\n________________________________",

        # HTML-cleaned versions (no underscores / newlines)
        " From:",
        " Sent:",
        " On ",
        " Subject:",
        " To:",

        # Raw newline forms, in case they survive
        "\r\nFrom:",
        "\nFrom:",
        "\r\nOn ",
        "\nOn ",
    ]:
        idx = body_text.find(sep)
        if idx != -1:
            body_text = body_text[:idx].strip()
            break


    # Optional but useful while testing:
    log.info(
        "Email ingestion text debug: raw=%r final=%r",
        (raw_text or "")[:160],
        (body_text or "")[:160],
    )

    ts = inbound.get("timestamp") or _dt.now(_tz.utc).isoformat()
    headers = inbound.get("headers") or {}

    # 1) find opp
    subscription_id = _resolve_subscription_id(inbound, headers)
    if not subscription_id:
        log.warning("No subscription_id resolved; cannot lookup opp in Fortellis")
        return
    
    tok = get_token(subscription_id)
    
    # Prefer header opp_id if present (nice when available)
    opp_id = headers.get("X-Opportunity-ID") or headers.get("x-opportunity-id")
    
    # If missing, lookup opp_id in Fortellis by sender email
    if not opp_id:
        sender_email = _extract_email(sender_raw)
        opp_id = _find_best_active_opp_for_email(
            shopper_email=sender_email,
            token=tok,
            subscription_id=subscription_id,
        )
        if not opp_id:
            log.warning("No active opp found in Fortellis for sender=%s (sub=%s)", sender_raw, subscription_id)
            return
    
    # Now try Airtable
    rec = find_by_opp_id(opp_id)
    if rec:
        opportunity = opp_from_record(rec)
    else:
        # Bootstrap from Fortellis by opp_id, then create Airtable lead
        opp = get_opportunity(opp_id, tok, subscription_id)
        opp["_subscription_id"] = subscription_id
    
        now_iso = inbound.get("timestamp") or _dt.now(_tz.utc).isoformat()
        opp.setdefault("followUP_date", now_iso)
    
        upsert_lead(opp_id, {
            "subscription_id": subscription_id,
            "source": opp.get("source") or "",
            "is_active": bool(opp.get("isActive", True)),
            "follow_up_at": opp.get("followUP_date"),
            "mode": "",
            "opp_json": _safe_json_dumps(opp),
        })
    
        rec2 = find_by_opp_id(opp_id)
        if not rec2:
            log.warning("Bootstrap upsert did not produce record opp=%s", opp_id)
            return
    
        opportunity = opp_from_record(rec2)

    
    # 2) Append inbound message into the thread (in-memory)
    ts = inbound.get("timestamp") or _dt.now(_tz.utc).isoformat()
    msg_dict = {
        "msgFrom": "customer",
        "subject": subject,
        "body": body_text,
        "date": ts,
    }
    opportunity.setdefault("messages", []).append(msg_dict)
    
    # 3) Mark inbound + set KBB convo signals
    now_iso = ts  # use the inbound timestamp we already computed
    opportunity.setdefault("checkedDict", {})["last_msg_by"] = "customer"
    opportunity["followUP_date"] = now_iso  # due now
    
    st = opportunity.setdefault("_kbb_state", {})
    st["mode"] = "convo"
    st["last_customer_msg_at"] = now_iso

    # 4) Persist to Airtable (save_opp already updates follow_up_at + opp_json)
    save_opp(opportunity)

    # 5) IMMEDIATE reply (do NOT wait for cron)
    try:
        from kbb_ico import process_kbb_ico_lead
        from rooftops import get_rooftop_info

        subscription_id = opportunity.get("_subscription_id")
        if not subscription_id:
            log.warning("Inbound email matched opp=%s but missing _subscription_id; cannot reply", opp_id)
            return

        tok = get_token(subscription_id)

        rt = get_rooftop_info(subscription_id) or {}
        rooftop_name   = rt.get("name") or rt.get("rooftop_name") or "Rooftop"
        rooftop_sender = rt.get("sender") or rt.get("patti_email") or None

        # Let the brain answer the customer's question immediately
        safe_mode = _safe_mode_from(inbound)
        state, action_taken = process_kbb_ico_lead(
            opportunity=opportunity,
            lead_age_days=0,              # not important for convo replies
            rooftop_name=rooftop_name,
            inquiry_text=body_text,       # <-- this is the customer's question
            token=tok,
            subscription_id=subscription_id,
            SAFE_MODE=safe_mode,
            rooftop_sender=rooftop_sender,
            trigger="email_webhook",        
            inbound_ts=ts,                  
            inbound_subject=subject, 
        )

        # Persist any state changes + schedule parking if convo logic does that
        if isinstance(state, dict):
            opportunity["_kbb_state"] = state
        save_opp(opportunity)

        log.info("Inbound email processed immediately opp=%s action_taken=%s", opp_id, action_taken)

    except Exception as e:
        log.exception("Immediate inbound reply failed opp=%s err=%s", opp_id, e)

    # 6) Optional: log inbound email to CRM as a comment (Fortellis writeback)
    subscription_id = opportunity.get("_subscription_id")
    if subscription_id:
        try:
            token = get_token(subscription_id)
            preview = (body_text or "")[:500]
            add_opportunity_comment(
                token,
                subscription_id,
                opp_id,
                f"Inbound email from {sender_raw}: {subject}\n\n{preview}",
            )
        except Exception as e:
            log.warning("Failed to log inbound email comment opp=%s err=%s", opp_id, e)

    log.info("Inbound email queued + processed immediately for opp=%s", opp_id)
    return
