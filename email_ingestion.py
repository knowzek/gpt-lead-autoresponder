#email_ingestion.py
import os
import re
import logging
from datetime import datetime as _dt, timezone as _tz
import json

from rooftops import get_rooftop_info
from fortellis import get_token, add_opportunity_comment
from kbb_ico import _top_reply_only
from airtable_store import (
    find_by_opp_id,
    find_by_customer_email,   # you will add this helper (below)
    opp_from_record,
    save_opp,
)


log = logging.getLogger("patti.email_ingestion")

# For now we only want this running on your single test opp
TEST_OPP_ID = "050a81e9-78d4-f011-814f-00505690ec8c"



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

    # 1) Try direct opp id from header
    opp_id = headers.get("X-Opportunity-ID") or None
    
    opportunity = None
    
    if opp_id:
        rec = find_by_opp_id(opp_id)
        if rec:
            opportunity = opp_from_record(rec)
    else:
        sender_email = _extract_email(sender_raw)
        opp_id, opportunity = _find_opportunity_by_sender(sender_email)
    
    if not opp_id or not opportunity:
        log.warning("No matching opportunity found for inbound email from=%s", sender_raw)
        return
    
    # 🔒 Optional test gate (keep during Phase 2 if you want)
    if not is_test_opp(opportunity, opp_id):
        log.info("Inbound email for opp %s is not TEST_OPP_ID; skipping", opp_id)
        return
    
    # 2) Append inbound message into the thread (in-memory)
    ts = inbound.get("timestamp") or _dt.now(_tz.utc).isoformat()
    msg_dict = {
        "msgFrom": "customer",
        "subject": subject,
        "body": body_text,
        "date": ts,
    }
    opportunity.setdefault("messages", []).append(msg_dict)
    
    # 3) Mark “new inbound” so processNewData will respond next run
    opportunity.setdefault("checkedDict", {})["last_msg_by"] = "customer"
    opportunity["followUP_date"] = _dt.now(_tz.utc).isoformat()
    
        # 3) Mark inbound + set KBB convo signals (so nudges stop, replies can happen)
    now_iso = _dt.now(_tz.utc).isoformat()
    opportunity.setdefault("checkedDict", {})["last_msg_by"] = "customer"
    opportunity["followUP_date"] = now_iso  # make Due Now immediately

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
        state, action_taken = process_kbb_ico_lead(
            opportunity=opportunity,
            lead_age_days=0,              # not important for convo replies
            rooftop_name=rooftop_name,
            inquiry_text=body_text,       # <-- this is the customer's question
            token=tok,
            subscription_id=subscription_id,
            SAFE_MODE=False,              # <-- allow send now
            rooftop_sender=rooftop_sender,
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
