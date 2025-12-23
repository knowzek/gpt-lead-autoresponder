# kbb_adf_ingestion.py
import re
import logging
from datetime import datetime, timezone

from email_ingestion import clean_html  
import json
from datetime import datetime as _dt, timezone as _tz

from airtable_store import (
    find_by_customer_email,   
    opp_from_record,
    save_opp,
)


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


def process_kbb_adf_notification(inbound: dict) -> None:
    """
    Handle the initial 'Offer Created from KBB' email.

    We:
      1. Extract shopper email from the ADF notification body.
      2. Find the most recent KBB opportunity in ES for that email.
      3. If it's a test opp, send the first Patti email via Outlook and log to CRM.
    """
    subject = inbound.get("subject") or ""
    body_html = inbound.get("body_html") or ""
    body_text = inbound.get("body_text") or clean_html(body_html)

    # Helpful debug so we can see the raw ADF content when debugging amount parsing
    log.info("KBB ADF: raw body_text sample: %r", (body_text or "")[:500])

    shopper_email = _extract_shopper_email(body_text)
    if not shopper_email:
        log.warning("KBB ADF inbound had no shopper email; subject=%s", subject)
        return

    # -----------------------------
    # Airtable lookup instead of ES
    # -----------------------------
    rec = find_by_customer_email(shopper_email)
    if not rec:
        log.warning("No Airtable opportunity found for shopper email %s", shopper_email)
        return

    opportunity = opp_from_record(rec)

    # opp_id should come from the JSON blob
    opp_id = opportunity.get("opportunityId") or opportunity.get("id")
    if not opp_id:
        log.warning("Airtable record matched email %s but opp_json missing opportunityId/id", shopper_email)
        return

    # Optional: strict KBB-only gate (recommended)
    src = (opportunity.get("source") or "").strip().lower()
    if not src.startswith("kbb"):
        log.warning("Airtable record matched email %s but source=%r is not KBB; skipping", shopper_email, opportunity.get("source"))
        return


    # Try to capture the KBB amount from the ADF email body and persist it on the opp
    # so later KBB flows can answer "what was my estimate?" deterministically.
    combined_body = "\n".join([
        body_text or "",
        body_html or "",
    ])
    
    amt = _extract_kbb_amount(combined_body)
    log.info("KBB ADF: _extract_kbb_amount len=%d -> %r", len(combined_body), amt)
    
    # -----------------------------
    # Store the offer amount on the opp_json (Airtable)
    # -----------------------------
    if amt:
        ctx = dict(opportunity.get("_kbb_offer_ctx") or {})
        if not ctx.get("amount_usd"):
            ctx["amount_usd"] = amt
            opportunity["_kbb_offer_ctx"] = ctx
            log.info("KBB ADF: stored offer amount %s for opp %s (Airtable)", amt, opp_id)


    # -----------------------------
    # TEST GATE (KEEP THIS)
    # -----------------------------
    TEST_OPP_ID = "050a81e9-78d4-f011-814f-00505690ec8c"
    
    if (opportunity.get("opportunityId") != TEST_OPP_ID) and (opportunity.get("id") != TEST_OPP_ID):
        log.info("KBB ADF matched opp %s but not TEST_OPP_ID; skipping", opp_id)
        return
    
    
    # -----------------------------
    # QUEUE FOR CRON (DO NOT SEND)
    # -----------------------------
    now_iso = _dt.now(_tz.utc).isoformat()
    
    # mark due immediately
    opportunity["followUP_date"] = now_iso
    opportunity["isActive"] = True
    
    # mark that customer initiated (important for cadence logic)
    opportunity.setdefault("checkedDict", {})["last_msg_by"] = "customer"
    
    # persist ONLY to Airtable
    save_opp(
        opportunity,
        extra_fields={
            "opp_id": opp_id,
            "subscription_id": opportunity.get("_subscription_id") or "",
            "is_active": True,
            "follow_up_at": now_iso,
            "source": opportunity.get("source") or "",
            "opp_json": json.dumps(opportunity, ensure_ascii=False),
        },
    )
    
    log.info("KBB ADF queued in Airtable for opp %s (follow_up_at=%s)", opp_id, now_iso)
    return


    # -----------------------------
    # Queue for processNewData.py
    # -----------------------------
    now_iso = _dt.now(_tz.utc).isoformat()

    opportunity["followUP_date"] = now_iso
    opportunity["isActive"] = True
    opportunity.setdefault("checkedDict", {})["last_msg_by"] = "customer"

    # Save back to Airtable and make it show in Due Now
    save_opp(
        opportunity,
        extra_fields={
            "opp_id": opp_id,
            "subscription_id": opportunity.get("_subscription_id") or "",
            "is_active": True,
            "follow_up_at": now_iso,
            "source": opportunity.get("source") or "",
            "mode": (opportunity.get("_kbb_state") or {}).get("mode", ""),
            "opp_json": json.dumps(opportunity, ensure_ascii=False),
        },
    )

    log.info("KBB ADF queued in Airtable for opp %s (follow_up_at=%s)", opp_id, now_iso)
    return

