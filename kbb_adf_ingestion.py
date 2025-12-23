# kbb_adf_ingestion.py
import re
import logging
from datetime import datetime, timezone

from fortellis import get_token, get_opportunity, find_recent_kbb_opportunity_by_email
from airtable_store import find_by_opp_id 

from email_ingestion import clean_html  
import json
from datetime import datetime as _dt, timezone as _tz

from airtable_store import (
    find_by_customer_email,   
    opp_from_record,
    save_opp,
)
# TEMP: while testing, only these rooftops
ALLOWED_SUBSCRIPTIONS = {
    "7a05ce2c-cf00-4748-b841-45b3442665a7",
    "c27d7f4f-4a4c-45c8-8154-a5de48421fc3",
}


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
    ...
    shopper_email = _extract_shopper_email(body_text)
    if not shopper_email:
        ...
        return

    # -----------------------------
    # 1) Fortellis lookup (NOT Airtable)
    # -----------------------------
    opp_id = None
    subscription_id = None

    for sub_id in ALLOWED_SUBSCRIPTIONS:
        try:
            tok = get_token(sub_id)
            found_id, _ = find_recent_kbb_opportunity_by_email(
                shopper_email=shopper_email,
                subscription_id=sub_id,
                token=tok,
                since_minutes=60 * 48,   # 48h window
            )
            if found_id:
                opp_id = found_id
                subscription_id = sub_id
                break
        except Exception as e:
            log.warning("KBB ADF: fortellis lookup failed sub=%s err=%s", sub_id, e)

    if not opp_id or not subscription_id:
        log.warning("KBB ADF: No Fortellis KBB opp found for shopper email %s", shopper_email)
        return

    # Pull full opp so Airtable has customer + everything processNewData expects
    token = get_token(subscription_id)
    fresh_opp = get_opportunity(opp_id, token, subscription_id) or {}

    # -----------------------------
    # 2) Build the opp_json we store in Airtable
    # -----------------------------
    now_iso = _dt.now(_tz.utc).isoformat()

    opportunity = dict(fresh_opp)
    opportunity["opportunityId"] = opportunity.get("opportunityId") or opportunity.get("id") or opp_id
    opportunity["id"] = opportunity.get("id") or opportunity["opportunityId"]
    opportunity["_subscription_id"] = subscription_id

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

    # IMPORTANT: queue for cron
    opportunity["followUP_date"] = now_iso

    # -----------------------------
    # 3) Store offer amount (optional)
    # -----------------------------
    combined_body = "\n".join([body_text or "", body_html or ""])
    amt = _extract_kbb_amount(combined_body)
    log.info("KBB ADF: _extract_kbb_amount -> %r", amt)

    if amt:
        ctx = dict(opportunity.get("_kbb_offer_ctx") or {})
        if not ctx.get("amount_usd"):
            ctx["amount_usd"] = amt
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
    # 4) TEST GATE
    #    Keep it, BUT apply it ONLY to whether cron should act.
    #    We STILL create the Airtable record either way.
    # -----------------------------
    TEST_OPP_ID = "050a81e9-78d4-f011-814f-00505690ec8c"
    is_test = (opportunity.get("opportunityId") == TEST_OPP_ID) or (opportunity.get("id") == TEST_OPP_ID)

    # If you want cron to ignore non-test while testing:
    if not is_test:
        opportunity["isActive"] = False  # prevents processNewData from sending
        opportunity.setdefault("checkedDict", {})["exit_type"] = "not_test_opp"

    # -----------------------------
    # 5) Save to Airtable (THIS is what was missing)
    #    follow_up_at is what your Due Now view uses.
    # -----------------------------
    save_opp(
        opportunity,
        extra_fields={
            "opp_id": opp_id,
            "subscription_id": subscription_id,
            "is_active": bool(opportunity.get("isActive", True)),
            "follow_up_at": now_iso,
            "source": opportunity.get("source") or "",
            "mode": (opportunity.get("_kbb_state") or {}).get("mode", ""),
            "opp_json": json.dumps(opportunity, ensure_ascii=False),
        },
    )

    log.info("KBB ADF: upserted Airtable opp=%s sub=%s follow_up_at=%s is_test=%s",
             opp_id, subscription_id, now_iso, is_test)
    return
