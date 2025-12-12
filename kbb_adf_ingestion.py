# kbb_adf_ingestion.py
import re
import logging
from datetime import datetime, timezone

from esQuerys import esClient
from feature_flags import is_test_opp
from fortellis import get_token
from rooftops import get_rooftop_info
from email_ingestion import clean_html  
from kbb_ico import process_kbb_ico_lead


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

    # Find KBB opportunity by shopper email
    query = {
        "bool": {
            "must": [
                {
                    "terms": {
                        "source.keyword": [
                            "KBB Instant Cash Offer",
                            "KBB ServiceDrive",
                        ]
                    }
                }
            ],
            "should": [
                # main ES structure
                {"term": {"customer.emails.address.keyword": shopper_email}},
                {"term": {"customer.emails.address": shopper_email}},
                # older / alternate mappings, just in case
                {"term": {"customerEmail.keyword": shopper_email}},
                {"term": {"customer.email.keyword": shopper_email}},
                {"term": {"customerEmail": shopper_email}},
            ],

            "minimum_should_match": 1,
        }
    }

    rsp = esClient.search(
        index="opportunities",
        query=query,
        size=1,
        sort=[{"created_at": {"order": "desc"}}],
    )
    hits = rsp["hits"]["hits"]
    if not hits:
        log.warning("No KBB opportunity found for shopper email %s", shopper_email)
        return

    hit = hits[0]
    opp_id = hit["_id"]
    opportunity = hit["_source"]

    # Try to capture the KBB amount from the ADF email body and persist it on the opp
    # so later KBB flows can answer "what was my estimate?" deterministically.
    combined_body = "\n".join([
        body_text or "",
        body_html or "",
    ])
    
    amt = _extract_kbb_amount(combined_body)
    log.info("KBB ADF: _extract_kbb_amount len=%d -> %r", len(combined_body), amt)
    
    if amt:
        ctx = dict(opportunity.get("_kbb_offer_ctx") or {})
        # Don't overwrite if we already have an amount
        if not ctx.get("amount_usd"):
            ctx["amount_usd"] = amt
            opportunity["_kbb_offer_ctx"] = ctx
            try:
                esClient.update(
                    index="opportunities",
                    id=opp_id,
                    body={"doc": {"_kbb_offer_ctx": ctx}},
                )
                log.info("KBB ADF: stored offer amount %s for opp %s", amt, opp_id)
            except Exception as e:
                log.warning("KBB ADF: failed to store _kbb_offer_ctx for opp %s: %s", opp_id, e)


    # Gate: only run Outlook Patti for test opps on this branch
    if not is_test_opp(opportunity):
        log.info(
            "KBB ADF inbound matched opp %s but it's not a test opp; skipping Outlook flow",
            opp_id,
        )
        return

    # --- Use the main KBB engine instead of hand-rolled GPT ---

    dealer_key = opportunity.get("_subscription_id")
    if not dealer_key:
        log.warning("KBB opp %s missing _subscription_id; cannot send Patti email", opp_id)
        return
    
    token = get_token(dealer_key)
    
    # Rooftop info (name + sender email)
    rt_info = get_rooftop_info(dealer_key) or {}
    rooftop_name   = rt_info.get("name") or (opportunity.get("rooftop_name") or opportunity.get("rooftop") or "Patterson Auto Group")
    rooftop_sender = rt_info.get("sender") or ""
    
    # Lead age in days (so KBB cadence picks Day 1 vs later)
    created_iso = opportunity.get("createdDate") or opportunity.get("created_on")
    lead_age_days = 0
    if created_iso:
        try:
            created_dt = datetime.fromisoformat(str(created_iso).replace("Z", "+00:00")).astimezone(timezone.utc)
            lead_age_days = (datetime.now(timezone.utc) - created_dt).days
        except Exception:
            log.warning("KBB ADF: could not parse created date %r", created_iso)
    
    # For a fresh ADF lead there’s usually no “question” yet, so inquiry_text can be blank
    inquiry_text = ""
    
    state, action_taken = process_kbb_ico_lead(
        opportunity=opportunity,
        lead_age_days=lead_age_days,
        rooftop_name=rooftop_name,
        inquiry_text=inquiry_text,
        token=token,
        subscription_id=dealer_key,
        SAFE_MODE=False,
        rooftop_sender=rooftop_sender,
        trigger="kbb_adf",
    )
    
    log.info("KBB ADF → process_kbb_ico_lead finished: opp=%s action_taken=%s mode=%s",
             opp_id, action_taken, (state or {}).get("mode"))
