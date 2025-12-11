import os
import re
import logging
from datetime import datetime as _dt, timezone as _tz

from esQuerys import esClient
from es_resilient import es_update_with_retry
from rooftops import get_rooftop_info
from fortellis import get_token, add_opportunity_comment
from kbb_ico import process_kbb_ico_lead
from kbb_ico import _top_reply_only

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
    Look up the opportunity in ES by customer email.

    We intentionally DO NOT use nested here because the mapping
    shows customer.emails as a simple object array.
    """
    if not sender_email:
        return None, None

    query = {
        "bool": {
            "should": [
                {"term": {"customer.emails.address.keyword": sender_email}},
                {"term": {"customer.emails.address": sender_email}},
                {"term": {"customerEmail.keyword": sender_email}},
                {"term": {"customerEmail": sender_email}},
            ],
            "minimum_should_match": 1,
        }
    }

    rsp = esClient.search(index="opportunities", query=query, size=1)
    hits = rsp.get("hits", {}).get("hits", [])
    if not hits:
        return None, None

    doc = hits[0]
    return doc["_id"], doc["_source"]


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

    # Use KBB's reply-stripper so we only see the *new* message,
    # not the quoted appointment email with "Wed at 2PM" in it.
    if body_html:
        try:
            top_html = _top_reply_only(body_html)
            body_text = clean_html(top_html)
        except Exception:
            # If anything weird happens, fall back to the raw text
            body_text = raw_text
    else:
        body_text = raw_text

    ts = inbound.get("timestamp") or _dt.now(_tz.utc).isoformat()
    headers = inbound.get("headers") or {}

    # 1️⃣ Try direct opp id from header
    opp_id = headers.get("X-Opportunity-ID") or None

    if opp_id:
        doc = esClient.get(index="opportunities", id=opp_id)
        opportunity = doc["_source"]
    else:
        # 2️⃣ Fallback: match sender email to customer.emails / customerEmail
        sender_email = _extract_email(sender_raw)
        opp_id, opportunity = _find_opportunity_by_sender(sender_email)
        if not opp_id:
            log.warning("No matching opportunity found for inbound email %s", sender_email)
            return

    # 🔒 Hard gate: only run this Outlook-based path on your single test opp
    if not is_test_opp(opportunity, opp_id):
        log.info("Inbound email for opp %s is not TEST_OPP_ID; skipping", opp_id)
        return

    # 3️⃣ Append the inbound message into the ES conversation thread
    msg_dict = {
        "msgFrom": "customer",
        "subject": subject,
        "body": body_text,
        "date": ts,
    }
    opportunity.setdefault("messages", []).append(msg_dict)
    
    # Load current state but do NOT touch last_customer_msg_at here.
    state = dict(opportunity.get("_kbb_state") or {})
    opportunity["_kbb_state"] = state
    
    # Persist the new message thread only; KBB will update _kbb_state itself.
    es_update_with_retry(
        esClient,
        index="opportunities",
        id=opp_id,
        doc={"messages": opportunity["messages"]},
    )
    
    # 4️⃣ Compute the same lead_age_days we use in processNewData
    lead_age_days = _compute_lead_age_days(opportunity)
    
    # 5️⃣ Get rooftop + token info
    subscription_id = opportunity.get("_subscription_id")
    if not subscription_id:
        log.warning("Opportunity %s missing _subscription_id; cannot run KBB flow", opp_id)
        return
    
    rooftop_info = get_rooftop_info(subscription_id)
    rooftop_name = rooftop_info["name"]
    rooftop_sender = rooftop_info["sender"]
    
    token = None
    if os.getenv("OFFLINE_MODE", "0") not in ("1", "true", "True"):
        token = get_token(subscription_id)
    
    # 3b️⃣ Log inbound email to CRM as a comment (now token IS defined)
    if token:
        try:
            preview = (body_text or "")[:500]
            add_opportunity_comment(
                token,
                subscription_id,
                opp_id,
                f"Inbound email from {sender_raw}: {subject}\n\n{preview}",
            )
        except Exception as e:
            log.warning(
                "Failed to log inbound email as CRM comment for opp %s: %s",
                opp_id,
                e,
            )


    # 6️⃣ Hand off to the existing KBB ICO logic
    # Use Outlook webhook trigger; this call itself *is* the new inbound signal
    inbound_ts = ts
    inbound_subject = subject
    inbound_msg_id = headers.get("Message-Id") or f"esmsg:{inbound_ts}"

    state, action_taken = process_kbb_ico_lead(
        opportunity=opportunity,
        lead_age_days=lead_age_days,
        rooftop_name=rooftop_name,
        inquiry_text=body_text,          # customer's reply text
        token=token,
        subscription_id=subscription_id,
        SAFE_MODE=False,
        rooftop_sender=rooftop_sender,
        trigger="email_webhook",
        inbound_ts=inbound_ts,
        inbound_msg_id=inbound_msg_id,
        inbound_subject=inbound_subject,
    )

    # 7️⃣ Persist any mutations KBB logic made to the opportunity
    try:
        es_update_with_retry(
            esClient,
            index="opportunities",
            id=opp_id,
            doc=opportunity,
        )
    except Exception as e:
        log.warning(
            "ES persist failed after KBB processing opp %s: %s",
            opp_id,
            e,
        )

    log.info(
        "KBB email ingestion handled for opp %s – action_taken=%s, state[last_template_day_sent]=%s",
        opp_id,
        bool(action_taken),
        (state or {}).get("last_template_day_sent"),
    )
