# sms_poller.py
import os
import logging
from datetime import datetime, timezone

from goto_sms import list_conversations, send_sms
from airtable_store import (
    find_by_customer_phone_loose,
    opp_from_record,
    save_opp,
)
from sms_brain import generate_sms_reply


log = logging.getLogger("patti.sms.poller")

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _sms_test_enabled() -> bool:
    return (os.getenv("SMS_TEST", "0").strip() == "1")

def _sms_test_to() -> str:
    return os.getenv("SMS_TEST_TO", "").strip()

def _patti_number() -> str:
    return os.getenv("PATTI_SMS_NUMBER", "+17145977229").strip()

def poll_once():
    """
    One polling pass:
    - pull latest conversations for Patti’s number
    - if lastMessage is inbound and new, flip mode=convo and reply (optional)
    """
    owner = _patti_number()

    data = list_conversations(owner_phone_e164=owner)
    items = data.get("items") or []
    log.info("SMS poll: got %d conversations", len(items))

    for conv in items:
        last = conv.get("lastMessage") or {}
        if not last:
            continue

        if (last.get("direction") or "").upper() != "IN":
            continue

        msg_id = last.get("id") or ""
        body = (last.get("body") or "").strip()
        author = last.get("authorPhoneNumber") or ""

        # Find the lead by author phone (customer)
        rec = find_by_customer_phone_loose(author)
        if not rec:
            log.info("SMS poll: no lead match for author=%s body=%r", author, body[:80])
            continue

        opp = opp_from_record(rec)
        patti = opp.setdefault("patti", {})

        # Dedupe: only act once per inbound message id
        last_seen = (opp.get("last_sms_inbound_message_id") or "").strip()
        if last_seen == msg_id:
            continue

        # Record inbound + switch mode
        patti["mode"] = "convo"
        extra = {
            "last_sms_inbound_message_id": msg_id,
            "last_sms_inbound_at": last.get("timestamp") or _now_iso(),
            "mode": "convo",
        }
        save_opp(opp, extra_fields=extra)

        log.info("SMS poll: new inbound msg opp=%s author=%s body=%r",
                 opp.get("opportunityId"), author, body[:120])

        # Build GPT reply
        decision = generate_sms_reply(
            opp=opp,
            inbound_text=body,
            include_optout_footer=False,  # guest already replied; no footer
        )
        
        reply_text = (decision.get("reply")).strip()


        to_number = author
        if _sms_test_enabled() and _sms_test_to():
            to_number = _sms_test_to()

        try:
            send_sms(from_number=owner, to_number=to_number, body=reply_text)
            log.info("SMS poll: replied to=%s (test=%s)", to_number, _sms_test_enabled())
        except Exception:
            log.exception("SMS poll: reply send failed opp=%s", opp.get("opportunityId"))
