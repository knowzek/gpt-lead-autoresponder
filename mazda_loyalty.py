import os
import logging
from datetime import datetime, timezone

from patti_common import EMAIL_RE
from email_ingestion import _extract_email  # re-use your helper (or copy it)
from patti_mailer import send_via_sendgrid  # the SendGrid Mail Send API helper we discussed

# IMPORTANT: these should point to the Mazda Loyalty Airtable base/table
from airtable_store import (
    find_mazda_by_customer_email,   # you will add these (see step 3)
    patch_mazda_by_id,
)

log = logging.getLogger("patti.mazda_loyalty")

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def handle_mazda_loyalty_inbound_email(*, inbound: dict, subject: str, body_text: str):
    sender_raw = (inbound.get("from") or "").strip()
    sender_email = _extract_email(sender_raw).strip().lower()

    ts = inbound.get("timestamp") or _now_iso()

    # Find Mazda record by sender email
    rec = find_mazda_by_customer_email(sender_email)
    if not rec:
        log.warning("Mazda Loyalty: no record match for sender_email=%s subj=%r", sender_email, (subject or "")[:120])
        return

    rec_id = rec.get("id")

    # ✅ STOP both cadences on any engagement
    patch_mazda_by_id(rec_id, {
        "email_status": "convo",
        "next_email_at": None,
        "sms_status": "convo",
        "next_sms_at": None,
        "last_inbound_at": ts,
        "last_inbound_text": (body_text or "")[:2000],
    })

    # Optional: send a simple acknowledgment via SendGrid (deliverability stays good)
    # (If you want Patti to do a full GPT reply later, keep this minimal or skip it.)
    reply_to = os.getenv("SENDGRID_REPLY_TO_EMAIL", "").strip()
    from_email = os.getenv("SENDGRID_FROM_EMAIL", "").strip()

    if not from_email:
        log.info("Mazda Loyalty: SENDGRID_FROM_EMAIL not set; skipping auto-reply")
        return

    # Very safe, short reply
    ack_subject = subject if subject else "[Mazda Loyalty] Thanks — we got your reply"
    ack_html = (
        "<p>Thanks — I got your message.</p>"
        "<p>If you can reply with your 16-digit voucher code (or tell me whether you want to use it or gift it), "
        "I can take the next step for you.</p>"
        "<p>— Patti</p>"
    )
    ack_text = "Thanks — I got your message. Reply with your 16-digit voucher code (or tell me if you want to use it or gift it) and I’ll take the next step. — Patti"

    # Send reply to the customer
    send_via_sendgrid(
        to_email=sender_email,
        subject=ack_subject,
        body_html=ack_html,
        body_text=ack_text,
    )
