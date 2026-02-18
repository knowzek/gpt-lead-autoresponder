import os
import logging
from datetime import datetime, timezone

from patti_common import EMAIL_RE
from email_ingestion import _extract_email  # re-use your helper (or copy it)
from patti_mailer import send_via_sendgrid  # the SendGrid Mail Send API helper we discussed
from mazda_loyalty_brain import generate_mazda_loyalty_email_reply
from patti_mailer import send_via_sendgrid
from airtable_store import find_by_customer_email, patch_by_id


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

    first_name = (rec.get("fields", {}).get("first_name") or "").strip()
    bucket = (rec.get("fields", {}).get("bucket") or "").strip()
    rooftop_name = (rec.get("fields", {}).get("rooftop_name") or "").strip()
    
    decision = generate_mazda_loyalty_email_reply(
        first_name=first_name,
        bucket=bucket,
        rooftop_name=rooftop_name,
        last_inbound=body_text,
    )
    
    # send reply back to customer via SendGrid
    send_via_sendgrid(
        to_email=sender_email,
        subject=f"Re: {subject}" if subject else "[Mazda Loyalty] Re: Your voucher",
        body_html=decision["reply_html"],
        body_text=decision["reply_text"],
    )
    
    # log + flags for your Airtable “Needs Reply” view (optional but recommended)
    patch = {
        "last_outbound_email_at": ts,
        "last_outbound_email_subject": f"Re: {subject}" if subject else "[Mazda Loyalty] Re: Your voucher",
        "last_outbound_email_body": (decision["reply_text"] or "")[:5000],
    }
    
    if decision.get("needs_handoff"):
        patch.update({
            "Needs Reply": True,  # only if this field exists
            "Human Review Reason": f"Mazda Loyalty handoff: {decision.get('handoff_reason')}",
        })
    
    patch_by_id(rec_id, patch)

