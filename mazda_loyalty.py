import logging
import re
from datetime import datetime, timezone

from email_ingestion import _extract_email
from patti_mailer import send_via_sendgrid
from mazda_loyalty_brain import generate_mazda_loyalty_email_reply
from airtable_store import find_by_customer_email, patch_by_id

# Reuse existing human handoff email logic (same as SMS)
try:
    from patti_triage import handoff_to_human
except Exception:
    handoff_to_human = None

log = logging.getLogger("patti.mazda_loyalty")

_TIME_RE = re.compile(r"\b(\d{1,2})(:\d{2})?\s?(am|pm)?\b", re.IGNORECASE)
_DOW_RE = re.compile(r"\b(mon(day)?|tue(sday)?|wed(nesday)?|thu(rsday)?|fri(day)?|sat(urday)?|sun(day)?)\b", re.IGNORECASE)

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _looks_like_appt_intent(text: str) -> bool:
    t = (text or "").lower()
    if any(k in t for k in (
        "appointment", "appt", "test drive", "testdrive", "come in", "come by",
        "stop by", "schedule", "available", "availability", "what times",
        "can i", "could i", "book", "reserve", "set up a time",
        "today", "tomorrow", "this weekend", "saturday", "sunday"
    )):
        return True
    if _DOW_RE.search(t):
        return True
    if _TIME_RE.search(t):
        return True
    return False

def handle_mazda_loyalty_inbound_email(*, inbound: dict, subject: str, body_text: str):
    sender_raw = (inbound.get("from") or "").strip()
    sender_email = _extract_email(sender_raw).strip().lower()
    ts = inbound.get("timestamp") or _now_iso()

    # Find Mazda record (this service is already pointed at Mazda table via env vars)
    rec = find_by_customer_email(sender_email)
    if not rec:
        log.warning("Mazda Loyalty: no record match for sender_email=%s subj=%r", sender_email, (subject or "")[:120])
        return

    rec_id = rec.get("id")
    fields = rec.get("fields") or {}

    # ✅ STOP both cadences on any engagement
    patch_by_id(rec_id, {
        "email_status": "convo",
        "next_email_at": None,
        "sms_status": "convo",
        "next_sms_at": None,
        "last_inbound_at": ts,
        "last_inbound_text": (body_text or "")[:2000],
    })

    first_name = (fields.get("first_name") or fields.get("customer_first_name") or "").strip()
    bucket = (fields.get("bucket") or "").strip()
    rooftop_name = (fields.get("rooftop_name") or fields.get("rooftop") or "").strip()

    # Deterministic appointment intent => force human handoff
    wants_appt = _looks_like_appt_intent(body_text)

    decision = generate_mazda_loyalty_email_reply(
        first_name=first_name,
        bucket=bucket,
        rooftop_name=rooftop_name,
        last_inbound=body_text,
    )

    # If appointment intent, force handoff regardless of model output
    if wants_appt:
        decision["needs_handoff"] = True
        decision["handoff_reason"] = "appointment"
        # Keep reply calm and structured; don't promise a time.
        decision["reply_text"] = (
            f"{'Hi ' + first_name + ',' if first_name else 'Hi there,'}\n\n"
            "Thanks — I can help with that. I’m looping in a team member to confirm the best time with you.\n\n"
            "What day and time were you hoping for?"
        )
        # simple HTML conversion if not provided
        if not decision.get("reply_html"):
            decision["reply_html"] = "<br>".join(decision["reply_text"].split("\n"))

    # Send reply back to customer via SendGrid
    out_subject = f"Re: {subject}" if subject else "[Mazda Loyalty] Re: Your voucher"
    send_via_sendgrid(
        to_email=sender_email,
        subject=out_subject,
        body_html=decision.get("reply_html") or "<br>".join((decision.get("reply_text") or "").split("\n")),
        body_text=decision.get("reply_text") or "",
    )

    # Update Airtable outbound log + human review flags
    patch = {
        "last_outbound_email_at": ts,
        "last_outbound_email_subject": out_subject,
        "last_outbound_email_body": (decision.get("reply_text") or "")[:5000],
    }

    if decision.get("needs_handoff"):
        reason = (decision.get("handoff_reason") or "other")
        patch.update({
            "Needs Reply": True,  # only if field exists
            "Human Review Reason": f"Mazda Loyalty handoff: {reason}",
        })

        # ✅ Actually notify humans (reuse existing function) if available
        if handoff_to_human:
            try:
                # Minimal "opportunity-like" dict that handoff templates can use
                pseudo_opp = {
                    "rooftop_name": rooftop_name,
                    "customer_first_name": first_name,
                    "customer_email": sender_email,
                    "customer_phone": (fields.get("phone") or fields.get("customer_phone") or "").strip(),
                    "bucket": bucket,
                    "program": "Mazda Loyalty",
                }

                handoff_to_human(
                    opportunity=pseudo_opp,
                    fresh_opp=None,
                    token=None,
                    subscription_id=None,
                    rooftop_name=rooftop_name,
                    inbound_subject=f"Mazda Loyalty handoff: {reason}",
                    inbound_text=body_text,
                    inbound_ts=ts,
                    triage={"reason": f"Mazda Loyalty handoff: {reason}", "confidence": 1.0},
                )
            except Exception:
                log.exception("Mazda Loyalty: handoff_to_human failed rec_id=%s", rec_id)

    patch_by_id(rec_id, patch)
    log.info("Mazda Loyalty: handled inbound sender=%s rec_id=%s handoff=%s", sender_email, rec_id, bool(decision.get("needs_handoff")))
