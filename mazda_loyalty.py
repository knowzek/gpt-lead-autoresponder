import logging
import re
from datetime import datetime, timezone

from email_ingestion import _extract_email
from patti_mailer import send_via_sendgrid
from mazda_loyalty_brain import generate_mazda_loyalty_email_reply
from airtable_store import find_by_customer_email, patch_by_id
from outlook_email import send_email_via_outlook


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

import re

_TIME_RE = re.compile(r"\b(\d{1,2})(:\d{2})?\s?(am|pm)?\b", re.IGNORECASE)
_DOW_RE = re.compile(r"\b(mon(day)?|tue(sday)?|wed(nesday)?|thu(rsday)?|fri(day)?|sat(urday)?|sun(day)?)\b", re.IGNORECASE)

def _looks_like_appt_intent(text: str) -> bool:
    t = (text or "").lower()
    if any(k in t for k in (
        "appointment", "appt", "test drive", "testdrive", "come in", "come by",
        "schedule", "available", "availability", "what times", "book", "set up a time",
        "today", "tomorrow", "this weekend"
    )):
        return True
    if _DOW_RE.search(t):
        return True
    if _TIME_RE.search(t):
        return True
    return False

def _parse_cc_env() -> list[str]:
    raw_cc = (os.getenv("HUMAN_REVIEW_CC") or "").strip()
    if not raw_cc:
        return []
    parts = raw_cc.replace(",", ";").split(";")
    return [p.strip() for p in parts if p.strip()]

def _send_mazda_handoff_email(
    *,
    to_addr: str,
    cc_addrs: list[str],
    rooftop_name: str,
    customer_name: str,
    customer_email: str,
    customer_phone: str,
    bucket: str,
    inbound_subject: str,
    inbound_text: str,
    reason: str,
    now_iso: str,
):
    subj = f"[Patti] Mazda Loyalty handoff - {rooftop_name} - {customer_name}"
    html = f"""
    <p><b>Mazda Loyalty handoff — please take over.</b></p>

    <p><b>Rooftop:</b> {rooftop_name}<br>
    <b>Bucket:</b> {bucket or "unknown"}</p>

    <p><b>Customer:</b> {customer_name}<br>
    <b>Email:</b> {customer_email or "unknown"}<br>
    <b>Phone:</b> {customer_phone or "unknown"}</p>

    <p><b>Reason:</b> {reason}</p>

    <p><b>Inbound subject:</b> {inbound_subject or ""}</p>

    <p><b>Latest customer message:</b><br>
    <pre style="white-space:pre-wrap;font-family:Arial,Helvetica,sans-serif;">{(inbound_text or "")[:2000]}</pre></p>

    <p style="color:#666;font-size:12px;">
      Logged by Patti • Mazda Loyalty • {now_iso}
    </p>
    """.strip()

    # SAFE MODE gate (reuse same semantics as patti_triage)
    safe_mode = (
        (os.getenv("PATTI_SAFE_MODE", "0").strip() == "1")
        or (os.getenv("SAFE_MODE", "0").strip() == "1")
    )
    if safe_mode:
        test_to = (
            (os.getenv("TEST_TO") or "").strip()
            or (os.getenv("INTERNET_TEST_EMAIL") or "").strip()
            or (os.getenv("HUMAN_REVIEW_FALLBACK_TO") or "").strip()
        )
        if not test_to:
            raise RuntimeError("SAFE_MODE enabled but TEST_TO/INTERNET_TEST_EMAIL/HUMAN_REVIEW_FALLBACK_TO not set")

        subj = f"[SAFE MODE] {subj}"
        html = (
            f"<div style='padding:10px;border:2px solid #cc0000;margin-bottom:12px;'>"
            f"<b>SAFE MODE:</b> rerouted to <b>{test_to}</b>.<br/>"
            f"<b>Original To:</b> {to_addr}<br/>"
            f"<b>Original CC:</b> {', '.join(cc_addrs) if cc_addrs else '(none)'}"
            f"</div>"
            + html
        )
        to_addr = test_to
        cc_addrs = []

    send_email_via_outlook(
        to_addr=to_addr,
        subject=subj[:180],
        html_body=html,
        opp_id="mazda-loyalty",   # any string; required by your sender helper for logging
        cc_addrs=cc_addrs,
        timeout=20,
        enforce_compliance=False,
    )


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
        reason = (decision.get("handoff_reason") or "other").strip().lower()
    
        patch.update({
            "Needs Reply": True,  # only if field exists
            "Human Review Reason": f"Mazda Loyalty handoff: {reason}",
        })
    
        # ✅ Notify salesperson + CC managers (Outlook), NOT handoff_to_human (needs opp_id)
        try:
            customer_name = (first_name or "Customer").strip()
            customer_phone = (fields.get("customer_phone") or fields.get("phone") or "").strip() or "unknown"
    
            to_addr = (fields.get("salesperson_email") or "").strip()
            if not to_addr:
                to_addr = (os.getenv("HUMAN_REVIEW_FALLBACK_TO") or "").strip()
            if not to_addr:
                to_addr = "knowzek@gmail.com"  # last-resort fallback
    
            cc_addrs = _parse_cc_env()
    
            _send_mazda_handoff_email(
                to_addr=to_addr,
                cc_addrs=cc_addrs,
                rooftop_name=rooftop_name or "Mazda",
                customer_name=customer_name,
                customer_email=sender_email,
                customer_phone=customer_phone,
                bucket=bucket,
                inbound_subject=subject,
                inbound_text=body_text,
                reason=f"Mazda Loyalty handoff: {reason}",
                now_iso=ts,
            )
    
        except Exception:
            log.exception("Mazda Loyalty: failed to send handoff email rec_id=%s", rec_id)
