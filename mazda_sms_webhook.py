import os
import re
import logging
from datetime import datetime, timezone

from airtable_store import find_by_customer_phone_loose, patch_by_id
from goto_sms import send_sms  # same send_sms you already use elsewhere
from mazda_loyalty_brain import generate_mazda_loyalty_sms_reply

log = logging.getLogger("patti.mazda.sms.webhook")

VOUCHER_RE = re.compile(r"\b(\d{16})\b")
APPT_RE = re.compile(r"\b(appointment|appt|test drive|come in|schedule|book|available|availability|what time|tomorrow|today|this (week|weekend)|weekday|saturday|sunday)\b", re.I)

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _looks_like_appt_intent(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    return bool(APPT_RE.search(t))

def _extract_voucher_code(text: str) -> str | None:
    m = VOUCHER_RE.search(text or "")
    return m.group(1) if m else None

def _parse_cc_env() -> list[str]:
    raw = (os.getenv("HUMAN_REVIEW_CC") or "").strip()
    if not raw:
        return []
    parts = raw.replace(",", ";").split(";")
    return [p.strip() for p in parts if p.strip()]

def _extract_goto_payload(payload_json: dict) -> dict:
    """
    Best-effort extraction for GoTo webhook shapes.
    Returns: {"msg_id": str, "from_phone": str, "text": str}
    """
    p = payload_json or {}

    # common candidates
    msg_id = (
        p.get("id")
        or p.get("messageId")
        or (p.get("message") or {}).get("id")
        or ""
    )

    from_phone = (
        p.get("authorPhoneNumber")
        or p.get("from")
        or (p.get("message") or {}).get("from")
        or (p.get("message") or {}).get("authorPhoneNumber")
        or ""
    )

    text = (
        p.get("body")
        or p.get("text")
        or (p.get("message") or {}).get("body")
        or (p.get("message") or {}).get("text")
        or ""
    )

    return {"msg_id": str(msg_id or "").strip(), "from_phone": str(from_phone or "").strip(), "text": str(text or "").strip()}


def handle_mazda_loyalty_inbound_sms_webhook(*, payload_json: dict) -> dict:
    """
    Mazda Loyalty inbound SMS handler (webhook path).
    - matches Mazda Airtable record by phone
    - flips sms_status=convo, stops cadence
    - GPT reply with bucket context
    - forces human handoff for voucher codes + appt intent
    - updates Airtable + sends handoff email (if configured)
    """
    extracted = _extract_goto_payload(payload_json)
    msg_id = extracted["msg_id"]
    author = extracted["from_phone"]
    inbound_text = extracted["text"]

    log.info("📥 Mazda SMS webhook: author=%r msg_id=%r text_preview=%r", author, msg_id, inbound_text[:80])

    if not author or not inbound_text:
        return {"ok": True, "ignored": True, "reason": "missing_author_or_text"}

    # Find Mazda record by phone (service env vars already point to Mazda base/table)
    rec = find_by_customer_phone_loose(author)
    if not rec:
        log.warning("Mazda SMS webhook: no Airtable match for phone=%s", author)
        return {"ok": True, "ignored": True, "reason": "no_airtable_match"}

    rec_id = rec.get("id")
    fields = rec.get("fields") or {}

    program = (fields.get("program") or "").strip().lower()
    bucket = (fields.get("bucket") or "").strip()
    is_mazda = ("mazda" in program) or bool(bucket)

    if not is_mazda:
        # Don’t hijack non-mazda records on this service
        return {"ok": True, "ignored": True, "reason": "not_mazda"}

    # Durable dedupe (prevents double reply if webhook replays)
    last_seen = (fields.get("last_sms_inbound_message_id") or "").strip()
    if last_seen and msg_id and last_seen == msg_id:
        log.info("Mazda SMS webhook: dedupe skip msg_id=%s rec=%s", msg_id, rec_id)
        return {"ok": True, "skipped": True, "reason": "dedupe"}

    rooftop_name = (fields.get("rooftop_name") or fields.get("rooftop") or "").strip()
    first_name = (fields.get("first_name") or "").strip()  # ✅ Mazda table only
    customer_email = (fields.get("customer_email") or fields.get("email") or "").strip()
    phone = (fields.get("customer_phone") or "").strip() or author

    # ✅ Stop cadence + store inbound markers (Mazda fields only)
    try:
        patch_by_id(rec_id, {
            "sms_status": "convo",
            "next_sms_at": None,
            "last_sms_inbound_message_id": msg_id,
            "last_sms_inbound_at": _now_iso(),
            "last_inbound_text": inbound_text[:2000],
        })
    except Exception:
        log.exception("Mazda SMS webhook: failed patching inbound markers rec=%s", rec_id)

    # Generate bucket-aware reply
    decision = generate_mazda_loyalty_sms_reply(
        first_name=first_name,
        bucket=bucket,
        rooftop_name=rooftop_name,
        last_inbound=inbound_text,
        thread_snippet=None,  # webhook usually doesn’t have thread
    )

    # ✅ Force voucher handoff (Patti can’t actually verify)
    voucher = _extract_voucher_code(inbound_text)
    if voucher:
        decision["needs_handoff"] = True
        decision["handoff_reason"] = "voucher_lookup"
        prefix = f"{first_name}, " if first_name else ""
        decision["reply"] = (
            f"{prefix}thanks — I got that. I’m looping in a team member now to confirm eligibility and make sure everything is set up correctly."
        )

    # ✅ Force appointment handoff + one narrowing question
    if _looks_like_appt_intent(inbound_text):
        decision["needs_handoff"] = True
        decision["handoff_reason"] = "appointment"
        prefix = f"{first_name}, " if first_name else ""
        decision["reply"] = f"{prefix}thanks — I’m looping in a team member to lock in a time. What day works best, and about what time?"

    reply_text = (decision.get("reply") or "").strip()
    if not reply_text:
        reply_text = "Thanks — if you have your 16-digit voucher code, text it here and I’ll help with next steps."

    # Send SMS reply
    owner = (os.getenv("PATTI_PHONE_E164") or os.getenv("PATTI_NUMBER") or "").strip()
    try:
        send_sms(from_number=owner, to_number=author, body=reply_text)
        log.info("Mazda SMS webhook: replied to=%s", author)
    except Exception:
        log.exception("Mazda SMS webhook: failed sending SMS rec=%s", rec_id)

    # Save outbound markers
    try:
        patch_by_id(rec_id, {
            "last_sms_at": _now_iso(),
            "last_sms_body": reply_text[:2000],
        })
    except Exception:
        log.exception("Mazda SMS webhook: failed saving outbound markers rec=%s", rec_id)

    # Human handoff email + Airtable flags
    if decision.get("needs_handoff"):
        reason = (decision.get("handoff_reason") or "other").strip().lower()

        try:
            patch_by_id(rec_id, {
                "Needs Reply": True,
                "Human Review Reason": f"Mazda Loyalty SMS handoff: {reason}",
            })
        except Exception:
            log.exception("Mazda SMS webhook: failed setting Needs Reply fields rec=%s", rec_id)

        try:
            # You already have this helper in sms_poller.py (you pasted the signature)
            from sms_poller import _send_mazda_sms_handoff_email

            to_addr = (fields.get("salesperson_email") or "").strip() or (os.getenv("HUMAN_REVIEW_FALLBACK_TO") or "").strip()
            if not to_addr:
                to_addr = "knowzek@gmail.com"

            _send_mazda_sms_handoff_email(
                to_addr=to_addr,
                cc_addrs=_parse_cc_env(),
                rooftop_name=rooftop_name or "Mazda",
                customer_name=first_name or "Customer",
                customer_email=customer_email or "unknown",
                customer_phone=phone or "unknown",
                bucket=bucket,
                inbound_text=inbound_text,
                reason=f"Mazda Loyalty SMS handoff: {reason}",
                now_iso=_now_iso(),
            )
            log.warning("Mazda SMS webhook: handoff email sent to=%s reason=%s", to_addr, reason)
        except Exception:
            log.exception("Mazda SMS webhook: failed sending handoff email rec=%s", rec_id)

    return {"ok": True, "handled": True, "handoff": bool(decision.get("needs_handoff"))}
