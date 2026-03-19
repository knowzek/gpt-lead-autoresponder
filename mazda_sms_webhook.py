# mazda_sms_webhook.py
import os
import re
import logging
from datetime import datetime, timezone

from airtable_store import find_by_customer_phone_loose, patch_by_id
from goto_sms import send_sms, list_conversations, list_messages
from mazda_loyalty_sms_brain import generate_mazda_loyalty_sms_reply

log = logging.getLogger("patti.mazda.sms.webhook")

VOUCHER_RE = re.compile(r"(?<![A-Z0-9])([A-Z0-9][A-Z0-9 -]{10,24}[A-Z0-9])(?![A-Z0-9])", re.I)
APPT_RE = re.compile(r"\b(appointment|appt|test drive|come in|schedule|book|available|availability|what time|tomorrow|today|this (week|weekend)|weekday|saturday|sunday)\b", re.I)

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _looks_like_appt_intent(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    return bool(APPT_RE.search(t))

def _extract_voucher_code(text: str) -> str | None:
    s = (text or "").strip().upper()
    if not s:
        return None

    for m in VOUCHER_RE.finditer(s):
        raw = m.group(1)
        cleaned = re.sub(r"[^A-Z0-9]", "", raw)

        if 12 <= len(cleaned) <= 20 and any(ch.isalpha() for ch in cleaned) and any(ch.isdigit() for ch in cleaned):
            return cleaned

        if len(cleaned) == 16 and cleaned.isdigit():
            return cleaned

    return None

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

def _thread_text(thread_snippet: list[dict] | None, max_msgs: int = 8) -> str:
    if not thread_snippet:
        return ""

    out = []
    for m in thread_snippet[-max_msgs:]:
        role = (m.get("role") or "").strip().lower()
        content = (m.get("content") or "").strip()
        if not content:
            continue
        who = "Customer" if role == "user" else "Patti"
        out.append(f"{who}: {content}")
    return "\n".join(out).strip()


def _thread_indicates_service_credit_flow(thread_snippet: list[dict] | None) -> bool:
    t = _thread_text(thread_snippet).lower()
    return any(x in t for x in (
        "service & parts credit",
        "service and parts credit",
        "service credit",
        "redeem it for service",
        "apply to as a credit for service",
        "apply it as a credit for service",
    ))


def _thread_indicates_handoff_started(thread_snippet: list[dict] | None) -> bool:
    t = _thread_text(thread_snippet).lower()
    return any(x in t for x in (
        "looping in a team member",
        "what day/time were you hoping for",
        "what day works best, and about what time",
    ))

def _find_conversation_id_for_phone(*, owner_number: str, customer_phone: str) -> str:
    try:
        convos = list_conversations(owner_number=owner_number, limit=500) or []
    except Exception:
        log.exception("Mazda SMS webhook: failed listing conversations")
        return ""

    want = re.sub(r"\D+", "", customer_phone or "")
    if not want:
        return ""

    for c in convos:
        participants = c.get("participants") or []
        for p in participants:
            num = re.sub(r"\D+", "", str(p.get("phoneNumber") or p.get("number") or ""))
            if num and num.endswith(want[-10:]):
                return str(c.get("id") or "").strip()

    return ""


def _load_thread_snippet(*, owner_number: str, customer_phone: str, limit: int = 8) -> list[dict]:
    convo_id = _find_conversation_id_for_phone(owner_number=owner_number, customer_phone=customer_phone)
    if not convo_id:
        return []

    try:
        msgs = list_messages(owner_number=owner_number, conversation_id=convo_id, limit=limit) or []
    except Exception:
        log.exception("Mazda SMS webhook: failed loading thread messages")
        return []

    out = []
    for m in msgs[-limit:]:
        body = str(m.get("body") or m.get("text") or "").strip()
        if not body:
            continue

        author = str(
            m.get("authorPhoneNumber")
            or m.get("from")
            or ""
        ).strip()

        role = "assistant"
        if author and re.sub(r"\D+", "", author).endswith(re.sub(r"\D+", "", customer_phone or "")[-10:]):
            role = "user"

        out.append({"role": role, "content": body})

    return out
    
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
    owner = (os.getenv("PATTI_PHONE_E164") or os.getenv("PATTI_NUMBER") or "").strip()
    thread = _load_thread_snippet(owner_number=owner, customer_phone=author, limit=8) if owner else []

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
        thread_snippet=thread,
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
    
        if _thread_indicates_service_credit_flow(thread) or _thread_indicates_handoff_started(thread):
            decision["reply"] = (
                f"{prefix}thanks — I’m looping in a team member to lock in a time. "
                "They’ll reach out shortly."
            )
        else:
            decision["reply"] = (
                f"{prefix}thanks — I’m looping in a team member to lock in a time. "
                "What day works best, and about what time?"
            )

    reply_text = (decision.get("reply") or "").strip()
    if not reply_text:
        reply_text = "Thanks, if you have your Mazda loyalty rebate code, text it here and I’ll help with next steps."
        
    # Send SMS reply

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
                rooftop_sender=rooftop_name or "Mazda",
                customer_name=first_name or "Customer",
                customer_email=customer_email or "unknown",
                customer_phone=phone or "unknown",
                bucket=bucket,
                inbound_text=inbound_text,
                thread_text=_thread_text(thread),
                reason=f"Mazda Loyalty SMS handoff: {reason}",
                now_iso=_now_iso(),
            )
            log.warning("Mazda SMS webhook: handoff email sent to=%s reason=%s", to_addr, reason)
        except Exception:
            log.exception("Mazda SMS webhook: failed sending handoff email rec=%s", rec_id)

    return {"ok": True, "handled": True, "handoff": bool(decision.get("needs_handoff"))}
