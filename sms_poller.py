# sms_poller.py
import os
import re
import logging
from datetime import datetime, timezone, timedelta
from datetime import datetime as _dt, timezone as _tz


from gpt import extract_appt_time
from fortellis import get_token, schedule_activity, get_opportunity, add_opportunity_comment

from goto_sms import list_conversations, list_messages, send_sms
from sms_brain import generate_sms_reply
from mazda_loyalty_sms_brain import generate_mazda_loyalty_sms_reply
from templates import build_mazda_loyalty_sms

from patti_triage import handoff_to_human, notify_staff_patti_scheduled_appt
from outlook_email import send_email_via_outlook

from models.airtable_model import Conversation, Message

from airtable_store import (
    find_by_customer_phone_loose,
    opp_from_record,
    save_opp,
    should_suppress_all_sends_airtable,

    # conversation thread storage
    _ensure_conversation,
    _get_messages_for_conversation,
    _get_conversation_record_id_by_opportunity_id,
    upsert_conversation,
    log_message,

    # misc (only keep if you actually use them below)
    list_records_by_view,
    patch_by_id,

    # message id helpers (pick ONE source; keeping airtable_store here)
    _generate_message_id,
    _normalize_message_id,
)

log = logging.getLogger("patti.sms.poller")

_TIME_RE = re.compile(r"\b(\d{1,2})(:\d{2})?\s?(am|pm)?\b", re.IGNORECASE)
_DOW_RE  = re.compile(r"\b(mon(day)?|tue(sday)?|wed(nesday)?|thu(rsday)?|fri(day)?|sat(urday)?|sun(day)?)\b", re.IGNORECASE)

VOUCHER_RE = re.compile(r"\b(\d{16})\b")
APPT_RE = re.compile(
    r"\b(appointment|appt|test drive|come in|schedule|book|available|availability|what time|tomorrow|today|this (week|weekend)|weekday|saturday|sunday)\b",
    re.I
)

_VOUCHERISH_RE = re.compile(r"(?<!\d)(\d{12,20})(?!\d)")  # catches 12–20 digit codes


def _extract_voucherish_code(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""
    m = _VOUCHERISH_RE.search(t.replace(" ", ""))
    return m.group(1) if m else ""


def _now_iso():
    return datetime.now(timezone.utc).isoformat()

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

    log.warning(
        "MAZDA_DECISION rec_id=%s needs_handoff=%s reason=%r reply_preview=%r",
        rec_id,
        bool(decision.get("needs_handoff")),
        (decision.get("handoff_reason") or ""),
        (decision.get("reply") or "")[:90],
    )

    # Human handoff email + Airtable flags
    if decision.get("needs_handoff"):
        reason = (decision.get("handoff_reason") or "other").strip().lower()
        msg = f"Mazda Loyalty SMS handoff: {reason}"
    
        # Try Mazda schema A
        try:
            patch_by_id(rec_id, {
                "Needs Reply": True,
                "Human Review Reason": msg,
            })
            log.warning("MAZDA_AIRTABLE_FLAGGED rec_id=%s schema=A", rec_id)
        except Exception:
            log.exception("Mazda SMS: failed flag schema A rec=%s", rec_id)
    
            # Try Mazda schema B
            try:
                patch_by_id(rec_id, {
                    "Needs Human Review": True,
                    "Human Review Reason": msg,
                })
                log.warning("MAZDA_AIRTABLE_FLAGGED rec_id=%s schema=B", rec_id)
            except Exception:
                log.exception("Mazda SMS: failed flag schema B rec=%s", rec_id)

        try:
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

def _looks_like_appt_intent(text: str) -> bool:
    """
    Best-effort detection that the guest is trying to schedule / book / confirm a time.
    Used to force human handoff + ask one narrowing question.
    """
    t = (text or "").lower().strip()
    if not t:
        return False

    # Strong intent phrases
    if any(k in t for k in (
        "appointment", "appt", "schedule", "book", "set up a time",
        "test drive", "testdrive", "come in", "come by", "stop by",
        "available", "availability", "what times", "what time works",
        "can i come", "could i come", "when can i", "works for you",
        "today", "tomorrow", "this weekend"
    )):
        return True

    # Day-of-week mention
    if _DOW_RE.search(t):
        return True

    # Time mention (e.g., 11:30, 3pm, 12, 7:15pm)
    if _TIME_RE.search(t):
        return True

    return False

def _send_mazda_sms_handoff_email(
    *,
    to_addr: str,
    cc_addrs: list[str],
    rooftop_name: str,
    customer_name: str,
    customer_email: str,
    customer_phone: str,
    bucket: str,
    inbound_text: str,
    reason: str,
    now_iso: str,
):
    subj = f"[Patti] Mazda Loyalty SMS handoff - {rooftop_name} - {customer_name}"
    html = f"""
    <p><b>Mazda Loyalty SMS handoff — please take over.</b></p>

    <p><b>Rooftop:</b> {rooftop_name}<br>
    <b>Bucket:</b> {bucket or "unknown"}</p>

    <p><b>Customer:</b> {customer_name}<br>
    <b>Email:</b> {customer_email or "unknown"}<br>
    <b>Phone:</b> {customer_phone or "unknown"}</p>

    <p><b>Reason:</b> {reason}</p>

    <p><b>Latest customer text:</b><br>
    <pre style="white-space:pre-wrap;font-family:Arial,Helvetica,sans-serif;">{(inbound_text or "")[:2000]}</pre></p>

    <p style="color:#666;font-size:12px;">
      Logged by Patti • Mazda Loyalty • {now_iso}
    </p>
    """.strip()

    safe_mode = (os.getenv("PATTI_SAFE_MODE", "0").strip() == "1") or (os.getenv("SAFE_MODE", "0").strip() == "1")
    if safe_mode:
        test_to = (os.getenv("TEST_TO") or "").strip() or (os.getenv("INTERNET_TEST_EMAIL") or "").strip() or (os.getenv("HUMAN_REVIEW_FALLBACK_TO") or "").strip()
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
        opp_id="mazda-loyalty",  # Mazda has no Fortellis opp id
        cc_addrs=cc_addrs,
        timeout=20,
        enforce_compliance=False,
    )


def mark_sms_convo_on_inbound(*, rec_id: str, inbound_text: str, inbound_ts: str | None = None):
    now_iso = inbound_ts or _now_iso()
    patch_by_id(rec_id, {
        "sms_status": "convo",
        "last_inbound_text": inbound_text,
        "last_inbound_at": now_iso,
        "next_sms_at": None,
    })


def _norm_phone_e164_us(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    if raw.startswith("+") and len(digits) >= 10:
        return "+" + digits
    return ""

def _now_utc_z() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def log_sms_note_to_crm(*, token: str, subscription_id: str, opp_id: str, direction: str, text: str):
    # Fortellis comment endpoint is the most reliable “note” mechanism across rooftops.
    comment = (
        f"<b>{direction} SMS</b><br/>"
        f"{(text or '').strip()}"
    )
    add_opportunity_comment(token, subscription_id, opp_id, comment)


def _sms_test_enabled() -> bool:
    return os.getenv("SMS_TEST", "0").strip() == "1"


def _sms_test_to() -> str:
    return _norm_phone_e164_us(os.getenv("SMS_TEST_TO", "").strip())


def _patti_number() -> str:
    return _norm_phone_e164_us(os.getenv("PATTI_SMS_NUMBER", "+17145977229").strip())

SMS_DUE_VIEW = os.getenv("SMS_DUE_VIEW", "SMS Due")

def send_sms_cadence_once():
    # Pull queue
    recs = list_records_by_view(SMS_DUE_VIEW, max_records=50)

    for r in recs:
        rid = r.get("id")
        f = (r.get("fields") or {})
        phone = (f.get("customer_phone") or f.get("phone") or "").strip()

        log.info("SMS cadence candidate rid=%s phone=%r sms_status=%r email_status=%r next_sms_at=%r sms_day=%r",
         rid, phone, f.get("sms_status"), f.get("email_status"), f.get("next_sms_at"), f.get("sms_day"))


        if not phone:
            continue

        # Global suppression / opt-out protection (reuse your existing guard)
        stop, reason = should_suppress_all_sends_airtable(f)
        if stop:
            patch_by_id(rid, {
                "sms_status": "paused",
                "last_sms_body": f"Suppressed: {reason}",
            })
            log.info("SMS cadence suppressed rid=%s reason=%r", rid, reason)
            continue


        day = int(f.get("sms_day") or 1)

        # v1: simplest templated nudge by day (fast + predictable)
        body = build_mazda_loyalty_sms(day=day, fields=f)

        owner = _patti_number()   # same helper used in poll_once()

        ok = send_sms(
            from_number=owner,
            to_number=phone,
            body=body
        )

        if ok:
            now_iso = datetime.now(timezone.utc).isoformat()
            patch_by_id(rid, {
                "last_sms_at": now_iso,
                "last_sms_body": body,
                "sms_day": day + 1,
                # You decide spacing; example 3-day cadence:
                "next_sms_at": (datetime.now(timezone.utc) + timedelta(days=3)).isoformat(),
                "sms_status": "ready"
            })



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
        author = last.get("authorPhoneNumber") or ""
        
        body = (last.get("body") or "").strip()
        media = last.get("media") or []
        
        # --- Handle media-only / blank body edge case ---
        if not body:
            try:
                raw = list_messages(owner_phone_e164=owner, contact_phone_e164=author, limit=12)
                items2 = raw.get("items") or []
        
                # oldest → newest
                items2 = sorted(items2, key=lambda m: m.get("timestamp") or "")
        
                # walk newest → oldest looking for last inbound with text
                for m in reversed(items2):
                    if (m.get("direction") or "").upper() == "IN":
                        txt = (m.get("body") or "").strip()
                        if txt:
                            body = txt
                            break
            except Exception:
                log.exception("SMS poll: media fallback lookup failed author=%s", author)
        
        if not body:
            log.info("SMS poll: skipping empty inbound (media_only=%s) author=%s", bool(media), author)
            continue


        # Pull last N messages in this thread so GPT can interpret short replies like "No thanks"
        thread = []
        items2 = []
        try:
            raw = list_messages(owner_phone_e164=owner, contact_phone_e164=author, limit=12)
            items2 = raw.get("items") or []
            # Oldest -> newest
            items2 = sorted(items2, key=lambda m: m.get("timestamp") or "")

            for m in items2[-12:]:
                txt = (m.get("body") or "").strip()
                if not txt:
                    continue

                author_num = (m.get("authorPhoneNumber") or "").strip()
                role = "assistant" if author_num == owner else "user"

                thread.append(
                    {
                        "role": role,
                        "content": txt[:800],
                    }
                )
        except Exception:
            log.exception("SMS poll: failed to fetch thread messages owner=%s contact=%s", owner, author)
            thread = []

        # ✅ inbound SMS text: some GoTo lastMessage bodies come through blank
        last_inbound = (body or "").strip()
        log.info("SMS poll: last_inbound_len=%d last_inbound_preview=%r", len(last_inbound or ""), (last_inbound or "")[:80])

        if not last_inbound and thread:
            for m in reversed(thread):
                if m.get("role") == "user":
                    last_inbound = (m.get("content") or "").strip()
                    break


        # ✅ Hard gate: only reply if the guest is STILL the last message in the thread
        # This prevents double-replies when another process/user already responded.
        if items2:
            newest = items2[-1]
            newest_id = (newest.get("id") or "").strip()
            newest_author = (newest.get("authorPhoneNumber") or "").strip()

            # If the newest message isn't THIS inbound message, skip
            if newest_id and msg_id and newest_id != msg_id:
                log.info("SMS poll: skip (newest_id != msg_id) newest_id=%s msg_id=%s", newest_id, msg_id)
                continue

            # If the newest message was authored by Patti (OUT), skip
            if newest_author == owner:
                log.info("SMS poll: skip (Patti already last message) newest_id=%s", newest_id)
                continue


        # Find the lead by author phone (customer)
        try:
            rec = find_by_customer_phone_loose(author)
        except Exception as e:
            log.warning("SMS poll: Airtable lookup failed (will skip this convo) author=%s err=%r", author, e)
            continue
        
        if not rec:
            log.info("SMS poll: no lead match for author=%s body=%r", author, body[:80])
            continue

        opp = opp_from_record(rec)
        fields = rec.get("fields") or {}
        program = (fields.get("program") or "").strip().lower()
        bucket = (fields.get("bucket") or "").strip()
        is_mazda = ("mazda" in program) or bool(bucket)

        # ✅ If it's Mazda Loyalty, ALWAYS use Mazda path (even if opp_id exists)
        if is_mazda:
            rec_id = rec.get("id")
            rooftop_name = (fields.get("rooftop_name") or fields.get("rooftop") or "").strip()
            first_name = (fields.get("first_name") or "").strip()  # <-- ONLY Mazda table name
            phone = (fields.get("phone") or fields.get("customer_phone") or "").strip() or author

            # 🔎 DEBUG: confirm which Airtable record we matched
            log.warning(
                "MAZDA_MATCH rec_id=%s first_name=%r bucket=%r customer_phone=%r inbound_author=%r",
                rec_id,
                first_name,
                (fields.get("bucket") or ""),
                (fields.get("customer_phone") or ""),
                author,
            )

            # --- Mazda durable dedupe (see section B) ---
            last_seen = (fields.get("last_sms_inbound_message_id") or "").strip()
            if last_seen and msg_id and last_seen == msg_id:
                log.info("Mazda SMS: skipping already-processed inbound msg_id=%s rec=%s", msg_id, rec_id)
                continue

            # ✅ Stop Mazda SMS cadence on engagement + store inbound markers (Mazda fields only)
            try:
                patch_by_id(rec_id, {
                    "sms_status": "convo",
                    "next_sms_at": None,
                    "last_sms_inbound_message_id": msg_id,
                    "last_sms_inbound_at": last.get("timestamp") or _now_iso(),
                    "last_inbound_text": (last_inbound or "")[:2000],
                })
            except Exception:
                log.exception("Mazda SMS: failed to patch convo status rec=%s", rec_id)

            decision = generate_mazda_loyalty_sms_reply(
                first_name=first_name,
                bucket=bucket,
                rooftop_name=rooftop_name,
                last_inbound=last_inbound,
                thread_snippet=thread,
            )

            # --- Force voucher lookup => human handoff ---
            code = _extract_voucherish_code(last_inbound)
            
            log.warning(
                "MAZDA_VOUCHER_CHECK rec_id=%s inbound=%r code=%r",
                rec_id,
                (last_inbound or "")[:80],
                code,
            )
            
            if code:
                decision["needs_handoff"] = True
                decision["handoff_reason"] = "voucher_lookup"
            
                prefix = f"{first_name}, " if first_name else ""
                decision["reply"] = (
                    f"{prefix}thank you — I got your voucher code. "
                    "I’m looping in a team member now to confirm eligibility and make sure everything is set up correctly."
                )


            # Appointment intent still overrides
            wants_appt = _looks_like_appt_intent(last_inbound)
            if wants_appt:
                decision["needs_handoff"] = True
                decision["handoff_reason"] = "appointment"
                prefix = f"{first_name}, " if first_name else ""
                decision["reply"] = f"{prefix}thanks — I’m looping in a team member to lock in a time. What day works best, and about what time?"

            reply_text = (decision.get("reply") or "").strip()
            if not reply_text:
                reply_text = "Thanks — if you have your 16-digit voucher code, text it here and I’ll help with next steps."

            to_number = author
            if _sms_test_enabled() and _sms_test_to():
                to_number = _sms_test_to()

            try:
                send_sms(from_number=owner, to_number=to_number, body=reply_text)
                log.info("Mazda SMS: replied to=%s (test=%s)", to_number, _sms_test_enabled())
            except Exception:
                log.exception("Mazda SMS: failed sending reply rec=%s", rec_id)

            # ✅ Save outbound markers using Mazda fields only
            try:
                patch_by_id(rec_id, {
                    "last_sms_at": _now_iso(),
                    "last_sms_body": reply_text[:2000],
                })
            except Exception:
                log.exception("Mazda SMS: failed saving outbound markers rec=%s", rec_id)

            log.warning(
                "MAZDA_DECISION rec_id=%s needs_handoff=%s reason=%r reply_preview=%r",
                rec_id,
                bool(decision.get("needs_handoff")),
                (decision.get("handoff_reason") or ""),
                (decision.get("reply") or "")[:90],
            )

            # ✅ If handoff required, do Airtable flags + email notify
            if decision.get("needs_handoff"):
                try:
                    reason = (decision.get("handoff_reason") or "other").strip().lower()
                    patch_by_id(rec_id, {
                        "Needs Reply": True,
                        "Human Review Reason": f"Mazda Loyalty SMS handoff: {reason}",
                    })
                except Exception:
                    log.exception("Mazda SMS: failed to flag Needs Reply rec=%s", rec_id)

                try:
                    salesperson_email = (fields.get("salesperson_email") or "").strip() or (os.getenv("HUMAN_REVIEW_FALLBACK_TO") or "").strip() or "knowzek@gmail.com"
                    cc_addrs = _parse_cc_env()
                    customer_email = (fields.get("customer_email") or "").strip()
                    customer_phone = phone or author
                    customer_name = first_name or "Customer"

                    log.warning(
                        "MAZDA_HANDOFF_EMAIL about to send rec_id=%s to=%r cc=%r",
                        rec_id, salesperson_email, cc_addrs
                    )

                    _send_mazda_sms_handoff_email(
                        to_addr=salesperson_email,
                        cc_addrs=cc_addrs,
                        rooftop_name=rooftop_name or "Mazda",
                        customer_name=customer_name,
                        customer_email=customer_email or "unknown",
                        customer_phone=customer_phone or "unknown",
                        bucket=bucket,
                        inbound_text=last_inbound,
                        reason=f"Mazda Loyalty SMS handoff: {reason}",
                        now_iso=_now_iso(),
                    )
                    
                    log.warning("MAZDA_HANDOFF_EMAIL sent rec_id=%s", rec_id)

                except Exception:
                    log.exception("Mazda SMS: failed handoff notify rec=%s", rec_id)

            continue  # ✅ Mazda handled; don't fall through to Fortellis

        subscription_id = (opp.get("subscription_id") or opp.get("dealer_key") or "").strip()
        opp_id = (opp.get("opportunityId") or opp.get("opportunity_id") or "").strip()

        patti = opp.setdefault("patti", {})
        
        # ✅ Hard suppression gate
        stop, reason = should_suppress_all_sends_airtable(opp)
        if stop:
            extra = {
                "last_sms_inbound_message_id": msg_id,
                "last_sms_inbound_at": last.get("timestamp") or _now_iso(),
            }
            try:
                save_opp(opp, extra_fields=extra)
            except Exception:
                log.exception("SMS poll: failed to save inbound markers while suppressed opp=%s", opp.get("opportunityId"))
            log.info("SMS poll: suppressed=%s opp=%s (no reply)", reason, opp.get("opportunityId"))

            # ✅ Still stop cadence nudges on inbound, even if suppressed
            try:
                inbound_ts = last.get("timestamp") or _now_iso()
                mark_sms_convo_on_inbound(
                    rec_id=rec.get("id"),
                    inbound_text=last_inbound,
                    inbound_ts=inbound_ts,
                )
            except Exception:
                log.exception("SMS poll: suppressed but failed to set sms_status=convo rec_id=%s", rec.get("id"))

            continue
        
        # ✅ Dedupe: only act once per inbound message id
        source = opp.get("source", "")
        opp_id = opp.get("opportunityId", "")
        subscription_id = opp.get("subscription_id", "")

        conversation_record_id = _ensure_conversation(opp, channel="sms", linked_lead_record_id=rec.get("id", ""))
        conversation_id = f"conv_{subscription_id}_{opp_id}"

        # Dedupe: only act once per inbound message id
        last_seen = (opp.get("last_sms_inbound_message_id") or "").strip()
        if last_seen == msg_id:
            log.info("SMS poll: skipping already-processed msg_id=%s", msg_id)
            continue

        # ✅ Flip Mazda record into convo mode so cadence stops
        try:
            inbound_ts = last.get("timestamp") or _now_iso()
            mark_sms_convo_on_inbound(
                rec_id=rec.get("id"),
                inbound_text=last_inbound,
                inbound_ts=inbound_ts,
            )
        except Exception:
            log.exception("SMS poll: failed to flip sms_status=convo for rec_id=%s", rec.get("id"))

        
        # ✅ NOW we know this inbound is new — log it to CRM once
        try:
            token = get_token(subscription_id)
            log_sms_note_to_crm(
                token=token,
                subscription_id=subscription_id,
                opp_id=opp_id,
                direction="Inbound",
                text=last_inbound,
            )

        except Exception:
            log.exception("SMS poll: failed to log inbound SMS to CRM opp=%s", opp.get("opportunityId"))

        timestamp = _dt.now(_tz.utc).replace(microsecond=0).isoformat()
        resolved_message_id = (
            _normalize_message_id(msg_id) if msg_id else _generate_message_id(opp_id, timestamp, "", author, body)
        )

        try:
            airtable_log = Message(
                message_id=resolved_message_id,
                conversation=conversation_record_id,
                direction="inbound",
                channel="sms",
                timestamp=timestamp,
                from_=author,
                to=owner,
                subject="",
                body_text=body,
                body_html="",
                provider=source,
                opp_id=opp_id,
                delivery_status="received",
                rooftop_name="",
                rooftop_sender="",
            )
            message_log_status = log_message(airtable_log)
            if message_log_status:
                log.info("Inbound sms logged successfully.")
            else:
                log.error("Failed to log inbound sms.")
        except Exception as e:
            log.error(f"Error during inbound message logging (process_inbound_sms): {e}.")
            pass

        try:
            message_update_convo = Conversation(
                conversation_id=conversation_id,
                last_channel="sms",
                last_activity_at=timestamp,
                last_customer_message=body[:300],
                customer_last_reply_at=timestamp,
                status="open",
            )
            conversation_record_id = upsert_conversation(message_update_convo)
        except Exception as e:
            log.error(
                f"Something went wrong while upserting message update in poll_once to Conversations table (1): {e}"
            )

        # Record inbound + switch mode
        patti["mode"] = "convo"
        extra = {
            "last_sms_inbound_message_id": msg_id,
            "last_sms_inbound_at": last.get("timestamp") or _now_iso(),
            "mode": "convo",
        }
        save_opp(opp, extra_fields=extra)

        log.info("SMS poll: new inbound msg opp=%s author=%s body=%r", opp.get("opportunityId"), author, body[:120])

        # Build GPT reply

        log.info(
            "SMS poll: thread context turns=%d (most recent=%r)",
            len(thread),
            (thread[-1]["content"][:80] if thread else None),
        )

        # opp is already canonicalized by opp_from_record()
        decision = generate_sms_reply(
            rooftop_name=opp["rooftop_name"],
            customer_first_name=opp["customer_first_name"],
            customer_phone=opp["customer_phone"],
            salesperson=opp["salesperson_name"],
            vehicle=opp.get("vehicle") or "",
            last_inbound=last_inbound,
            thread_snippet=thread,  # ✅ pass real history
            include_optout_footer=False,
        )

        needs_handoff = bool(decision.get("needs_handoff"))
        handoff_reason = (decision.get("handoff_reason") or "other").strip().lower()

        if needs_handoff:
            name = (opp.get("customer_first_name") or "").strip()
            prefix = f"Thanks, {name}. " if name else "Thanks. "

            if handoff_reason == "pricing":
                decision["reply"] = prefix + "I’ll have the team follow up with pricing details shortly."
            elif handoff_reason == "phone_call":
                decision["reply"] = prefix + "I’ll have someone give you a quick call shortly."
            elif handoff_reason in ("angry", "complaint"):
                decision["reply"] = prefix + "I’m sorry about that. I’m looping in a manager now so we can help."
            else:
                decision["reply"] = prefix + "I’m looping in a team member to help, and they’ll follow up shortly."
            try:
                handoff_update_convo = Conversation(
                    conversation_id=conversation_id,
                    status="needs_review",
                    needs_human_review=True,
                    needs_human_review_reason=handoff_reason,
                    last_activity_at=_dt.now(_tz.utc).replace(microsecond=0).isoformat(),
                    last_channel="sms",
                )
                conversation_record_id = upsert_conversation(handoff_update_convo)
            except Exception as e:
                log.error(
                    f"Something went wrong while upserting message update in poll_once to Conversations table (2): {e}"
                )

        # --- Appointment detect + schedule (authoritative actions happen here, not in GPT text) ---
        appt = extract_appt_time(last_inbound, tz="America/Los_Angeles")
        appt_iso = (appt.get("iso") or "").strip()
        appt_conf = float(appt.get("confidence") or 0)

        if appt_iso and appt_conf >= 0.80:
            try:
                # Convert local ISO w/ offset -> UTC Z for Fortellis
                dt_local = datetime.fromisoformat(appt_iso)
                dt_utc = dt_local.astimezone(timezone.utc)
                due_utc = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

                # Get CRM token + schedule activity
                dealer_key = opp.get("subscription_id") or opp.get("dealer_key")
                if not dealer_key:
                    raise RuntimeError(f"Missing subscription_id/dealer_key for opp={opp.get('opportunityId')}")
                token = get_token(dealer_key)

                schedule_activity(
                    token,
                    opp["subscription_id"],
                    opp["opportunityId"],
                    due_dt_iso_utc=due_utc,
                    activity_name="Sales Appointment",
                    activity_type=7,
                    comments=f"Patti scheduled via SMS based on customer reply: {last_inbound[:200]}",
                )
        
                # ✅ Send appointment notification email (same as email flow)
                try:
                    fresh_opp = None
                    try:
                        fresh_opp = get_opportunity(
                            opp["opportunityId"],
                            token,
                            dealer_key
                        )
                    except Exception:
                        log.exception(
                            "SMS poll: failed to fetch fresh opp for appt notify opp=%s",
                            opp.get("opportunityId"),
                        )
        
                    appt_human = dt_local.strftime("%a %-m/%-d %-I:%M %p")
        
                    notify_staff_patti_scheduled_appt(
                        opportunity=opp,
                        fresh_opp=fresh_opp,
                        subscription_id=dealer_key,
                        rooftop_name=opp.get("rooftop_name") or opp.get("rooftop") or "",
                        appt_human=appt_human,
                        customer_reply=last_inbound,
                    )
        
                except Exception:
                    log.exception(
                        "SMS poll: failed to send appt notify email opp=%s",
                        opp.get("opportunityId"),
                    )
        
                # ✅ Best-effort Airtable update (should NOT block notify)
                try:
                    extra_appt = {
                        "AI Set Appointment": True,
                        "AI Appointment At": due_utc,
                    }
                    save_opp(opp, extra_fields=extra_appt)
                except Exception:
                    log.exception(
                        "SMS poll: failed to save Airtable appt fields opp=%s",
                        opp.get("opportunityId"),
                    )

        
            except Exception:
                log.exception("SMS poll: failed to schedule appointment opp=%s appt_iso=%r", opp.get("opportunityId"), appt_iso)

            try:
                activity_update_convo = Conversation(
                    conversation_id=conversation_id,
                    last_activity_at=_dt.now(_tz.utc).replace(microsecond=0).isoformat(),
                    last_channel="sms",
                )
                conversation_record_id = upsert_conversation(activity_update_convo)
            except Exception as e:
                log.error(
                    f"Something went wrong while upserting last activity in poll_once to Conversations table (3): {e}"
                )

        # --- Send SMS reply + persist metrics + optional handoff escalation ---
        reply_text = (decision.get("reply") or "").strip()
        if not reply_text:
            reply_text = "Thanks — what day/time works best for you to come in?"

        to_number = author
        if _sms_test_enabled() and _sms_test_to():
            to_number = _sms_test_to()

        try:
            # 1) Send the SMS reply first
            send_sms(
                from_number=owner,
                to_number=to_number,
                body=reply_text,
            )
            log.info("SMS poll: replied to=%s (test=%s)", to_number, _sms_test_enabled())
            
            # ✅ Log outbound SMS to CRM
            try:
                token = get_token(subscription_id)
                log_sms_note_to_crm(
                    token=token,
                    subscription_id=subscription_id,
                    opp_id=opp_id,
                    direction="Outbound",
                    text=reply_text,
                )
            except Exception:
                log.exception("SMS poll: failed to log outbound SMS to CRM opp=%s", opp_id)
                

            inbound_count = len(_get_messages_for_conversation(conversation_id, "inbound"))
            outbound_count = len(_get_messages_for_conversation(conversation_id, "outbound"))

            try:
                event_now = _dt.now(_tz.utc).replace(microsecond=0).isoformat()
                activity_update_convo = Conversation(
                    conversation_id=conversation_id,
                    ai_last_reply_at=event_now,
                    last_activity_at=event_now,
                    last_channel="sms",
                    status="open" if not needs_handoff else None,
                    message_count_inbound=inbound_count,
                    message_count_outbound=outbound_count,
                    message_count_total=inbound_count+outbound_count
                )
                conversation_record_id = upsert_conversation(activity_update_convo)
            except Exception as e:
                log.error(
                    f"Something went wrong while upserting message activity in poll_once to Conversations table (4): {e}"
                )

            # 2) Always update “sent” metrics (fail-open)
            now_iso = _now_iso()
            next_due = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()

            try:
                extra_sent = {
                    "last_sms_sent_at": now_iso,
                    "sms_followup_due_at": next_due,
                    "sms_nudge_count": int(opp.get("sms_nudge_count") or 0) + 1,
                    "AI Texts Sent": int(opp.get("AI Texts Sent") or 0) + 1,
                }
                save_opp(opp, extra_fields=extra_sent)
            except Exception:
                log.exception("SMS poll: failed to save SMS sent metrics opp=%s", opp.get("opportunityId"))

            # 3) If handoff, flag Airtable + notify salesperson/GMs (fail-open)
            if needs_handoff:
                try:
                    save_opp(
                        opp,
                        extra_fields={
                            "Needs Human Review": True,
                            "Human Review Reason": f"SMS handoff: {handoff_reason}",
                            # "Human Review At": now_iso,  # only if this Airtable field exists
                        },
                    )
                except Exception:
                    log.exception("SMS poll: failed to set Needs Human Review fields opp=%s", opp.get("opportunityId"))

                try:
                    subscription_id = opp.get("subscription_id") or opp.get("dealer_key")
                    token = get_token(subscription_id)

                    log.warning(
                      "SMS_HR_DEBUG opp=%s rooftop=%r sub=%r salesperson_name=%r salesperson_email=%r keys_has_rep_email=%s",
                      opp.get("opportunityId"),
                      opp.get("rooftop_name"),
                      opp.get("subscription_id"),
                      opp.get("salesperson_name"),
                      opp.get("Assigned Sales Rep Email") or opp.get("salesperson_email") or opp.get("salespersonEmail"),
                      any(k in opp for k in ("Assigned Sales Rep Email", "salesperson_email", "salespersonEmail"))
                    )

                    fresh_opp = None
                    try:
                        fresh_opp = get_opportunity(opp_id, token, subscription_id)  # matches fortellis.py signature
                    except Exception:
                        log.exception("SMS poll: failed to fetch fresh opp from Fortellis opp=%s", opp_id)
                    
                    handoff_to_human(
                        opportunity=opp,
                        fresh_opp=fresh_opp,   # ✅ this is the whole point
                        token=token,
                        subscription_id=subscription_id,
                        rooftop_name=opp.get("rooftop_name") or "",
                        inbound_subject=f"SMS handoff: {handoff_reason}",
                        inbound_text=last_inbound,
                        inbound_ts=now_iso,
                        triage={"reason": f"SMS handoff: {handoff_reason}", "confidence": 1.0},
                    )

                except Exception:
                    log.exception("SMS poll: failed to trigger handoff_to_human opp=%s", opp.get("opportunityId"))

        except Exception:
            log.exception("SMS poll: reply send failed opp=%s", opp.get("opportunityId"))

if __name__ == "__main__":
    import os, logging, traceback
    logging.basicConfig(
        level=getattr(logging, os.getenv("APP_LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log = logging.getLogger("patti.sms_poller")
    try:
        log.info("sms_poller starting (service=%s)", os.getenv("RENDER_SERVICE_NAME"))
        poll_once()
        log.info("sms_poller finished")
    except Exception:
        log.error("sms_poller crashed:\n%s", traceback.format_exc())
        raise

