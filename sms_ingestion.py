# sms_ingestion.py
import os
import re
import json
import logging
from datetime import datetime as _dt, timezone as _tz


from airtable_store import (
    _ensure_conversation,
    _fetch_customer_details,
    _get_messages_for_conversation,
    find_by_customer_phone,
    opp_from_record,
    save_opp,
    upsert_conversation,
    should_suppress_all_sends_airtable
)
from goto_sms import send_sms, list_messages
from airtable_store import _generate_message_id, _normalize_message_id
from sms_brain import generate_sms_reply
from models.airtable_model import Conversation, Message
from airtable_store import log_message, _get_conversation_record_id_by_opportunity_id


log = logging.getLogger("patti.sms")
_LOGGED_DIR_ID_ONCE: set[tuple[str, str]] = set()


# --- Normalization / routing ---
def _norm_phone_e164_us(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    digits = re.sub(r"\D+", "", raw)
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    if raw.startswith("+") and len(digits) >= 10:
        return "+" + digits
    return ""


def _sms_test_enabled() -> bool:
    return os.getenv("SMS_TEST", "0").strip() == "1"


def _sms_test_to() -> str:
    return _norm_phone_e164_us(os.getenv("SMS_TEST_TO", "").strip())


def _patti_from_number() -> str:
    return _norm_phone_e164_us(os.getenv("PATTI_SMS_NUMBER", "+17145977229").strip())


# --- Rule detectors (simple v1) ---
_STOP_RE = re.compile(r"(?i)\b(stop|unsubscribe|cancel|end|quit)\b")

_PRICING_RE = re.compile(
    r"(?i)\b("
    r"otd|out\s*the\s*door|price|best\s+price|lowest|quote|numbers|breakdown|"
    r"monthly|payment|lease|apr|interest|down\s*payment|finance|financing|"
    r"incentive|rebate|discount|msrp|invoice|"
    r"trade\s*in|trade-in|value\s+my\s+trade|kbb|carmax|carvana"
    r")\b"
)


def _now_iso() -> str:
    return _dt.now(_tz.utc).isoformat()


# --- Payload extraction (unknown schema; best-effort) ---
def _find_first_str(payload, keys):
    """Try keys in order on a dict payload."""
    if not isinstance(payload, dict):
        return ""
    for k in keys:
        v = payload.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _find_first_key(payload, keys: list):
    """Return the first matching key in payload (case-insensitive)."""
    if not isinstance(payload, dict):
        return ""

    payload_keys = list(payload.keys())

    for candidate in keys:
        for actual in payload_keys:
            if actual.casefold() == candidate.casefold():
                return actual

    return ""


def _walk_find(payload, predicate):
    """Depth-first search through dict/list for a value that matches predicate."""
    seen = set()

    def _inner(x):
        xid = id(x)
        if xid in seen:
            return None
        seen.add(xid)

        if predicate(x):
            return x

        if isinstance(x, dict):
            for _, v in x.items():
                r = _inner(v)
                if r is not None:
                    return r
        elif isinstance(x, list):
            for v in x:
                r = _inner(v)
                if r is not None:
                    return r
        return None

    return _inner(payload)


def _extract_inbound(payload: dict, raw_text: str) -> dict:
    """
    Returns:
      {from_phone, to_phone, body, conversation_id, ts}
    Works best-effort. Also logs raw for mapping.
    """
    # Common direct keys first
    body = _find_first_str(payload, ["body", "text", "message", "content"])
    from_phone = _find_first_str(payload, ["fromPhoneNumber", "from", "sender", "source", "contactPhoneNumber"])
    to_phone = _find_first_str(payload, ["toPhoneNumber", "to", "ownerPhoneNumber", "destination"])
    conversation_id = _find_first_str(
        payload, ["conversationId", "conversation_id", "threadId", "thread_id", "chatId", "id"]
    )  # Alias -> messsage_id
    ts = _find_first_str(payload, ["timestamp", "time", "createdAt", "created_at"])

    # ✅ Message id (prefer nested lastMessage.id, then lastMessageId)
    message_id = ""
    if isinstance(payload.get("lastMessage"), dict):
        message_id = _find_first_str(payload["lastMessage"], ["id"])
    if not message_id:
        message_id = _find_first_str(payload, ["lastMessageId", "messageId", "message_id", "id"])

    # ------------------------------------------------------------------
    # ✅ DIRECTION GUARD (ADD HERE)
    # ------------------------------------------------------------------
    direction = ""
    if isinstance(payload.get("lastMessage"), dict):
        direction = _find_first_str(payload["lastMessage"], ["direction"])

    # If this payload represents an OUTBOUND (Patti) message, ignore it
    if direction and direction.upper() != "IN":
        return {
            "from_phone": "",
            "to_phone": "",
            "body": "",
            "conversation_id": "",
            "message_id": "",
            "ts": ts or _now_iso(),
        }
        
    # ✅ Safety log (direction, message_id) once
    try:
        key = ((direction or "").upper(), (message_id or ""))
        if key not in _LOGGED_DIR_ID_ONCE:
            _LOGGED_DIR_ID_ONCE.add(key)
            log.info("SMS inbound debug: direction=%s message_id=%s", key[0], key[1])
    except Exception:
        pass

    # If body is nested as an object (e.g. {"message":{"body":"..."}})
    if not body and isinstance(payload.get("message"), dict):
        body = _find_first_str(payload["message"], ["body", "text", "content"])

    # If from phone number resides in a list wihin contactPhoneNumbers
    from_phone_key = _find_first_key(payload, ["fromPhoneNumber", "from", "sender", "source", "contactPhoneNumber"])

    if not from_phone and from_phone_key:
        value = payload.get(from_phone_key)
        if isinstance(value, list) and value:
            from_phone = value[0] or ""

    # If phones are nested as objects
    if not from_phone and isinstance(payload.get("message"), dict):
        from_phone = _find_first_str(payload["message"], ["from", "fromPhoneNumber", "sender", "contactPhoneNumber"])
    if not to_phone and isinstance(payload.get("message"), dict):
        to_phone = _find_first_str(payload["message"], ["to", "toPhoneNumber", "ownerPhoneNumber"])

    # Last resort: scan raw text for a phone-looking string
    if not from_phone:
        m = re.search(r"(\+?1?\s*\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4})", raw_text or "")
        if m:
            from_phone = m.group(1)

    # If still no body, look for any stringy leaf that looks like a message
    if not body:
        candidate = _walk_find(payload, lambda x: isinstance(x, str) and len(x.strip()) >= 2)
        if isinstance(candidate, str):
            body = candidate.strip()

    return {
        "from_phone": _norm_phone_e164_us(from_phone),
        "to_phone": _norm_phone_e164_us(to_phone),
        "body": (body or "").strip(),
        "conversation_id": (conversation_id or "").strip(),
        "message_id": (message_id or "").strip(),
        "ts": ts or _now_iso(),
    }


# --- Main handler ---
def process_inbound_sms(payload_json: dict | None, raw_text: str = "") -> dict:
    payload_json = payload_json or {}

    # Always log raw payload so we can map it once we see real schema
    try:
        log.info("📩 SMS inbound raw_json=%s", json.dumps(payload_json)[:4000])
    except Exception:
        log.info("📩 SMS inbound raw_json=<unserializable> len=%s", len(str(payload_json)))

    if raw_text:
        log.info("📩 SMS inbound raw_text=%s", (raw_text[:2000] + ("..." if len(raw_text) > 2000 else "")))

    inbound = _extract_inbound(payload_json, raw_text)
    from_phone = inbound["from_phone"]
    body = inbound["body"]
    to_number = inbound.get("to_phone", "")

    if not from_phone or not body:
        # Return ok so GoTo doesn't retry forever; we’ll map schema after seeing logs
        log.warning("SMS inbound missing from/body extracted=%s", inbound)
        return {"status": "ok", "note": "missing from/body"}

    # Look up lead by customer_phone
    rec = find_by_customer_phone(from_phone)
    if not rec:
        log.warning("SMS inbound: no lead found for phone=%s", from_phone)
        # Still reply? For now, no (avoid texting unknown numbers)
        return {"status": "ok", "note": "lead not found"}

    opp = opp_from_record(rec)
    opp.setdefault("patti", {})

    source = opp.get("source", "")
    opp_id = opp.get("opportunityId", "")
    message_id = inbound.get("conversation_id", "")

    customer_details = _fetch_customer_details(opp_id=opp_id) or {}

    subscription_id = opp.get("subscription_id", "")
    conversation_id = f"conv_{subscription_id}_{opp_id}"

    conversation_record_id = _ensure_conversation(opp=opp, channel="sms", linked_lead_record_id=rec.get("id", ""))

    patti_mode = (opp.get("patti") or {}).get("mode") or ""
    now_iso = _now_iso()

    resolved_message_id = (
        _normalize_message_id(message_id) if message_id else _generate_message_id(opp_id, now_iso, "", from_phone, body)
    )

    #

    try:
        airtable_log = Message(
            message_id=resolved_message_id,
            conversation=conversation_record_id,
            direction="inbound",
            channel="sms",
            timestamp=now_iso,
            from_=from_phone,
            to=to_number,
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

        try:
            inbound_sms_upsert = Conversation(
                conversation_id=conversation_id,
                last_channel="sms",
                last_activity_at=now_iso,
                last_customer_message=body[:300],
                customer_last_reply_at=now_iso,
                status="open",
                customer_full_name=customer_details.get("customer_full_name", ""),
                customer_email=customer_details.get("customer_email", ""),
                customer_phone=customer_details.get("customer_phone", ""),
                salesperson_assigned=customer_details.get("salesperson_assigned", ""),
                linked_lead_record=customer_details.get("linked_lead_record", "")
            )
            conversation_record_id = upsert_conversation(inbound_sms_upsert)
        except Exception as e:
            log.error(f"Something went wrong while upserting inbound sms (process_inbound_sms) (1). {e}")

    except Exception as e:
        log.error(f"Error during inbound message logging (process_inbound_sms): {e}.")

    # Always store inbound markers
    msg_id = (inbound.get("message_id") or "").strip()

    base_patch = {
        "last_sms_inbound_at": now_iso,
        "sms_conversation_id": inbound.get("conversation_id") or (opp.get("sms_conversation_id") or ""),
        "last_sms_inbound_message_id": msg_id,
    }



    # --- Rule 1: STOP / opt-out ---
    if _STOP_RE.search(body):
        opp["patti"]["mode"] = "opt_out"
        opp["compliance"] = {
            "suppressed": True,
            "reason": "sms_opt_out",
            "channel": "sms",
            "at": now_iso,
        }

        base_patch.update(
            {
                "sms_opted_out": True,
                "sms_opted_out_at": now_iso,
                "sms_opt_out_reason": "STOP",
                "sms_followup_due_at": None,
            }
        )

        save_opp(opp, extra_fields=base_patch)

        # Confirmation (NO footer)
        reply_text = "Got it — you’re opted out and we won’t text you again."

        # Respect SMS_TEST routing
        to_number = from_phone
        if _sms_test_enabled():
            test_to = _sms_test_to()
            if test_to:
                to_number = test_to

        try:

            send_sms(
                from_number=_patti_from_number(), to_number=to_number, body=reply_text, opp_id=opp_id, source=source
            )
        except Exception:
            log.exception("SMS opt-out confirmation send failed opp=%s", opp.get("opportunityId"))

        try:
            opt_out_convo = Conversation(
                conversation_id=conversation_id,
                subscription_id=subscription_id,
                opportunity_id=opp_id,
                opted_out=True,
                opt_out_channel="sms",
                opt_out_at=now_iso,
                status="suppressed",
                last_activity_at=now_iso,
                last_channel="sms",
            )
            conversation_record_id = upsert_conversation(opt_out_convo)
        except Exception as e:
            log.error(f"Something went wrong while upserting opt out status (process_inbound_sms) (2). {e}")

        return {"status": "ok", "action": "opt_out"}

    # ✅ Suppression gate: if already opted-out/suppressed, do not send replies.
    # (STOP handling below still works; if they text again post-opt-out, we simply no-op.)
    stop_send, stop_reason = should_suppress_all_sends_airtable(opp)

    if stop_send:
        # Still record inbound markers so we don't reprocess this message
        save_opp(opp, extra_fields=base_patch)
        log.info(
            "SMS inbound suppressed=%s opp=%s (no reply)",
            stop_reason,
            opp.get("opportunityId"),
        )
        return {"status": "ok", "action": "suppressed_no_reply"}

    # Guest replied (any non-stop) => mode="convo" and stop SMS nudges
    # (You said you’ll use mode instead of in_conversation)
    if patti_mode != "convo":
        opp["patti"]["mode"] = "convo"

    base_patch.update(
        {
            "sms_followup_due_at": None,
        }
    )

    # --- Rule 2: Pricing / OTD => handoff + immediate “team checking” reply ---
    if _PRICING_RE.search(body):
        opp["patti"]["mode"] = "handoff"
        opp["needs_human_review"] = True
        opp["human_review_reason"] = "pricing"
        opp["compliance"] = {
            "suppressed": True,
            "reason": "handoff_pricing",
            "channel": "sms",
            "at": now_iso,
        }

        base_patch.update(
            {
                "Needs Human Review": True,
                "Human Review Reason": "pricing",
                "Human Review At": now_iso,
            }
        )

        save_opp(opp, extra_fields=base_patch)

        reply_text = "Totally - the team is checking on that now and will text you shortly."

        to_number = from_phone
        if _sms_test_enabled():
            test_to = _sms_test_to()
            if test_to:
                to_number = test_to

        try:
            handoff_convo = Conversation(
                conversation_id=conversation_id,
                status="needs_review",
                needs_human_review=True,
                needs_human_review_reason="pricing",
                last_activity_at=now_iso,
                last_channel="sms",
            )
            conversation_record_id = upsert_conversation(handoff_convo)
        except Exception as e:
            log.error(f"Something went wrong while upserting handoff update (process_inbound_sms) (3). {e}")

        try:
            send_sms(
                from_number=_patti_from_number(), to_number=to_number, body=reply_text, opp_id=opp_id, source=source
            )
        except Exception:
            log.exception("SMS pricing handoff reply failed opp=%s", opp.get("opportunityId"))

        try:
            now_iso = _dt.now(_tz.utc).replace(microsecond=0).isoformat()
            conversation_id = f"conv_{subscription_id}_{opp_id}"

            convo = Conversation(
                conversation_id=conversation_id,
                last_channel="sms",
                last_activity_at=now_iso,
                status="open"
            )
            upsert_conversation(convo)
        except Exception as e:
            log.error(f"Conversation upsert failed (process_inbound_sms) (4): {e}")

        # TODO: trigger your existing handoff notification (salesperson/Mickey)
        return {"status": "ok", "action": "handoff_pricing"}

    # --- Default: immediate simple convo reply (NO footer once guest has replied) ---
    save_opp(opp, extra_fields=base_patch)

    # Impel-style GPT reply (single question, no opt-out footer once guest replies)
    vehicle = (opp.get("vehicle") or opp.get("Vehicle") or "").strip() or "the vehicle you asked about"

    # Pull last N messages in this thread so GPT can interpret short replies & be conversational
    thread = []
    try:
        owner = _patti_from_number()
        raw = list_messages(owner_phone_e164=owner, contact_phone_e164=from_phone, limit=12)
        items2 = raw.get("items") or []
        # Oldest -> newest
        items2 = sorted(items2, key=lambda m: m.get("timestamp") or "")

        for m in items2[-12:]:
            txt = (m.get("body") or "").strip()
            if not txt:
                continue

            author_num = (m.get("authorPhoneNumber") or "").strip()
            role = "assistant" if author_num == owner else "user"
            thread.append({"role": role, "content": txt[:800]})
    except Exception:
        log.exception(
            "SMS inbound: failed to fetch thread messages owner=%s contact=%s", _patti_from_number(), from_phone
        )
        thread = []

    # ✅ Only reply if the newest message is from the guest (not Patti)
    if thread and thread[-1].get("role") != "user":
        log.info("SMS inbound: skip reply because Patti is last message opp=%s", opp.get("opportunityId"))
        return {"status": "ok", "action": "skip_patti_last"}


    decision = generate_sms_reply(
        rooftop_name=(opp.get("rooftop_name") or ""),
        customer_first_name=(opp.get("customer_first_name") or ""),
        customer_phone=from_phone,
        salesperson=(opp.get("Assigned Sales Rep") or "our team"),
        vehicle=vehicle,
        last_inbound=body,
        thread_snippet=thread,
        include_optout_footer=False,
    )

    reply_text = (decision.get("reply") or "Thanks — what day/time works best for you to connect?").strip()

    to_number = from_phone
    if _sms_test_enabled():
        test_to = _sms_test_to()
        if test_to:
            to_number = test_to

    try:
        send_sms(from_number=_patti_from_number(), to_number=to_number, body=reply_text, opp_id=opp_id, source=source)

        inbound_count = len(_get_messages_for_conversation(conversation_id, "inbound"))
        outbound_count = len(_get_messages_for_conversation(conversation_id, "outbound"))

    except Exception:
        log.exception("SMS convo reply failed opp=%s", opp.get("opportunityId"))

    try:
        ai_reply_update = Conversation(
            conversation_id=conversation_id,
            ai_last_reply_at=now_iso,
            last_activity_at=now_iso,
            last_channel="sms",
            message_count_inbound=inbound_count,
            message_count_outbound=outbound_count,
            message_count_total=inbound_count + outbound_count,
            status="open",
        )
        upsert_conversation(ai_reply_update)
    except Exception as e:
        log.error(f"Something went wrong while upserting ai reply update (process_inbound_sms) (5). {e}")

    return {"status": "ok", "action": "reply"}
