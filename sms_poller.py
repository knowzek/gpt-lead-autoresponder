# sms_poller.py
import os
import logging
from datetime import datetime, timezone
from datetime import timedelta
from gpt import extract_appt_time
from fortellis import get_token, schedule_activity
from patti_triage import handoff_to_human

from goto_sms import list_conversations, list_messages, send_sms
from airtable_store import (
    find_by_customer_phone_loose,
    opp_from_record,
    save_opp,
    should_suppress_all_sends_airtable,
)
from sms_brain import generate_sms_reply


log = logging.getLogger("patti.sms.poller")

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

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

def _sms_test_enabled() -> bool:
    return (os.getenv("SMS_TEST", "0").strip() == "1")

def _sms_test_to() -> str:
    return _norm_phone_e164_us(os.getenv("SMS_TEST_TO", "").strip())

def _patti_number() -> str:
    return _norm_phone_e164_us(os.getenv("PATTI_SMS_NUMBER", "+17145977229").strip())

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

        # Pull last N messages in this thread so GPT can interpret short replies like "No thanks"
        thread = []
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
        
                thread.append({
                    "role": role,
                    "content": txt[:800],
                })
        except Exception:
            log.exception("SMS poll: failed to fetch thread messages owner=%s contact=%s", owner, author)
            thread = []

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
        rec = find_by_customer_phone_loose(author)
        if not rec:
            log.info("SMS poll: no lead match for author=%s body=%r", author, body[:80])
            continue

        opp = opp_from_record(rec)
        patti = opp.setdefault("patti", {})

        # ✅ Hard suppression gate (STOP/opt-out/suppressed/etc.)
        stop, reason = should_suppress_all_sends_airtable(opp)
        if stop:
            # Still record inbound id so we don't spin on it forever
            extra = {
                "last_sms_inbound_message_id": msg_id,
                "last_sms_inbound_at": last.get("timestamp") or _now_iso(),
            }
            try:
                save_opp(opp, extra_fields=extra)
            except Exception:
                log.exception("SMS poll: failed to save inbound markers while suppressed opp=%s", opp.get("opportunityId"))
            log.info("SMS poll: suppressed=%s opp=%s (no reply)", reason, opp.get("opportunityId"))
            continue


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
        # inbound SMS text
        last_inbound = body or ""

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
            thread_snippet=thread,          # ✅ pass real history
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
        
                # Update Airtable appointment + metrics fields
                now_iso = _now_iso()
                extra_appt = {
                    "AI Set Appointment": True,
                    "AI Appointment At": due_utc,
                }
                save_opp(opp, extra_fields=extra_appt)
        
            except Exception:
                log.exception("SMS poll: failed to schedule appointment opp=%s appt_iso=%r", opp.get("opportunityId"), appt_iso)

        
        # --- Send SMS reply + persist metrics + optional handoff escalation ---
        reply_text = (decision.get("reply") or "").strip()
        if not reply_text:
            reply_text = "Thanks — what day/time works best for you to come in?"

        to_number = author
        if _sms_test_enabled() and _sms_test_to():
            to_number = _sms_test_to()

        try:
            # 1) Send the SMS reply first
            send_sms(from_number=owner, to_number=to_number, body=reply_text)
            log.info("SMS poll: replied to=%s (test=%s)", to_number, _sms_test_enabled())

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
                    save_opp(opp, extra_fields={
                        "Needs Human Review": True,
                        "Human Review Reason": f"SMS handoff: {handoff_reason}",
                        # "Human Review At": now_iso,  # only if this Airtable field exists
                    })
                except Exception:
                    log.exception("SMS poll: failed to set Needs Human Review fields opp=%s", opp.get("opportunityId"))

                try:
                    subscription_id = opp.get("subscription_id") or opp.get("dealer_key")
                    token = get_token(subscription_id)

                    handoff_to_human(
                        opportunity=opp,
                        fresh_opp=None,
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
    import os
    import logging

    logging.basicConfig(
        level=getattr(logging, os.getenv("APP_LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    log = logging.getLogger("patti.sms_poller")
    log.info("sms_poller starting")
    poll_once()
    log.info("sms_poller done")
