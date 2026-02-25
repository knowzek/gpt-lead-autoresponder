import os
import logging
from datetime import datetime, timezone
import hashlib
import re

from outlook_email import send_email_via_outlook
from fortellis import send_opportunity_email_activity, complete_send_email_activity
from airtable_store import (
    _ensure_conversation,
    find_by_opp_id,
    log_message,
    opp_from_record,
    patch_conversations_by_id,
    save_opp,
    is_opp_suppressed,
    patch_by_id,
    mark_ai_email_sent,
    upsert_conversation,
)
from airtable_store import _normalize_message_id, _generate_message_id
from models.airtable_model import Conversation, Message
from airtable_store import _get_conversation_record_id_by_opportunity_id
from bs4 import BeautifulSoup

log = logging.getLogger("patti.mailer")

EMAIL_MODE = os.getenv("EMAIL_MODE", "outlook")  # "crm" or "outlook"


def _clean_body_html_to_body_text(body_html: str) -> str:
    soup = BeautifulSoup(body_html, "html.parser")
    return soup.get_text(strip=True)


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

import os
import requests
import logging

def send_via_sendgrid(*, to_email: str, subject: str, body_html: str, body_text: str | None = None) -> bool:
    api_key = os.getenv("SENDGRID_API_KEY", "").strip()
    from_email = os.getenv("SENDGRID_FROM_EMAIL", "").strip()
    reply_to = os.getenv("SENDGRID_REPLY_TO_EMAIL", "").strip()

    if not api_key or not from_email:
        raise RuntimeError("Missing SENDGRID_API_KEY or SENDGRID_FROM_EMAIL")

    payload = {
        "personalizations": [{"to": [{"email": to_email}]}],
        "from": {"email": from_email},
        "subject": subject,
        "content": [{"type": "text/html", "value": body_html}],
    }

    if reply_to:
        payload["reply_to"] = {"email": reply_to}

    if body_text:
        payload["content"].insert(0, {"type": "text/plain", "value": body_text})

    r = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=20,
    )

    if r.status_code in (200, 202):
        return True

    log.error("SendGrid send failed status=%s body=%s", r.status_code, r.text[:800])
    return False

def _bump_ai_send_metrics_in_airtable(opp_id: str) -> None:
    try:
        rec = find_by_opp_id(opp_id)
        if not rec:
            return

        f = rec.get("fields", {}) or {}
        current = int(f.get("AI Messages Sent") or 0)

        when_iso = _now_iso_utc()

        patch_by_id(
            rec["id"],
            {
                "AI Messages Sent": current + 1,
                "AI First Message Sent At": f.get("AI First Message Sent At") or when_iso,
                "Last AI Message At": when_iso,
            },
        )

    except Exception as e:
        log.warning("AI metrics update failed (non-blocking) opp=%s err=%s", opp_id, str(e)[:500])


def _bump_ai_send_metrics_in_conversations_airtable(opp_id: str) -> None:
    try:
        rec = find_by_opp_id(opp_id)
        if not rec:
            return

        f = rec.get("fields", {}) or {}
        current = int(f.get("AI Messages Sent") or 0)

        when_iso = _now_iso_utc()

        patch_conversations_by_id(
            rec["id"],
            {
                "AI Messages Sent": current + 1,
                "AI First Message Sent At": f.get("AI First Message Sent At") or when_iso,
                "Last AI Message At": when_iso,
            },
        )

    except Exception as e:
        log.warning("AI metrics for Conversations table update failed (non-blocking) opp=%s err=%s", opp_id, str(e)[:500])


def _post_send_airtable_update(
    *,
    opp_id: str,
    next_follow_up_at: str | None,
    force_mode: str | None,
    template_day: int | None = None,
) -> None:
    """
    Airtable is the brain. After a successful send, update counters / timestamps / follow_up_at.
    Fail-open (non-blocking).
    """
    try:
        rec = find_by_opp_id(opp_id)
        if not rec:
            # fallback: keep legacy bump so you don't lose metrics if record missing
            _bump_ai_send_metrics_in_airtable(opp_id)
            return

        opp = opp_from_record(rec)

        when_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        # ✅ This matches your mark_ai_email_sent signature (opp-dict based)
        mark_ai_email_sent(
            opp,
            when_iso=when_iso,
            next_follow_up_at=next_follow_up_at,
            force_mode=force_mode,
        )

        # Optional: if you want template_day persisted too, do it here
        if template_day is not None:
            try:
                save_opp(opp, extra_fields={"last_template_day_sent": int(template_day)})
            except Exception:
                pass

    except Exception as e:
        # fail-open: don't break sending due to Airtable flake
        log.warning("Airtable post-send update failed opp=%s err=%s", opp_id, e)
        try:
            _bump_ai_send_metrics_in_airtable(opp_id)  # best-effort legacy
        except Exception:
            pass


def send_patti_email(
    *,
    token,
    subscription_id,
    opp_id,
    rooftop_name,
    rooftop_sender,
    to_addr,
    subject,
    body_html,
    cc_addrs=None,
    reply_to_activity_id=None,
    # ✅ new wiring
    force_mode: str | None = None,
    next_follow_up_at: str | None = None,
    template_day: int | None = None,
    message_id: str | None = None,
    timestamp=None,
    source: str | None = None,
):

    log.info("📬 send_patti_email EMAIL_MODE=%s opp=%s to=%s subject=%s", EMAIL_MODE, opp_id, to_addr, subject)

    # ⛔ Compliance kill switch (centralized)
    try:
        suppressed, reason = is_opp_suppressed(opp_id)
        if suppressed:
            log.info("⛔ Suppressed opp=%s — skipping outbound send (%s)", opp_id, reason)
            return False
    except Exception as e:
        # Recommended: fail-open so a transient Airtable issue doesn't stop all sending.
        # If you prefer strict compliance over continuity, switch back to fail-closed.
        log.warning("Compliance check failed opp=%s — proceeding (fail-open). err=%s", opp_id, e)

    cc_addrs = cc_addrs or []
    sent_ok = False

    if not timestamp:
        timestamp = datetime.now(timezone.utc).isoformat()
    elif isinstance(timestamp, datetime):
        timestamp = timestamp.astimezone(timezone.utc).isoformat()

    resolved_message_id = (
        _normalize_message_id(message_id)
        if message_id
        else _generate_message_id(opp_id, timestamp, subject, to_addr, body_html)
    )
    conversation_record_id = _get_conversation_record_id_by_opportunity_id(opportunity_id=opp_id)
    if not conversation_record_id:
        rec = find_by_opp_id(opp_id=opp_id)
        if isinstance(rec, dict):
            opp = opp_from_record(rec=rec)
            conversation_record_id = _ensure_conversation(opp=opp, channel="email", linked_lead_record_id=rec.get("id", ""))
        else:
            try:
                conversation_bootstrap = Conversation(
                    conversation_id=f"conv_{subscription_id}_{opp_id}",
                    subscription_id=subscription_id,
                    opportunity_id=opp_id,
                    last_channel="email",
                    last_activity_at=_now_iso_utc(),
                    rooftop_name=rooftop_name
                )
                conversation_record_id = upsert_conversation(conversation_bootstrap)
            except Exception as e:
                log.error(f"Conversation upsert failed (send_patti_email) (1): {e}")
                conversation_record_id = ""
                log.warning(f"conversation_record_id for opp_id: {opp_id} could not be determined")

    clean_body_text = _clean_body_html_to_body_text(body_html=body_html)

    # --- CRM path ---
    if EMAIL_MODE != "outlook":
        log.info("📨 MAILER using CRM sendEmailActivity opp=%s", opp_id)
        try:
            sent_ok = bool(
                send_opportunity_email_activity(
                    token,
                    subscription_id,
                    opp_id,
                    sender=rooftop_sender,
                    recipients=[to_addr],
                    carbon_copies=cc_addrs,
                    subject=subject,
                    body_html=body_html,
                    rooftop_name=rooftop_name,
                    reply_to_activity_id=reply_to_activity_id,
                )
            )
        except Exception as e:
            log.warning("CRM send failed opp=%s: %s", opp_id, e)
            sent_ok = False

        if sent_ok:
            _post_send_airtable_update(
                opp_id=opp_id,
                next_follow_up_at=next_follow_up_at,
                force_mode=force_mode,
                template_day=template_day,
            )

            _bump_ai_send_metrics_in_conversations_airtable(opp_id)

        try:
            delivery_status = "sent" if sent_ok else "failed"
            airtable_log = Message(
                message_id=resolved_message_id,
                conversation=conversation_record_id,
                direction="outbound",
                channel="email",
                timestamp=timestamp,
                from_="patti@pattersonautos.com",
                to=to_addr,
                subject=subject,
                body_text=clean_body_text,
                body_html=body_html[:200],
                provider=source,
                opp_id=opp_id,
                delivery_status=delivery_status,
                rooftop_name=rooftop_name,
                rooftop_sender=rooftop_sender,
            )
            message_log_status = log_message(airtable_log)
            if message_log_status:
                log.info("Outbound message logged successfully.")
            else:
                log.error("Outbound message logging failed (send_patti_email) (1).")

        except Exception as e:
            log.error(f"Error during Messages data model construction (send_patti_email): {e}")
        return sent_ok

    # --- Outlook path ---
    log.info("📧 MAILER using Outlook send opp=%s", opp_id)
    try:
        send_email_via_outlook(
            to_addr=to_addr,
            subject=subject,
            html_body=body_html,
            headers={"X-Opportunity-ID": opp_id},
        )
        sent_ok = True
    except Exception as e:
        log.warning("Outlook send failed opp=%s: %s", opp_id, e)
        sent_ok = False

    # Log the outbound to CRM as a COMPLETED ACTIVITY
    if sent_ok and token and subscription_id:
        try:
            complete_send_email_activity(
                token=token,
                subscription_id=subscription_id,
                opportunity_id=opp_id,
                to_addr=to_addr,
                subject=subject,
                body_html=body_html,
            )
            log.info("Completed CRM activity: Send Email opp=%s", opp_id)
        except Exception as e:
            log.warning("Failed to complete 'Send Email' activity opp=%s: %s", opp_id, e)

    if sent_ok:
        _post_send_airtable_update(
            opp_id=opp_id,
            next_follow_up_at=next_follow_up_at,
            force_mode=force_mode,
            template_day=template_day,
        )
        _bump_ai_send_metrics_in_conversations_airtable(opp_id)
    try:
        delivery_status = "sent" if sent_ok else "failed"
        airtable_log = Message(
            message_id=resolved_message_id,
            conversation=conversation_record_id,
            direction="outbound",
            channel="email",
            timestamp=timestamp,
            from_="patti@pattersonautos.com",
            to=to_addr,
            subject=subject,
            body_text=clean_body_text,
            body_html=body_html[:200],
            provider=source,
            opp_id=opp_id,
            delivery_status=delivery_status,
            rooftop_name=rooftop_name,
            rooftop_sender=rooftop_sender,
        )
        message_log_status = log_message(airtable_log)
        if message_log_status:
            log.info("Outbound message logged successfully.")
        else:
            log.error("Outbound message logging failed.")
    except Exception as e:
        log.error(f"Error during outbound message logging (send_patti_email) (2): {e}")
    return sent_ok
