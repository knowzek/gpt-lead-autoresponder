import os
import logging
from datetime import datetime, timezone

from outlook_email import send_email_via_outlook
from fortellis import send_opportunity_email_activity, complete_send_email_activity

from airtable_store import find_by_opp_id, opp_from_record, save_opp  # ✅ add

log = logging.getLogger("patti.mailer")

EMAIL_MODE = os.getenv("EMAIL_MODE", "outlook")  # "crm" or "outlook"


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bump_ai_send_metrics_in_airtable(opp_id: str) -> None:
    """
    After a successful outbound send, increment AI messages + timestamps in Airtable.
    Non-blocking.
    """
    try:
        rec = find_by_opp_id(opp_id)
        if not rec:
            return

        opp = opp_from_record(rec)
        m = opp.setdefault("patti_metrics", {})

        when_iso = _now_iso_utc()
        m["ai_messages_sent"] = int(m.get("ai_messages_sent") or 0) + 1
        m.setdefault("ai_first_message_sent_at", when_iso)
        m["last_ai_message_at"] = when_iso

        # ⚠️ Must match your Airtable column names exactly:
        save_opp(opp, extra_fields={
            "AI Messages Sent": m["ai_messages_sent"],
            "AI First Message Sent At": m["ai_first_message_sent_at"],
            "Last AI Message At": m["last_ai_message_at"],
        })

    except Exception as e:
        log.warning("AI metrics update failed (non-blocking) opp=%s err=%s", opp_id, str(e)[:200])


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
):
    cc_addrs = cc_addrs or []
    log.info("📬 send_patti_email EMAIL_MODE=%s opp=%s to=%s subject=%s", EMAIL_MODE, opp_id, to_addr, subject)

    sent_ok = False

    # --- CRM path ---
    if EMAIL_MODE != "outlook":
        log.info("📨 MAILER using CRM sendEmailActivity opp=%s", opp_id)
        try:
            sent_ok = bool(send_opportunity_email_activity(
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
            ))
        except Exception as e:
            log.warning("CRM send failed opp=%s: %s", opp_id, e)
            sent_ok = False

        if sent_ok:
            _bump_ai_send_metrics_in_airtable(opp_id)

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
        return False

    # Log the outbound to CRM as a COMPLETED ACTIVITY
    if token and subscription_id:
        try:
            complete_send_email_activity(
                token=token,
                subscription_id=subscription_id,
                opportunity_id=opp_id,
                to_addr=to_addr,
                subject=subject,
            )
            log.info("Completed CRM activity: Send Email opp=%s", opp_id)
        except Exception as e:
            log.warning("Failed to complete 'Send Email' activity opp=%s: %s", opp_id, e)

    # ✅ Metrics hook (only once, only on success)
    if sent_ok:
        _bump_ai_send_metrics_in_airtable(opp_id)

    return sent_ok

