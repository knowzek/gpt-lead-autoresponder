import os
import logging
from datetime import datetime, timezone

from outlook_email import send_email_via_outlook
from fortellis import send_opportunity_email_activity, complete_send_email_activity
from airtable_store import find_by_opp_id, opp_from_record, save_opp, is_opp_suppressed, patch_by_id

log = logging.getLogger("patti.mailer")

EMAIL_MODE = os.getenv("EMAIL_MODE", "outlook")  # "crm" or "outlook"


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def _bump_ai_send_metrics_in_airtable(opp_id: str) -> None:
    try:
        rec = find_by_opp_id(opp_id)
        if not rec:
            return

        f = rec.get("fields", {}) or {}
        current = int(f.get("AI Messages Sent") or 0)

        when_iso = _now_iso_utc()

        patch_by_id(rec["id"], {
            "AI Messages Sent": current + 1,
            "AI First Message Sent At": f.get("AI First Message Sent At") or when_iso,
            "Last AI Message At": when_iso,
        })

    except Exception as e:
        log.warning("AI metrics update failed (non-blocking) opp=%s err=%s", opp_id, str(e)[:500])


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
    log.info("📬 send_patti_email EMAIL_MODE=%s opp=%s to=%s subject=%s",
             EMAIL_MODE, opp_id, to_addr, subject)

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
                body_html=body_html,
            )
            log.info("Completed CRM activity: Send Email opp=%s", opp_id)
        except Exception as e:
            log.warning("Failed to complete 'Send Email' activity opp=%s: %s", opp_id, e)

    if sent_ok:
        _bump_ai_send_metrics_in_airtable(opp_id)

    return sent_ok
