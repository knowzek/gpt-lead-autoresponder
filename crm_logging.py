# crm_logging.py
from datetime import datetime, timezone
import logging
from fortellis import schedule_activity

log = logging.getLogger("patti.crm_logging")


def log_email_to_crm(
    *,
    token,
    dealer_key: str,
    opportunity_id: str,
    subject: str,
    body_preview: str,
) -> None:
    """
    Log a simple 'Email' style activity against an opportunity.
    For now we just use schedule_activity; we don't care if CRM
    renders this as Appointment vs Email as long as it's searchable.
    """
    if not opportunity_id:
        log.warning("log_email_to_crm called with no opportunity_id")
        return

    now_utc = datetime.now(timezone.utc).isoformat()

    comments = f"EMAIL via Patti Outlook\n\nSubject: {subject}\n\nPreview: {body_preview}"

    try:
        schedule_activity(
            token=token,
            dealer_key=dealer_key,
            opportunity_id=opportunity_id,
            due_dt_iso_utc=now_utc,
            activity_name="Patti Email (Outlook)",
            activity_type="Email",
            comments=comments,
        )
        log.info("Logged Patti Outlook email to CRM for opp %s", opportunity_id)
    except Exception as e:
        log.error("Failed to log Patti email activity for opp %s: %s", opportunity_id, e)
