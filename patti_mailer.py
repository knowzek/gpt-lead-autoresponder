import os
import logging
from email_ingestion import clean_html as _clean_html
from outlook_email import send_email_via_outlook
from fortellis import send_opportunity_email_activity
from datetime import datetime, timezone
from fortellis import complete_send_email_activity


log = logging.getLogger("patti.mailer")

EMAIL_MODE = os.getenv("EMAIL_MODE", "outlook")  # "crm" or "outlook"

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

    # --- CRM path (old behavior) ---
    if EMAIL_MODE != "outlook":
        log.info("📨 MAILER using CRM sendEmailActivity opp=%s", opp_id)
        return send_opportunity_email_activity(
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

    # --- Outlook path (new behavior) ---
    log.info("📧 MAILER using Outlook send opp=%s", opp_id)
    try:
        send_email_via_outlook(
            to_addr=to_addr,
            subject=subject,
            html_body=body_html,
            headers={"X-Opportunity-ID": opp_id},
        )
    except Exception as e:
        log.warning("Outlook send failed opp=%s: %s", opp_id, e)
        return False

    # Log the outbound to CRM as a COMPLETED ACTIVITY (not a Note)
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

    return True
