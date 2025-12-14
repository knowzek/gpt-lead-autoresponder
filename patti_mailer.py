import os
import logging
from email_ingestion import clean_html as _clean_html
from outlook_email import send_email_via_outlook
from fortellis import send_opportunity_email_activity, add_opportunity_comment

log = logging.getLogger("patti.mailer")

EMAIL_MODE = os.getenv("EMAIL_MODE", "crm")  # "crm" or "outlook"

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

    # --- CRM path (old behavior) ---
    if EMAIL_MODE != "outlook":
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
    send_email_via_outlook(
        to_addr=to_addr,
        subject=subject,
        html_body=body_html,
        headers={"X-Opportunity-ID": opp_id},
    )

    # Always log the outbound to CRM as a comment
    if token and subscription_id:
        try:
            preview = _clean_html(body_html)[:500]
            add_opportunity_comment(
                token,
                subscription_id,
                opp_id,
                f"Outbound email to {to_addr}: {subject}\n\n{preview}",
            )
        except Exception as e:
            log.warning("Failed to log Outlook outbound email for opp %s: %s", opp_id, e)
