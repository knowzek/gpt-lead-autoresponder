
import os
import logging
import requests

log = logging.getLogger("patti.outlook")

OUTLOOK_SEND_ENDPOINT = os.getenv("OUTLOOK_SEND_ENDPOINT")

if not OUTLOOK_SEND_ENDPOINT:
    log.warning("OUTLOOK_SEND_ENDPOINT is not set – Outlook sending will fail")

def send_email_via_outlook(to_addr, subject, html_body, headers=None, timeout=10):
    """
    Sends an email from the Patti Outlook inbox via Power Automate.
    Power Automate flow: 'Patti – Send Email via HTTP'.

    Payload shape MUST match the flow's HTTP trigger schema.
    """
    if not OUTLOOK_SEND_ENDPOINT:
        log.error("OUTLOOK_SEND_ENDPOINT missing; cannot send Outlook email")
        return

    payload = {
        "to": to_addr,
        "subject": subject,
        "html_body": html_body,
        "headers": headers or {}
    }

    try:
        resp = requests.post(
            OUTLOOK_SEND_ENDPOINT,
            json=payload,
            timeout=timeout,
        )
        resp.raise_for_status()
        log.info("Sent Outlook email to %s via Power Automate", to_addr)
    except Exception as e:
        log.exception("Failed to send Outlook email to %s: %s", to_addr, e)
        raise
