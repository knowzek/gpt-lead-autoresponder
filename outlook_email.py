# outlook_email.py
import os
import logging
import requests

log = logging.getLogger("patti.outlook")

OUTLOOK_WEBHOOK_URL = os.getenv("OUTLOOK_SEND_WEBHOOK_URL")


def send_email_via_outlook(
    *,
    to_addr: str,
    subject: str,
    html_body: str,
    headers: dict | None = None,
) -> None:
    """
    Send an email out of patti@pattersonautos.com via a Power Automate HTTP flow.
    """

    if not OUTLOOK_WEBHOOK_URL:
        log.error("OUTLOOK_SEND_WEBHOOK_URL is not configured; cannot send email")
        return

    payload = {
        "to": to_addr,
        "subject": subject,
        "html_body": html_body,
        "headers": headers or {},
    }

    try:
        resp = requests.post(OUTLOOK_WEBHOOK_URL, json=payload, timeout=20)
        resp.raise_for_status()
        log.info("Outlook email sent to %s (len=%d)", to_addr, len(html_body))
    except Exception as e:
        log.error("Failed to send Outlook email to %s: %s", to_addr, e)
