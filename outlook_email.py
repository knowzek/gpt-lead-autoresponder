
import os
import logging
import requests

log = logging.getLogger("patti.outlook")

OUTLOOK_SEND_ENDPOINT = os.getenv("OUTLOOK_SEND_ENDPOINT")

if not OUTLOOK_SEND_ENDPOINT:
    log.warning("OUTLOOK_SEND_ENDPOINT is not set – Outlook sending will fail")
    
def send_email_via_outlook(to_addr, subject, html_body, cc_addrs=None, headers=None, timeout=10):
    if not OUTLOOK_SEND_ENDPOINT:
        log.error("OUTLOOK_SEND_ENDPOINT missing; cannot send Outlook email")
        return

    cc_addrs = cc_addrs or []
    # Outlook V2 is happiest with semicolon-delimited strings
    cc_str = "; ".join([c.strip() for c in cc_addrs if c and c.strip()])

    payload = {
        "to": to_addr,
        "cc": cc_str,              # ✅ first-class
        "subject": subject,
        "html_body": html_body,
        "headers": headers or {},
    }

    try:
        resp = requests.post(OUTLOOK_SEND_ENDPOINT, json=payload, timeout=timeout)
        resp.raise_for_status()
        log.info("Sent Outlook email to %s (cc=%s) via Power Automate", to_addr, cc_str)
    except Exception as e:
        log.exception("Failed to send Outlook email to %s: %s", to_addr, e)
        raise

