import re
import os
import logging
import requests
from airtable_store import is_opp_suppressed
log = logging.getLogger("patti.outlook")

OUTLOOK_SEND_ENDPOINT = os.getenv("OUTLOOK_SEND_ENDPOINT")

if not OUTLOOK_SEND_ENDPOINT:
    log.warning("OUTLOOK_SEND_ENDPOINT is not set – Outlook sending will fail")

_EXTERNAL_SENDER_PREFIX_RE = re.compile(
    r"^\s*(?:\[\s*EXTERNAL\s+SENDER\s*\]\s*)+",
    re.IGNORECASE,
)
    
def send_email_via_outlook(
    to_addr,
    subject,
    html_body,
    *,
    opp_id=None,
    cc_addrs=None,
    headers=None,
    timeout=10,
    enforce_compliance=True,   
):
    if not OUTLOOK_SEND_ENDPOINT:
        log.error("OUTLOOK_SEND_ENDPOINT missing; cannot send Outlook email")
        return

    # ⛔ Compliance kill switch (covers bypasses)
    if enforce_compliance:
        try:
            _opp_id = (opp_id or "").strip()
            if not _opp_id:
                hdrs = headers or {}
                _opp_id = (hdrs.get("X-Opportunity-ID") or hdrs.get("x-opportunity-id") or "").strip()

            if _opp_id:
                suppressed, reason = is_opp_suppressed(_opp_id)
                if suppressed:
                    log.info("⛔ Suppressed opp=%s — blocking Outlook send (%s)", _opp_id, reason)
                    return
        except Exception as e:
            log.warning(
                "Compliance check failed in send_email_via_outlook — proceeding (fail-open). err=%s",
                e,
            )

    # Normalize headers and inject opp id
    headers = headers or {}
    if opp_id and "X-Opportunity-ID" not in headers:
        headers["X-Opportunity-ID"] = opp_id

    # ✅ strip "[EXTERNAL SENDER]" from the FRONT of the subject, always
    if isinstance(subject, str) and subject.strip():
        subject = _EXTERNAL_SENDER_PREFIX_RE.sub("", subject).strip()

    cc_addrs = cc_addrs or []
    cc_str = "; ".join([c.strip() for c in cc_addrs if c and c.strip()])

    payload = {
        "to": to_addr,
        "cc": cc_str,
        "subject": subject,
        "html_body": html_body,
        "headers": headers,
    }

    resp = requests.post(OUTLOOK_SEND_ENDPOINT, json=payload, timeout=timeout)
    resp.raise_for_status()
    log.info("Sent Outlook email to %s (cc=%s) via Power Automate", to_addr, cc_str)
