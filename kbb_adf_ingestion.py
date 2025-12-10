# kbb_adf_ingestion.py
import re
import logging
from datetime import datetime, timezone

from esQuerys import esClient
from gpt import run_gpt
from feature_flags import is_test_opp
from outlook_email import send_email_via_outlook
from crm_logging import log_email_to_crm
from fortellis import get_token
from rooftops import get_rooftop_info
from email_ingestion import clean_html  # reuse helper

log = logging.getLogger("patti.kbb_adf")


def _extract_shopper_email(body_text: str) -> str | None:
    """
    Parse the ADF notification email text to find shopper's email.
    Example lines:
      Email: knowzek@gmail.com
    """
    if not body_text:
        return None

    m = re.search(r"Email:\s*([^\s]+@[^\s]+)", body_text, re.IGNORECASE)
    if not m:
        return None
    return m.group(1).strip().lower()


def process_kbb_adf_notification(inbound: dict) -> None:
    """
    Handle the initial 'Offer Created from KBB' email.

    We:
      1. Extract shopper email from the ADF notification body.
      2. Find the most recent KBB opportunity in ES for that email.
      3. If it's a test opp, send the first Patti email via Outlook and log to CRM.
    """
    subject = inbound.get("subject") or ""
    body_html = inbound.get("body_html") or ""
    body_text = inbound.get("body_text") or clean_html(body_html)

    shopper_email = _extract_shopper_email(body_text)
    if not shopper_email:
        log.warning("KBB ADF inbound had no shopper email; subject=%s", subject)
        return

    # Find KBB opportunity by shopper email
    query = {
        "bool": {
            "must": [
                {
                    "terms": {
                        "source.keyword": [
                            "KBB Instant Cash Offer",
                            "KBB ServiceDrive",
                        ]
                    }
                }
            ],
            "should": [
                # main ES structure
                {"term": {"customer.emails.address.keyword": shopper_email}},
                {"term": {"customer.emails.address": shopper_email}},
                # older / alternate mappings, just in case
                {"term": {"customerEmail.keyword": shopper_email}},
                {"term": {"customer.email.keyword": shopper_email}},
                {"term": {"customerEmail": shopper_email}},
            ],

            "minimum_should_match": 1,
        }
    }

    rsp = esClient.search(
        index="opportunities",
        query=query,
        size=1,
        sort=[{"created_at": {"order": "desc"}}],
    )
    hits = rsp["hits"]["hits"]
    if not hits:
        log.warning("No KBB opportunity found for shopper email %s", shopper_email)
        return

    hit = hits[0]
    opp_id = hit["_id"]
    opportunity = hit["_source"]

    # Gate: only run Outlook Patti for test opps on this branch
    if not is_test_opp(opportunity):
        log.info(
            "KBB ADF inbound matched opp %s but it's not a test opp; skipping Outlook flow",
            opp_id,
        )
        return

    # Build a simple Day-1 style email via GPT
    customer_name = (opportunity.get("customer") or {}).get("firstName") or "there"
    rooftop_name = (
        opportunity.get("rooftop_name")
        or opportunity.get("rooftop")
        or "Tustin Mazda"
    )

    some_prompt = f"""
You are Patti, a friendly acquisition specialist at {rooftop_name}.
A new Kelley Blue Book Instant Cash Offer lead just came in.

Write the FIRST outreach email to the customer to schedule a time
for them to bring their vehicle in for inspection and finalize their offer.

Keep it:
- short and clear (3–5 short paragraphs)
- specific to Instant Cash Offer
- warm, professional, and human
Do NOT include subject line in the body.
"""

    reply = run_gpt(
        prompt=some_prompt,
        customer_name=shopper_first_name,
        rooftop_name=rooftop_name,
        prevMessages=False,
        persona="kbb_ico",
        kbb_ctx={"source": "kbb_adf"},
    )
    
    subject_out = reply.get("subject") or f"Your Kelley Blue Book Instant Cash Offer with {rooftop_name}"
    body_out = reply.get("body") or "Thanks for your interest in your Kelley Blue Book Instant Cash Offer."


    # Send via Outlook
    send_email_via_outlook(
        to_addr=shopper_email,
        subject=subject_out,
        html_body=body_out,
        headers={"X-Opportunity-ID": opp_id},
    )

    # Log to CRM
    dealer_key = opportunity.get("_subscription_id")
    if dealer_key:
        token = get_token(dealer_key)
        log_email_to_crm(
            token=token,
            dealer_key=dealer_key,
            opportunity_id=opp_id,
            subject=subject_out,
            body_preview=clean_html(body_out)[:500],
        )
    else:
        log.warning("KBB opp %s missing _subscription_id; cannot log to CRM", opp_id)
