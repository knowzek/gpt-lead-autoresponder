import re
from esQuerys import esClient
from es_resilient import es_update_with_retry
from gpt import run_gpt
from rooftops import get_rooftop_info
from fortellis import get_token, send_opportunity_email_activity
TEST_OPP_ID = "050a81e9-78d4-f011-814f-00505690ec8c"
KBB_SOURCE_HINTS = ("kbb", "instant cash offer", "ico")
from outlook_email import send_email_via_outlook  
from crm_logging import log_email_to_crm          
from feature_flags import is_test_opp             



from datetime import datetime, timezone
import logging
log = logging.getLogger("patti.email_ingestion")


def clean_html(html: str) -> str:
    """Strip HTML tags and reduce to plain text."""
    text = re.sub(r"(?is)<[^>]+>", " ", html or "")
    return re.sub(r"\s+", " ", text).strip()


def process_inbound_email(inbound):
    """
    Convert an inbound email into a Patti customer message and
    trigger Patti's reply workflow — WITHOUT calling Fortellis
    Activity History.
    """

    sender = (inbound.get("from") or "").strip()
    sender_lower = sender.lower()
    subject = inbound.get("subject") or ""
    body_html = inbound.get("body_html") or ""
    body_text = inbound.get("body_text") or clean_html(body_html)
    timestamp = inbound.get("timestamp")
    headers = inbound.get("headers") or {}

    # 1️⃣ Normalize sender email (strip display name)
    import re
    m = re.search(r"<([^>]+)>", sender_lower)
    sender_email = (m.group(1) if m else sender_lower).strip().lower()

    # 2️⃣ Best case: try to get opportunityId from header
    opp_id = headers.get("X-Opportunity-ID") or None
    opportunity = None

    if opp_id:
        # we had an opp_id from header
        doc = esClient.get(index="opportunities", id=opp_id)
        opportunity = doc["_source"]
    else:
        # 3️⃣ Fallback: find opp by customer email
        query = {
            "bool": {
                "should": [
                    # actual structure: customer.emails[].address
                    {"term": {"customer.emails.address.keyword": sender_email}},
                    {"term": {"customer.emails.address": sender_email}},
                    # keep some fallbacks in case other rooftops index differently
                    {"term": {"customerEmail.keyword": sender_email}},
                    {"term": {"customerEmail": sender_email}},
                ],
                "minimum_should_match": 1,
            }
        }

        rsp = esClient.search(index="opportunities", query=query, size=1)
        hits = rsp["hits"]["hits"]
        if not hits:
            log.warning(
                "No matching opportunity found for inbound email %s",
                sender_email,
            )
            return

        opp_id = hits[0]["_id"]
        opportunity = hits[0]["_source"]

    # Safety: if for some reason we still don't have an opportunity
    if not opportunity:
        log.warning("Inbound email resolved opp_id=%s but no source doc", opp_id)
        return

    # 🔒 Gate: only run Outlook-based flow for *test opps* on this branch
    if not is_test_opp(opportunity):
        log.info(
            "Inbound email for opp %s is not a test opp; skipping Outlook Patti flow",
            opp_id,
        )
        return

    # 4️⃣ Append the message into the ES conversation thread
    msg_dict = {
        "msgFrom": "customer",
        "subject": subject,
        "body": body_text,
        "date": timestamp,
    }

    opportunity.setdefault("messages", []).append(msg_dict)

    # Save updated opportunity in ES (same pattern as kbb_ico)
    es_update_with_retry(
        esClient,
        index="opportunities",
        id=opp_id,
        doc={"messages": opportunity["messages"]},
    )

    # 5️⃣ Generate Patti reply with existing logic
    rooftop_name = get_rooftop_info(opportunity["_subscription_id"])["name"]
    first_name = (opportunity.get("customer") or {}).get("firstName")

    prompt = f"""
Generate Patti's next reply based on this email thread:
{opportunity["messages"]}
"""

    response = run_gpt(
        prompt,
        first_name,
        rooftop_name,
        prevMessages=True,
    )

    subject_out = response["subject"]
    body_out = response["body"]

    # 6️⃣ Send reply using Outlook (patti@pattersonautos.com) and log to CRM

    # Use the sender email we normalized earlier
    customer_email = sender_email

    # Send email out of Patti's inbox
    send_email_via_outlook(
        to_addr=customer_email,
        subject=subject_out,
        html_body=body_out,
        headers={"X-Opportunity-ID": opp_id},
    )

    # Log the email back to CRM as an activity
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
        log.warning(
            "Opportunity %s missing _subscription_id; cannot log to CRM",
            opp_id,
        )
