import re
from esQuerys import esClient
from es_resilient import es_update_with_retry
from gpt import run_gpt
from rooftops import get_rooftop_info
from fortellis import send_opportunity_email_activity

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

    sender = (inbound.get("from") or "").lower().strip()
    subject = inbound.get("subject") or ""
    body_html = inbound.get("body_html") or ""
    body_text = inbound.get("body_text") or clean_html(body_html)
    timestamp = inbound.get("timestamp")
    headers = inbound.get("headers") or {}

    # 1️⃣ Try to extract opportunityId from header (best method)
    opp_id = headers.get("X-Opportunity-ID") or None

    # 2️⃣ If no header, fallback: find opp by email address match (non-nested)
    if not opp_id:
        # strip display name if present, e.g. "Kristin <foo@bar.com>"
        import re
        m = re.search(r"<([^>]+)>", sender)
        sender_email = (m.group(1) if m else sender).strip().lower()

        # try several likely fields; missing fields just return 0 hits, no error
        query = {
            "bool": {
                "should": [
                    {"term": {"customerEmail.keyword": sender_email}},
                    {"term": {"customer.email.keyword": sender_email}},
                    {"term": {"customerEmail": sender_email}},
                ],
                "minimum_should_match": 1
            }
        }

        rsp = esClient.search(index="opportunities", query=query, size=1)
        hits = rsp["hits"]["hits"]
        if not hits:
            log.warning("No matching opportunity found for inbound email %s", sender_email)
            return

        opp_id = hits[0]["_id"]
        opportunity = hits[0]["_source"]
    else:
        opportunity = esClient.get(index="opportunities", id=opp_id)["_source"]

    # 3️⃣ Append the message into the ES conversation thread
    msg_dict = {
        "msgFrom": "customer",
        "subject": subject,
        "body": body_text,
        "date": timestamp,
    }

    opportunity.setdefault("messages", []).append(msg_dict)

    # Save updated opportunity in ES
    es_update_with_retry(
        esClient, "opportunities", opp_id, {"messages": opportunity["messages"]}
    )

    # 4️⃣ Generate Patti reply with existing logic
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
        prevMessages=True
    )

    # Build email
    subject_out = response["subject"]
    body_out = response["body"]

    # 5️⃣ Send reply using Fortellis email endpoint
    customer_email = (
        ((opportunity.get("customer") or {}).get("emails") or [{}])[0].get("address")
    )

    token = get_token(opportunity["_subscription_id"])

    send_opportunity_email_activity(
        token,
        opportunity["_subscription_id"],
        opp_id,
        sender=get_rooftop_info(opportunity["_subscription_id"])["sender"],
        recipients=[customer_email],
        carbon_copies=[],
        subject=subject_out,
        body_html=body_out,
        rooftop_name=rooftop_name,
    )
