# goto_sms.py
from datetime import datetime, timezone
import os
import time
import requests
import logging
from airtable_store import _generate_message_id, _normalize_message_id, find_by_customer_phone, opp_from_record
from airtable_store import log_message, _get_conversation_record_id_by_opportunity_id
from email_ingestion import _norm_phone_e164_us
from models.airtable_model import Message
from rooftops import get_rooftop_info

log = logging.getLogger("patti.goto_sms")


GOTO_TOKEN_URL = "https://authentication.logmeininc.com/oauth/token"
GOTO_SMS_URL = "https://api.goto.com/messaging/v1/messages"

# Simple in-memory cache so we don't request a token on every send
_ACCESS_TOKEN = None
_ACCESS_TOKEN_EXP = 0

GOTO_API = "https://api.goto.com"


def list_conversations(owner_phone_e164: str):
    url = f"{GOTO_API}/messaging/v1/conversations"
    params = {
        "ownerPhoneNumber": owner_phone_e164,
    }
    r = requests.get(url, headers=_auth_headers(), params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def list_messages(owner_phone_e164: str, contact_phone_e164: str, limit: int = 20):
    url = f"{GOTO_API}/messaging/v1/messages"
    params = {
        "ownerPhoneNumber": owner_phone_e164,
        "contactPhoneNumber": contact_phone_e164,
        "limit": limit,
    }
    r = requests.get(url, headers=_auth_headers(), params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def _auth_headers():
    token = _get_access_token()
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _get_access_token() -> str:
    """
    Uses PAT -> OAuth access token exchange (the flow you got working in Postman).
    Requires:
      - GOTO_CLIENT_ID
      - GOTO_CLIENT_SECRET
      - GOTO_PAT
    """
    global _ACCESS_TOKEN, _ACCESS_TOKEN_EXP

    now = int(time.time())
    if _ACCESS_TOKEN and now < (_ACCESS_TOKEN_EXP - 30):
        return _ACCESS_TOKEN

    client_id = _env("GOTO_CLIENT_ID")
    client_secret = _env("GOTO_CLIENT_SECRET")
    pat = _env("GOTO_PAT")

    if not client_id or not client_secret or not pat:
        raise RuntimeError("Missing GoTo env vars: GOTO_CLIENT_ID, GOTO_CLIENT_SECRET, GOTO_PAT")

    auth = (client_id, client_secret)
    data = {
        "grant_type": "personal_access_token",
        "pat": pat,
    }

    r = requests.post(GOTO_TOKEN_URL, auth=auth, data=data, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"GoTo token exchange failed {r.status_code}: {r.text[:800]}")

    j = r.json() or {}
    token = j.get("access_token")
    expires_in = int(j.get("expires_in") or 3600)

    if not token:
        raise RuntimeError(f"GoTo token exchange missing access_token: {str(j)[:500]}")

    _ACCESS_TOKEN = token
    _ACCESS_TOKEN_EXP = now + expires_in
    return token


def send_sms(*, from_number: str, to_number: str, body: str) -> dict:
    """
    Send one SMS via GoTo.
    Returns the API response JSON (includes ids you can store in Airtable).
    """
    # ✅ Global SMS kill switch (no redeploy needed; flip env var)
    if (os.getenv("SMS_KILL_SWITCH", "0").strip() == "1"):
        log.warning(
            "🛑 SMS_KILL_SWITCH=1 — blocked SMS send from=%s to=%s body=%r",
            from_number,
            to_number,
            (body or "")[:180],
        )
        # Return a stub that looks like a normal response
        return {
            "blocked": True,
            "reason": "SMS_KILL_SWITCH",
            "conversationId": "",
            "id": "",
        }
    
    access_token = _get_access_token()
    rooftop_name = ""
    rooftop_sender = ""
    rec = None
    opp_id = ""
    record_id = ""

    payload = {
        "ownerPhoneNumber": from_number,
        "contactPhoneNumbers": [to_number],
        "body": body,
    }

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    timestamp = datetime.now(timezone.utc).isoformat()

    r = requests.post(GOTO_SMS_URL, json=payload, headers=headers, timeout=30)

    response_json = r.json() or {}
    response_message_id = response_json.get("id", "")

    opp = {}
    e164_to_number = None
    try:
        e164_to_number = _norm_phone_e164_us(to_number)
        rec = find_by_customer_phone(e164_to_number)
        if not rec:
            log.error(f"Could not fetch record by customer's phone number: {e164_to_number}")
        if rec:
            opp = opp_from_record(rec)
    except Exception as e:
        log.error(f"Failed to fech opp (send_sms): {e}")

    if rec:
        opp_id = rec.get("opp_id", "")
        record_id = _get_conversation_record_id_by_opportunity_id(opp_id) or ""
        if not record_id:
            raise RuntimeError(f"Conversation does exists with opp_id: {opp_id}")
        subscription_id = rec.get("subscription_id", "")
        rooftop_info = get_rooftop_info(subscription_id) or {}
        rooftop_name = rooftop_info.get("name", "")
        rooftop_sender = rooftop_info.get("sender", "")

    delivery_status = "failed" if r.status_code >= 400 else "sent"

    message_id = (
        _generate_message_id(opp_id=opp_id, timestamp=timestamp, to_addr=to_number, body_html=body)
        if r.status_code >= 400
        else _normalize_message_id(response_message_id)
    )
    try:
        airtable_log = Message(
            message_id=message_id,
            conversation=record_id,
            direction="outbound",
            channel="sms",
            timestamp=timestamp,
            from_=from_number,
            to=e164_to_number or to_number,
            subject="",
            body_text=body,
            body_html="",
            provider=opp.get("source", "") or "",
            opp_id=opp_id,
            delivery_status=delivery_status,
            rooftop_name=rooftop_name,
            rooftop_sender=rooftop_sender,
        )
        message_log_status = log_message(airtable_log)
        (
            log.info("outbound sms logged successfully to airtables")
            if message_log_status
            else log.error("outbound sms logging failed.")
        )
    except Exception as e:
        log.error(f"Failed to log sms to Messages (send_sms): {e}")

    if r.status_code >= 400:
        raise RuntimeError(f"GoTo send_sms failed {r.status_code}: {r.text[:800]}")

    return response_json or {}
