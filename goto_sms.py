# goto_sms.py
import os
import time
import requests
import logging

log = logging.getLogger("patti.goto_sms")

GOTO_TOKEN_URL = "https://authentication.logmeininc.com/oauth/token"
GOTO_SMS_URL   = "https://api.goto.com/messaging/v1/messages"

# Simple in-memory cache so we don't request a token on every send
_ACCESS_TOKEN = None
_ACCESS_TOKEN_EXP = 0

GOTO_API = "https://api.goto.com"

def list_conversations(owner_phone_e164: str, sort: str = "DESCENDINGLAST_MESSAGE_TIMESTAMP", limit: int = 20):
    url = f"{GOTO_API}/messaging/v1/conversations"
    params = {
        "ownerPhoneNumber": owner_phone_e164,
        "sort": sort,
        "limit": limit,
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
    access_token = _get_access_token()

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

    r = requests.post(GOTO_SMS_URL, json=payload, headers=headers, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"GoTo send_sms failed {r.status_code}: {r.text[:800]}")
    return r.json() or {}
