# fortellis.py
import os
import uuid
import requests
from datetime import datetime, timedelta
import json
import time
import logging

LOG_LEVEL = os.getenv("APP_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fortellis")


SENSITIVE_HEADERS = {"Authorization"}

def _mask_headers(h: dict) -> dict:
    h = dict(h or {})
    for k in list(h.keys()):
        if k in SENSITIVE_HEADERS:
            h[k] = "***redacted***"
    return h

def _log_txn_compact(level, *, method, url, headers, status, duration_ms, request_id, note=None):
    sub_id = (headers or {}).get("Subscription-Id", "N/A")
    msg = f"{method} {url} status={status} dur_ms={duration_ms} req_id={request_id} sub_id={sub_id}"
    if note:
        msg += f" note={note}"
    log.log(level, msg)



BASE_URL = os.getenv("FORTELLIS_BASE_URL", "https://api.fortellis.io")  # prod default
LEADS_BASE = "/cdk/sales/elead/v1/leads"
OPPS_BASE        = "/sales/v2/elead/opportunities"   
ACTIVITIES_BASE  = "/sales/v1/elead/activities" 
CUSTOMERS_BASE  = "/cdk/sales/elead/v1/customers"
REFDATA_BASE     = "/cdk/sales/elead/v1/reference-data" # if you call product reference data via CRM
MESSAGING_BASE   = "/cdk/sales/elead/v1/messaging"      # if you call CRM Post Messaging
SUB_MAP = json.loads(os.getenv("FORTELLIS_SUBSCRIPTIONS_JSON","{}"))
# Fortellis Identity token endpoint (prod)
AUTH_SERVER_ID = os.getenv("FORTELLIS_AUTH_SERVER_ID", "aus1p1ixy7YL8cMq02p7")
TOKEN_URL = os.getenv(
    "FORTELLIS_TOKEN_URL",
    f"https://identity.fortellis.io/oauth2/{AUTH_SERVER_ID}/v1/token"
)


def get_token(dealer_key: str):
    # Note: Subscription-Id is NOT required on the token call; it’s used on API calls.
    headers = {
        "Accept": "application/json",
        "Cache-Control": "no-cache",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "client_credentials",
        "scope": "anonymous",
    }
    # Prefer HTTP Basic for client_id/secret per Fortellis examples
    resp = requests.post(TOKEN_URL, headers=headers, data=data,
                         auth=(CLIENT_ID, CLIENT_SECRET), timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]


def _headers(dealer_key: str, token: str, extra: dict | None = None) -> dict:
    # ✅ Guard: make sure this dealer_key exists in SUB_MAP
    sub_id = SUB_MAP.get(dealer_key)
    if not sub_id:
        raise KeyError(f"Unknown dealer_key '{dealer_key}'. Valid: {list(SUB_MAP.keys())}")

    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": sub_id,
        "Request-Id": str(uuid.uuid4()),
        "Accept": "application/json",
    }
    if extra:
        headers.update(extra)
    return headers
  
CLIENT_ID = os.getenv("FORTELLIS_CLIENT_ID")
CLIENT_SECRET = os.getenv("FORTELLIS_CLIENT_SECRET")

def post_and_wrap(method, url, *, headers, payload=None, json_body=None):
    body_to_send = payload if payload is not None else json_body
    resp = _request(method, url, headers=headers, json_body=body_to_send)
    try:
        body = resp.json() if resp.text else None
    except ValueError:
        body = None
    result = {"status": resp.status_code, "requestId": getattr(resp, "request_id", None)}
    if isinstance(body, dict):
        result.update(body)
    return result


def _request(method, url, headers=None, json_body=None, params=None):
    t0 = time.time()
    req_id = (headers or {}).get("Request-Id")
    try:
        resp = requests.request(method, url, headers=headers, json=json_body, params=params, timeout=30)
        dt = int((time.time() - t0) * 1000)

        # success path: compact info only
        _log_txn_compact(
            logging.INFO,
            method=method, url=url, headers=_mask_headers(headers),
            status=resp.status_code, duration_ms=dt, request_id=req_id,
            note=None
        )

        # surface non-2xx with a short preview to help debug quickly
        if not (200 <= resp.status_code < 300):
            preview = (resp.text or "")[:400].replace("\n", " ")
            _log_txn_compact(
                logging.WARN,
                method=method, url=url, headers=_mask_headers(headers),
                status=resp.status_code, duration_ms=dt, request_id=req_id,
                note=f"non-2xx body_preview={preview}"
            )

        resp.raise_for_status()
        resp.request_id = req_id
        return resp

    except requests.RequestException as e:
        dt = int((time.time() - t0) * 1000)
        note = f"exception={type(e).__name__} msg={str(e)[:200].replace(chr(10),' ')}"
        _log_txn_compact(
            logging.ERROR,
            method=method, url=url, headers=_mask_headers(headers or {}),
            status=getattr(e.response, "status_code", "ERR"),
            duration_ms=dt, request_id=req_id, note=note
        )
        raise


def send_opportunity_email_activity(token, dealer_key,
                                    opportunity_id, sender,
                                    recipients, carbon_copies,
                                    subject, body_html):
    url = f"{BASE_URL}{OPPS_BASE}/sendEmail"
    payload = {
        "opportunityId": opportunity_id,
        "message": {
            "from": sender,
            "recipients": recipients,
            "carbonCopies": carbon_copies or [],
            "subject": subject,
            "body": body_html,
            "isHtml": True
        }
    }
    return post_and_wrap("POST", url, headers=_headers(dealer_key, token), json_body=payload)


def add_opportunity_comment(token, dealer_key, opportunity_id, comment_text):
    url = f"{BASE_URL}{OPPS_BASE}/comment"
    payload = {
        "opportunityId": opportunity_id,
        "comment": comment_text
    }
    return post_and_wrap("POST", url, headers=_headers(dealer_key, token), json_body=payload)


def add_vehicle_sought(token, dealer_key, opportunity_id,
                       is_new=True, year_from=None, year_to=None,
                       make="", model="", trim="", stock_number="", is_primary=True):
    url = f"{BASE_URL}{OPPS_BASE}/vehicleSought"
    payload = {
        "opportunityId": opportunity_id,
        "isNew": bool(is_new),
        "yearFrom": year_from,
        "yearTo": year_to,
        "make": make,
        "model": model,
        "trim": trim,
        "stockNumber": stock_number,
        "isPrimary": bool(is_primary)
    }
    return post_and_wrap("POST", url, headers=_headers(dealer_key, token), json_body=payload)

import uuid
from datetime import datetime, timedelta

def _coerce_activity_type(value):
    """
    Accepts an int (pass-through), a numeric string ('14' -> 14),
    or a small set of known labels -> codes. Raise if unrecognized.
    """
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)

    # Known-safe label(s) you've confirmed in Postman:
    LABEL_TO_CODE = {
        "Send Email/Letter": 14,
        # Add more when you verify their exact numeric codes.
    }
    if isinstance(value, str) and value in LABEL_TO_CODE:
        return LABEL_TO_CODE[value]

    raise ValueError(f"Unrecognized activityType: {value!r}. Use a numeric code "
                     "or a known label like 'Send Email/Letter'.")

def schedule_activity(
    token,
    dealer_key,
    opportunity_id,
    *,
    due_dt_iso_utc,
    activity_name,
    activity_type,
    comments=""
):
    url = f"{BASE_URL}{ACTIVITIES_BASE}/schedule"
    payload = {
        "opportunityId": opportunity_id,
        "dueDate": due_dt_iso_utc,
        "activityName": activity_name,
        "activityType": _coerce_activity_type(activity_type),
        "comments": comments or ""
    }
    return post_and_wrap("POST", url, headers=_headers(dealer_key, token), json_body=payload)


def complete_activity(
    token,
    dealer_key,
    opportunity_id,
    *,
    due_dt_iso_utc,
    completed_dt_iso_utc,
    activity_name,
    activity_type,
    comments="",
    activity_id=None,
):
    url = f"{BASE_URL}{ACTIVITIES_BASE}/complete"
    payload = {
        "opportunityId": opportunity_id,
        "dueDate": due_dt_iso_utc,
        "completedDate": completed_dt_iso_utc,
        "activityName": activity_name,
        "activityType": _coerce_activity_type(activity_type),
        "comments": comments or ""
    }
    if activity_id:
        payload["activityId"] = activity_id

    return post_and_wrap("POST", url, headers=_headers(dealer_key, token), json_body=payload)


def search_activities_by_opportunity(opportunity_id, token, dealer_key, page=1, page_size=10):
    url = f"{BASE_URL}{ACTIVITIES_BASE}/search"
    payload = {
        "filters": [{"field": "opportunityId", "operator": "eq", "value": opportunity_id}],
        "sort": [{"field": "createdDate", "direction": "desc"}],
        "page": page,
        "pageSize": page_size
    }
    resp = requests.post(url, headers=_headers(dealer_key, token), json=payload)
    resp.raise_for_status()
    return resp.json().get("items", [])


def get_activity_by_url(url, token, dealer_key):
    resp = requests.get(url, headers=_headers(dealer_key, token))
    resp.raise_for_status()
    return resp.json()


def get_activity_by_id_v1(activity_id, token, dealer_key):
    url = f"{BASE_URL}{ACTIVITIES_BASE}/{activity_id}"
    resp = requests.get(url, headers=_headers(dealer_key, token))
    resp.raise_for_status()
    return resp.json()

def get_recent_leads(token, dealer_key, since_minutes=40000, page=1, page_size=100):
    since_iso = (datetime.utcnow() - timedelta(minutes=since_minutes)).replace(microsecond=0).isoformat() + "Z"
    url = f"{BASE_URL}{LEADS_BASE}/search-delta"   # ← CRM op name uses a hyphen
    params = {"since": since_iso, "page": page, "pageSize": page_size}
    resp = _request("GET", url, headers=_headers(dealer_key, token), params=params)
    resp.raise_for_status()
    return resp.json()


def get_customer_by_url(url, token, dealer_key):
    resp = requests.get(url, headers=_headers(dealer_key, token))
    resp.raise_for_status()
    return resp.json()


def get_opportunity(opportunity_id, token, dealer_key):
    # FIX: OPPS_BASE already ends with '/opportunities'
    url = f"{BASE_URL}{OPPS_BASE}/{opportunity_id}"
    resp = requests.get(url, headers=_headers(dealer_key, token))
    resp.raise_for_status()
    return resp.json()
