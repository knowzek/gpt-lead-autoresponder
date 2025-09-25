# fortellis.py
import os
import uuid
import requests
from datetime import datetime, timedelta
import json
import time
import logging

# --- simple JSON logger (stdout) ---
def _mask_headers(h):
    h = dict(h or {})
    if "Authorization" in h:
        h["Authorization"] = "Bearer ***redacted***"
    return h

def _log_txn(method, url, headers, req_body, status, resp_body, duration_ms):
    import json
    print(json.dumps({
        "kind": "fortellis_transaction",
        "method": method,
        "url": url,
        "request_headers": _mask_headers(headers),
        "request_body": req_body,
        "status_code": status,
        "response_body": resp_body,
        "duration_ms": duration_ms
    }, ensure_ascii=False))


BASE_URL = "https://api.fortellis.io/sales"
TOKEN_URL = "https://identity.fortellis.io/oauth2/aus1p1ixy7YL8cMq02p7/v1/token"

CLIENT_ID = os.getenv("FORTELLIS_CLIENT_ID")
CLIENT_SECRET = os.getenv("FORTELLIS_CLIENT_SECRET")
SUBSCRIPTION_ID = os.getenv("FORTELLIS_SUBSCRIPTION_ID")

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
    resp = requests.request(method, url, headers=headers, json=json_body, params=params)
    dt = int((time.time() - t0) * 1000)
    try:
        body = resp.json()
    except Exception:
        body = resp.text
    _log_txn(method, url, headers, json_body, resp.status_code, body, dt)
    resp.raise_for_status()
    # Attach request id for downstream logging
    req_id = headers.get("Request-Id") if headers else None
    resp.request_id = req_id
    return resp

def send_opportunity_email_activity(token, subscription_id,
                                    opportunity_id, sender,
                                    recipients, carbon_copies,
                                    subject, body_html):
    url = f"{BASE_URL}/sales/v2/elead/opportunities/sendEmail"
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
    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": subscription_id,
        "Request-Id": str(uuid.uuid4()),
        "Accept": "application/json"
    }
    return post_and_wrap("POST", url, headers=headers, json_body=payload)

def add_opportunity_comment(token, subscription_id, opportunity_id, comment_text):
    url = f"{BASE_URL}/sales/v2/elead/opportunities/comment"
    payload = {
        "opportunityId": opportunity_id,
        "comment": comment_text
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": subscription_id,
        "Request-Id": str(uuid.uuid4()),
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    return post_and_wrap("POST", url, headers=headers, json_body=payload)

def add_vehicle_sought(token, subscription_id, opportunity_id,
                       is_new=True, year_from=None, year_to=None,
                       make="", model="", trim="", stock_number="", is_primary=True):
    url = f"{BASE_URL}/sales/v2/elead/opportunities/vehicleSought"
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
    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": subscription_id,
        "Request-Id": str(uuid.uuid4()),
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    return post_and_wrap("POST", url, headers=headers, json_body=payload)

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
    subscription_id,
    opportunity_id,
    *,
    due_dt_iso_utc,
    activity_name,
    activity_type,
    comments=""
):
    """
    CDK CRM Activities v1 — Schedule activity
    Endpoint: /sales/v1/elead/activities/schedule

    Required payload:
    {
        "opportunityId": "...",
        "dueDate": "2025-09-13T10:39:51.402Z",
        "activityName": "Send Email/Letter",
        "activityType": 14,
        "comments": "Comments go here"
    }
    """
    url = f"{BASE_URL}/sales/v1/elead/activities/schedule"
    payload = {
        "opportunityId": opportunity_id,
        "dueDate": due_dt_iso_utc,                   # e.g., '2025-09-13T10:39:51.402Z'
        "activityName": activity_name,               # e.g., 'Send Email/Letter'
        "activityType": _coerce_activity_type(activity_type),  # numeric or mapped
        "comments": comments or ""
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": subscription_id,
        "Request-Id": str(uuid.uuid4()),
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    return post_and_wrap("POST", url, headers=headers, json_body=payload)


def complete_activity(
    token,
    subscription_id,
    opportunity_id,
    *,
    due_dt_iso_utc,
    completed_dt_iso_utc,
    activity_name,
    activity_type,
    comments="",
    activity_id=None,
):
    """
    CDK CRM Activities v1 — Complete activity
    Endpoint: /sales/v1/elead/activities/complete

    Expected payload (per your Postman):
    {
        "opportunityId": "...",
        "dueDate": "2025-09-13T10:39:51.402Z",
        "completedDate": "2025-09-11T10:39:51.402Z",
        "activityName": "Send Email/Letter",
        "activityType": 14,
        "comments": "Comments go here",
        # Some tenants may also accept/require activityId:
        # "activityId": "..."
    }
    """
    url = f"{BASE_URL}/sales/v1/elead/activities/complete"
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

    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": subscription_id,
        "Request-Id": str(uuid.uuid4()),
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    return post_and_wrap("POST", url, headers=headers, json_body=payload)


def search_activities_by_opportunity(opportunity_id, token):
    url = f"{BASE_URL}/sales/elead/v1/activities/search"
    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": SUBSCRIPTION_ID,
        "Request-Id": str(uuid.uuid4()),
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    payload = {
        "filters": [
            {
                "field": "opportunityId",
                "operator": "eq",
                "value": opportunity_id
            }
        ],
        "sort": [{"field": "createdDate", "direction": "desc"}],
        "page": 1,
        "pageSize": 10
    }
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json().get("items", [])


def get_activity_by_url(url, token):
    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": SUBSCRIPTION_ID,
        "Request-Id": str(uuid.uuid4()),
        "Accept": "application/json"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def get_activity_by_id_v1(activity_id, token):
    url = f"{BASE_URL}/sales/v1/elead/activities/{activity_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": SUBSCRIPTION_ID,
        "Request-Id": str(uuid.uuid4()),
        "Accept": "application/json"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def get_token():
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Subscription-Id": SUBSCRIPTION_ID
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "anonymous"
    }
    response = requests.post(TOKEN_URL, headers=headers, data=data)
    response.raise_for_status()
    return response.json()["access_token"]


def get_recent_leads(token, since_minutes=10):
    # go back ~1 week (6 days + 20 hours) and format as ISO 8601 UTC with "Z"
    since = (datetime.utcnow() - timedelta(days=6, hours=20)) \
        .isoformat() + "Z"

    url = f"{BASE_URL}/sales/elead/v1/leads/search-delta?since={since}&page=1&pageSize=100"
    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": SUBSCRIPTION_ID,
        "Request-Id": str(uuid.uuid4()),
        "Accept": "application/json"
    }
    resp = _request("GET", url, headers=headers)
    return resp.json().get("items", [])

def get_customer_by_url(url, token):
    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": SUBSCRIPTION_ID,
        "Request-Id": str(uuid.uuid4()),
        "Accept": "application/json"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def get_opportunity(opportunity_id, token):
    url = f"{BASE_URL}/sales/v2/elead/opportunities/{opportunity_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": SUBSCRIPTION_ID,
        "Request-Id": str(uuid.uuid4()),
        "Accept": "application/json"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()
