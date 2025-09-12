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


BASE_URL = "https://api.fortellis.io/cdk-test"
TOKEN_URL = "https://identity.fortellis.io/oauth2/aus1p1ixy7YL8cMq02p7/v1/token"

CLIENT_ID = os.getenv("FORTELLIS_CLIENT_ID")
CLIENT_SECRET = os.getenv("FORTELLIS_CLIENT_SECRET")
SUBSCRIPTION_ID = os.getenv("FORTELLIS_SUBSCRIPTION_ID")


BASE_URL = "https://api.fortellis.io/cdk-test"  # your test‐env base

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
    resp = _request("POST", url, headers=headers, json_body=payload)
    return resp.json()

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
    resp = _request("POST", url, headers=headers, json_body=payload)
    # This endpoint commonly returns 204 No Content; normalize to dict for logging bundle
    return {"status": resp.status_code}

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
    resp = _request("POST", url, headers=headers, json_body=payload)
    return resp.json()

def schedule_activity(token, subscription_id, opportunity_id,
                      subject, notes, due_dt_iso_utc, activity_type="Call"):
    # Note: v1 schedule endpoint
    url = f"{BASE_URL}/sales/v1/elead/activities/schedule"
    payload = {
        "opportunityId": opportunity_id,
        "subject": subject,
        "notes": notes,
        "dueDate": due_dt_iso_utc,   # ISO UTC, e.g. '2025-09-12T20:33:00Z'
        "activityType": activity_type
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": subscription_id,
        "Request-Id": str(uuid.uuid4()),
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    resp = _request("POST", url, headers=headers, json_body=payload)
    try:
        return resp.json()
    except Exception:
        return {"status": resp.status_code}

def complete_activity(token, subscription_id, activity_id):
    # Note: v1 complete endpoint
    url = f"{BASE_URL}/sales/v1/elead/activities/complete"
    payload = { "activityId": activity_id }
    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": subscription_id,
        "Request-Id": str(uuid.uuid4()),
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    resp = _request("POST", url, headers=headers, json_body=payload)
    try:
        return resp.json()
    except Exception:
        return {"status": resp.status_code}

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
