# fortellis.py
import os
import uuid
import requests
from datetime import datetime, timedelta

BASE_URL = "https://api.fortellis.io/cdk-test"
TOKEN_URL = "https://identity.fortellis.io/oauth2/aus1p1ixy7YL8cMq02p7/v1/token"

CLIENT_ID = os.getenv("FORTELLIS_CLIENT_ID")
CLIENT_SECRET = os.getenv("FORTELLIS_CLIENT_SECRET")
SUBSCRIPTION_ID = os.getenv("FORTELLIS_SUBSCRIPTION_ID")

# ── fortellis.py ──

BASE_URL = "https://api.fortellis.io/cdk-test"  # your test‐env base

def send_opportunity_email_activity(token, subscription_id,
                                    opportunity_id, sender,
                                    recipients, carbon_copies,
                                    subject, body_html):
    """
    Logs an email in the CRM by POST /sales/v2/elead/opportunities/sendEmail
    """
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
    resp = requests.post(url, json=payload, headers=headers)
    if resp.status_code != 200:
        print("❌ sendEmail payload:", json.dumps(payload, indent=2))
        print("❌ sendEmail response:", resp.status_code, resp.text)
    resp.raise_for_status()

    return resp.json()  # e.g. { "activityId": "..." }


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
    since = (datetime.utcnow() - timedelta(days=6, hours=20)).isoformat() + "Z"
    url = f"{BASE_URL}/sales/elead/v1/leads/search-delta?since={since}&page=1&pageSize=100"
    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": SUBSCRIPTION_ID,
        "Request-Id": str(uuid.uuid4()),
        "Accept": "application/json"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get("items", [])

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
