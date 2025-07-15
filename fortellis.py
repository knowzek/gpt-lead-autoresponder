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

def get_opportunity_activities(opportunity_id, token):
    url = f"{BASE_URL}/sales/v2/elead/opportunities/{opportunity_id}/activities"
    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": SUBSCRIPTION_ID,
        "Request-Id": str(uuid.uuid4()),
        "Accept": "application/json"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get("items", [])


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
