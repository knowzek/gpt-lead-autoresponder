# fortellis.py
import os
import uuid
import requests
from datetime import datetime, timezone, timedelta
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


def _headers(id_or_key: str, token: str, extra: dict | None = None) -> dict:
    """
    Accepts either:
      - a dealer_key present in SUB_MAP  -> resolves to Subscription-Id, or
      - a raw Subscription-Id string     -> used as-is
    """
    sub_id = SUB_MAP.get(id_or_key) or id_or_key  # if not a key, treat as Subscription-Id

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

def post_and_wrap(method, url, *, headers, params=None, json=None, allow_404=False):
    resp = _request(method, url, headers=headers, params=params, json=json, allow_404=allow_404)
    try:
        body = resp.json() if resp.text else None
    except ValueError:
        body = None
    result = {"status": resp.status_code, "requestId": getattr(resp, "request_id", None)}
    if isinstance(body, dict):
        result.update(body)
    return result



def _request(method, url, headers=None, params=None, json=None, allow_404=False):
    t0 = time.time()
    req_id = (headers or {}).get("Request-Id")
    try:
        resp = requests.request(method, url, headers=headers, json=json, params=params, timeout=30)
        dt = int((time.time() - t0) * 1000)
        status = resp.status_code

        # allow 404 without warning/exception (e.g., empty searchDelta window)
        if status == 404 and allow_404:
            _log_txn_compact(
                logging.INFO,
                method=method, url=url, headers=_mask_headers(headers),
                status=status, duration_ms=dt, request_id=req_id,
                note="empty-delta (404 allowed)"
            )
            resp.request_id = req_id
            return resp

        if 200 <= status < 300:
            _log_txn_compact(
                logging.INFO,
                method=method, url=url, headers=_mask_headers(headers),
                status=status, duration_ms=dt, request_id=req_id,
                note=None
            )
            resp.request_id = req_id
            return resp

        # non-2xx and not an allowed 404 → warn + raise
        preview = (resp.text or "")[:400].replace("\n", " ")
        _log_txn_compact(
            logging.WARNING,  # (use WARNING not WARN)
            method=method, url=url, headers=_mask_headers(headers),
            status=status, duration_ms=dt, request_id=req_id,
            note=f"non-2xx body_preview={preview}"
        )
        resp.raise_for_status()

    except requests.RequestException as e:
        dt = int((time.time() - t0) * 1000)
        note = f"exception={type(e).__name__} msg={str(e)[:200].replace(chr(10),' ')}"
        _log_txn_compact(
            logging.ERROR,
            method=method, url=url, headers=_mask_headers(headers or {}),
            status=getattr(getattr(e, 'response', None), 'status_code', 'ERR'),
            duration_ms=dt, request_id=req_id, note=note
        )
        raise


from datetime import datetime, timezone, timedelta

def _since_iso(minutes: int | None = 30) -> str:
    dt = datetime.now(timezone.utc) - timedelta(minutes=minutes or 30)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

# make sure at top of fortellis.py:
# import requests

def get_recent_opportunities(token, dealer_key, since_minutes=360, page=1, page_size=100):
    url = f"{BASE_URL}{OPPS_BASE}/searchDelta"  # OPPS_BASE == "/sales/v2/elead/opportunities"
    params = {
        "dateFrom": _since_iso(since_minutes),  # NOTE: camelCase
        "page": page,
        "pageSize": page_size,
    }

    # allow_404=True: _request will return the response without warning/raising on 404
    resp = _request("GET", url, headers=_headers(dealer_key, token), params=params, allow_404=True)

    # Fortellis uses 404 for “no opportunities in this window”
    if resp.status_code == 404:
        return {"items": [], "totalItems": 0, "searchDate": params["dateFrom"]}

    # 204 or empty body → treat as empty window
    if resp.status_code == 204 or not (resp.content and resp.text.strip()):
        return {"items": [], "totalItems": 0, "searchDate": params["dateFrom"]}

    # For other non-2xx, raise now
    resp.raise_for_status()

    # Parse & normalize
    try:
        data = resp.json() or {}
    except ValueError:
        data = {}

    items = data.get("items") or []
    total = data.get("totalItems", len(items))
    search_date = data.get("searchDate", params["dateFrom"])

    return {"items": items, "totalItems": total, "searchDate": search_date}



def send_opportunity_email_activity(token,
                                    dealer_key,
                                    opportunity_id,
                                    sender,
                                    recipients,
                                    carbon_copies,
                                    subject,
                                    body_html,
                                    rooftop_name: str = None):
    """
    Send an email via Opportunities POST /sendEmail.

    Notes:
      - We don't hardcode 'from'. Caller must pass a rooftop-specific sender (or alias).
      - If rooftop_name is provided, we (a) scrub any 'Patterson Auto Group' and
        (b) ensure the subject mentions the rooftop.
    """
    url = f"{BASE_URL}{OPPS_BASE}/sendEmail"

    # Normalize lists
    recipients = recipients if isinstance(recipients, list) else ([recipients] if recipients else [])
    carbon_copies = carbon_copies or []
    if not isinstance(carbon_copies, list):
        carbon_copies = [carbon_copies]

    # Basic sanity warnings (after normalization)
    if not sender:
        log.warning("sendEmail: empty sender for dealer_key=%s", dealer_key)
    if not recipients:
        log.warning("sendEmail: empty recipients for dealer_key=%s", dealer_key)

    # Rooftop-aware subject/body cleanup (no hardcoding of sender)
    subj = subject or ""
    body = body_html or ""

    if rooftop_name:
        # Replace any legacy branding
        subj = subj.replace("Patterson Auto Group", rooftop_name)
        body = body.replace("Patterson Auto Group", rooftop_name)

        # Ensure subject contains the rooftop name (idempotent)
        if rooftop_name not in subj:
            subj = f"{subj} | {rooftop_name}" if subj else f"Your vehicle inquiry with {rooftop_name}"

    payload = {
        "opportunityId": opportunity_id,
        "message": {
            "from": sender,                  # caller supplies per-rooftop/alias
            "recipients": recipients,
            "carbonCopies": carbon_copies,
            "subject": subj,
            "body": body,
            "isHtml": True
        }
    }

    return post_and_wrap("POST", url, headers=_headers(dealer_key, token), json=payload)




def add_opportunity_comment(token, dealer_key, opportunity_id, comment_text):
    url = f"{BASE_URL}{OPPS_BASE}/comment"
    payload = {
        "opportunityId": opportunity_id,
        "comment": comment_text
    }
    return post_and_wrap("POST", url, headers=_headers(dealer_key, token), json=payload)


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
    return post_and_wrap("POST", url, headers=_headers(dealer_key, token), json=payload)


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
    return post_and_wrap("POST", url, headers=_headers(dealer_key, token), json=payload)


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

    return post_and_wrap("POST", url, headers=_headers(dealer_key, token), json=payload)


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
