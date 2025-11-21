# fortellis.py
import os
import uuid
import requests
from datetime import datetime, timezone, timedelta
import json
import time
import logging

from dotenv import load_dotenv
load_dotenv()

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

import uuid

def _headers_get(subscription_id: str, token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",       # ensure token is raw (no 'Bearer ' prefix in var)
        "Subscription-Id": subscription_id,
        "Accept": "application/json",
        "Request-Id": str(uuid.uuid4()),
    }

def _clean_token(tok: str) -> str:
    tok = (tok or "").strip()
    if tok.lower().startswith("bearer "):
        tok = tok.split(None, 1)[1]
    return tok

def _headers_post(subscription_id: str, token: str) -> dict:
    t = _clean_token(token)
    return {
        "Authorization": f"Bearer {t}",
        "Subscription-Id": subscription_id,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Request-Id": str(uuid.uuid4()),
    }

BASE_URL = os.getenv("FORTELLIS_BASE_URL", "https://api.fortellis.io")  # prod default
LEADS_BASE = "/cdk/sales/elead/v1/leads"
OPPS_BASE        = "/sales/v2/elead/opportunities"   
ACTIVITIES_BASE  = "/sales/v1/elead/activities" 
ACTIVITIES_SEARCH = "/cdk/sales/elead/v1/activity-history/search"
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

def set_customer_do_not_email(token, subscription_id, customer_id, email_address, do_not=True):
    """
    Marks the given customer email as DoNotEmail=True via Fortellis Customers API.
    """
    import requests
    from common import get_fortellis_headers

    url = f"https://api.fortellis.io/sales/v2/elead/customers/{customer_id}"
    headers = get_fortellis_headers(token, subscription_id)

    # Fetch full customer record first (so we can preserve other emails)
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    cust = resp.json()

    emails = cust.get("emails") or []
    for e in emails:
        if e.get("address", "").lower() == email_address.lower():
            e["doNotEmail"] = do_not
            break
    else:
        # If we didn't find it, append a new email entry
        emails.append({
            "address": email_address,
            "emailType": "Personal",
            "doNotEmail": do_not,
            "isPreferred": True
        })

    cust["emails"] = emails

    patch_resp = requests.put(url, headers=headers, json=cust)
    log_msg = f"set_customer_do_not_email({email_address}) status={patch_resp.status_code}"
    print(log_msg)
    return patch_resp


def set_opportunity_inactive(token: str, subscription_id: str, opportunity_id: str,
                             sub_status: str = "Not In Market", comments: str = "Marked inactive by Patti"):
    """
    Mark an eLead opportunity as inactive using Fortellis Sales V2 API.
    sub_status examples: "Not In Market", "Purchased Elsewhere", etc.
    """
    url = f"https://api.fortellis.io/sales/v2/elead/opportunities/{opportunity_id}/set-inactive"
    payload = {
        "opportunityId": opportunity_id,
        "inactiveSubStatus": sub_status,
        "comments": comments,
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": subscription_id,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    try:
        t0 = datetime.now(timezone.utc)
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        dur_ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
        _log_txn_compact(
            logging.INFO,
            method="POST", url=url, headers=headers,
            status=resp.status_code, duration_ms=dur_ms,
            request_id=headers.get("Request-Id", "auto"),
            note="set inactive"
        )
        if resp.status_code not in (200, 204):
            log.warning("set_opportunity_inactive failed: %s %s", resp.status_code, resp.text)
        return resp
    except Exception as e:
        log.warning("Fortellis set_opportunity_inactive error: %s", e)
        return None

def set_opportunity_substatus(token: str, subscription_id: str, opportunity_id: str,
                              sub_status: str = "Appointment Set"):
    """
    Update eLead opportunity subStatus (e.g., 'Appointment Set').
    POST /sales/v2/elead/opportunities/{opportunityId}/subStatus/update
    """
    url = f"https://api.fortellis.io/sales/v2/elead/opportunities/{opportunity_id}/subStatus/update"
    payload = {"subStatus": sub_status}
    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": subscription_id,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    t0 = datetime.now(timezone.utc)
    resp = requests.post(url, headers=headers, json=payload, timeout=10)
    dur_ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
    _log_txn_compact(logging.INFO, method="POST", url=url, headers=headers,
                     status=resp.status_code, duration_ms=dur_ms,
                     request_id=headers.get("Request-Id", "auto"),
                     note=f"subStatus→{sub_status}")
    if resp.status_code not in (200, 204):
        log.warning("set_opportunity_substatus failed: %s %s", resp.status_code, getattr(resp, "text", ""))
    return resp


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

def get_activity_history_v1(token, subscription_id, opportunity_id, customer_id, page=1, size=100):
    url = "https://api.fortellis.io/cdk/sales/elead/v1/activity-history/search"
    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": subscription_id,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    params = {
        "opportunityId": opportunity_id,
        "customerId": customer_id,
        "pageNumber": page,
        "pageSize": size,
    }
    t0 = datetime.now(timezone.utc)
    resp = requests.get(url, headers=headers, params=params, timeout=10)
    dur_ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
    _log_txn_compact(logging.INFO, method="ActivityHistory GET", url=url, headers=headers,
                     status=resp.status_code, duration_ms=dur_ms, request_id=headers.get("Request-Id", "auto"))
    resp.raise_for_status()
    return resp.json()


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



from typing import Optional

from typing import Optional

def send_opportunity_email_activity(token, subscription_id, opportunity_id,
                                    *, sender, recipients, carbon_copies,
                                    subject, body_html, rooftop_name,
                                    reply_to_activity_id: Optional[str] = None):
    """
    Send an email via Opportunities POST /sendEmail (Sales v2).

    Notes:
      - Caller must pass a rooftop-specific sender (or alias).
      - If rooftop_name is provided, we (a) scrub any 'Patterson Auto Group' and
        (b) ensure the subject mentions the rooftop.
    """
    url = f"{BASE_URL}{OPPS_BASE}/sendEmail"
    dealer_key = subscription_id

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

    # ✅ Correct v2 /sendEmail payload using "message"
    payload = {
        "opportunityId": opportunity_id,
        "message": {
            "from": sender,                  # caller supplies per-rooftop/alias
            "recipients": recipients,
            "carbonCopies": carbon_copies,
            "subject": subj,
            "body": body,
            "isHtml": True,
        },
    }

    # Optional threading for replies
    if reply_to_activity_id:
        payload["message"]["replyToActivityId"] = reply_to_activity_id

    try:
        log.info(
            "Fortellis: sending NEW outbound (reply_to_activity_id=%s) dealer_key=%s opp=%s sender=%s recipients=%s",
            reply_to_activity_id,
            dealer_key,
            opportunity_id,
            sender,
            recipients,
        )
        return post_and_wrap(
            "POST",
            url,
            headers=_headers(dealer_key, token),
            json=payload,
        )
    except requests.HTTPError as e:
        # Log a compact view of what we tried to send so we can debug 4xx/5xx
        try:
            payload_preview = json.dumps(
                {
                    "opportunityId": opportunity_id,
                    "dealer_key": dealer_key,
                    "sender": sender,
                    "recipients": recipients,
                    "subject": subj,
                    "body_len": len(body or ""),
                }
            )[:500]
        except Exception:
            payload_preview = "unable-to-serialize-payload"

        log.warning(
            "Fortellis: sendEmail failed dealer_key=%s opp=%s: %s payload_preview=%s",
            dealer_key,
            opportunity_id,
            repr(e),
            payload_preview,
        )
        # Re-raise so the caller's try/except can handle it (sent_ok=False)
        raise




def add_opportunity_comment(token: str, subscription_id: str, opportunity_id: str, comment_html: str):
    """
    POST /sales/v2/elead/opportunities/comment
    Auto-refreshes token once on 401 and logs correlation id on failure.
    """
    url = f"{BASE_URL}/sales/v2/elead/opportunities/comment"
    body = {
        "opportunityId": opportunity_id,
        "comment": comment_html
    }

    # 1st attempt
    resp = requests.post(url, headers=_headers_post(subscription_id, token), json=body, timeout=30)

    # Retry once on 401 with a fresh token for this sub-id
    if resp.status_code == 401:
        try:
            from fortellis import get_token
            try:
                fresh = get_token(subscription_id, force_refresh=True)
            except TypeError:
                fresh = get_token(subscription_id)
            resp = requests.post(url, headers=_headers_post(subscription_id, fresh), json=body, timeout=30)
        except Exception as e:
            log.error("Token refresh failed (comment) for sub %s: %s", subscription_id, e)

    if resp.status_code >= 400:
        corr = resp.headers.get("x-correlation-id")
        log.error(
            "POST %s status=%s corr=%s sub=%s body_preview=%s",
            url, resp.status_code, corr, subscription_id, (resp.text or "")[:200]
        )
    resp.raise_for_status()
    return resp.json() if resp.text else {}


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
    or a small set of known labels -> codes.

    Standardized for all rooftops:
      - "Send Email" -> 3
    """
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)

    label = (value or "").strip()

    BUILTIN = {
        "Send Email": 3,   # <-- standardize on 3
        "Task": 3,
        "Call": 1,
        "Appointment": 2,
        "Note": 37
        # note: we intentionally drop "Send Email/Letter"
    }
    if label in BUILTIN:
        return BUILTIN[label]

    raise ValueError(
        f"Unrecognized activityType: {value!r}. "
        "Use a numeric code or one of the supported labels: "
        f"{', '.join(BUILTIN.keys())}"
    )


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
        "activityType": "Appointment",
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


def search_activities_by_opportunity(opportunity_id, token, dealer_key, page=1, page_size=10, customer_id=None):
    if not opportunity_id:
        raise ValueError("opportunity_id is required")

    url = f"{BASE_URL}{ACTIVITIES_SEARCH}"  # "/cdk/sales/elead/v1/activity-history/search"
    params = {
        "opportunityId": opportunity_id,
        "pageNumber": page,
        "pageSize": page_size,
    }
    if customer_id:
        params["customerId"] = customer_id

    # First attempt (GET; no Content-Type)
    resp = requests.get(url, headers=_headers_get(dealer_key, token), params=params, timeout=30)

    # If the token is stale or wrong for this rooftop, refresh once and retry
    if resp.status_code == 401:
        try:
            from fortellis import get_token
            try:
                fresh = get_token(dealer_key, force_refresh=True)
            except TypeError:
                fresh = get_token(dealer_key)
            resp = requests.get(url, headers=_headers_get(dealer_key, fresh), params=params, timeout=30)
        except Exception as e:
            # don't hide the original 401; just log it if you have a logger
            pass

    # Helpful diagnostics on failure (corr id mirrors Postman)
    if resp.status_code >= 400:
        corr = resp.headers.get("x-correlation-id")
        try:
            log.error("ActivityHistory search failed: %s corr=%s body=%s",
                      resp.status_code, corr, (resp.text or "")[:400])
        except Exception:
            pass
    resp.raise_for_status()

    data = resp.json()
    # Some tenants return a flat object with arrays; others wrap items—be tolerant:
    return data.get("items") or data.get("activities") or data.get("completedActivities") or []



def get_activities(opportunity_id, customer_id, token, dealer_key):
    url = f"{BASE_URL}{ACTIVITIES_SEARCH}"
    params = {
        "opportunityId": opportunity_id,
        "customerId":   customer_id,   # your tenant requires this
        "pageNumber":   1,
        "pageSize":     100,
    }

    # TEMP debug
    try:
        log.info("ActivityHistory GET %s params=%s", url, params)
    except Exception:
        pass

    resp = requests.get(url, headers=_headers(dealer_key, token), params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()




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

import requests

def get_vehicle_inventory_xml(username: str, password: str, enterprise_code: str, company_number: str) -> str:
    """
    Calls the OpenTrack VehicleInventory endpoint and returns raw XML text.
    """
    url = "https://otstaging.arkona.com/vehicleapi.asmx"
    soap = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                   xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
                   xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
                   xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
      <soap:Header>
        <wsse:Security>
          <wsu:Timestamp wsu:Id="TS-1">
            <wsu:Created>2025-10-07T19:25:00Z</wsu:Created>
            <wsu:Expires>2025-10-07T19:30:00Z</wsu:Expires>
          </wsu:Timestamp>
          <wsse:UsernameToken wsu:Id="UT-1">
            <wsse:Username>{username}</wsse:Username>
            <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{password}</wsse:Password>
          </wsse:UsernameToken>
        </wsse:Security>
      </soap:Header>
      <soap:Body>
        <VehicleInventory xmlns="opentrack.dealertrack.com">
          <Dealer>
            <CompanyNumber>{company_number}</CompanyNumber>
            <EnterpriseCode>{enterprise_code}</EnterpriseCode>
          </Dealer>
        </VehicleInventory>
      </soap:Body>
    </soap:Envelope>"""

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "opentrack.dealertrack.com/VehicleInventory",
    }
    resp = requests.post(url, data=soap.encode("utf-8"), headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.text

