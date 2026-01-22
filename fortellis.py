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

ACTIVITY_TYPE_MAP = {
    "attend meeting": 1,
    "phone call": 2,
    "send email": 3,
    "other": 5,
    "appointment": 7,
    "send letter": 12,
    "send email/letter": 14,
    "inbound email": 20,            # This is what CDK calls “Read Email”
    "leadlink up": 21,
    "service reminder": 27,
    "lease reminder": 28,
    "confirm appointment": 34,
    "auto response": 36,
    "note": 37,
    "inbound call": 38,
    "duplicate lead": 40,
    "brochure": 41,
    "parts up": 42,
    "service appt up": 43,
    "miscellaneous sold": 44,
    "directcall": 45,
    "ivr": 46,
    "directmail": 47,
    "text message": 48,
    "service appointment": 49,
    "delivery appointment": 50,
    "miscellaneous appointment": 51,
    "credit application": 53,
    "appraisal integration": 54,
    "birthday/anniv survey": 56,
    "ace email": 57,
    "alert sent": 59,
    "data enrichment": 60,
    "in market notification": 63,
    "appraisal appointment": 65,
    "service plus survey": 66,
    "livestream": 67,
    "credit soft pull": 68,
    "bulk text message": 69,

    # Also add missing ones from CDK Activity History:
    "internet up": 13,              
}

BASE_URL = "https://api.fortellis.io"

def _headers(subscription_id: str, token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": subscription_id,
        "Request-Id": str(uuid.uuid4()),
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

def search_customers_by_email(email: str, token: str, subscription_id: str, page_size: int = 10) -> list[dict]:
    url = f"{BASE_URL}/sales/v1/elead/customers/search"
    payload = {"emailAddress": (email or "").strip()}
    params = {"page": 1, "pageSize": page_size}
    r = requests.post(url, headers=_headers(subscription_id, token), params=params, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json() or {}
    return data.get("items") or []

def get_opps_by_customer_id(customer_id: str, token: str, subscription_id: str, page_size: int = 50) -> list[dict]:
    url = f"{BASE_URL}/sales/v2/elead/opportunities/search-by-customerId/{customer_id}"
    params = {"page": 1, "pageSize": page_size}
    r = requests.get(url, headers=_headers(subscription_id, token), params=params, timeout=30)

    if r.status_code == 404:
        log.warning("search-by-customerId not found (404). sub=%s customer_id=%s", subscription_id, customer_id)
        return []

    r.raise_for_status()
    data = r.json() or {}
    return data.get("items") or []


def _parse_dt(s: str | None) -> datetime:
    if not s:
        return datetime.min.replace(tzinfo=timezone.utc)
    # Fortellis often returns "...Z"
    s2 = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s2)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)

def find_best_kbb_opp_for_email(
    *,
    shopper_email: str,
    token: str,
    subscription_id: str,
    kbb_sources: set[str] | None = None,
) -> tuple[str | None, str | None, str | None]:
    """
    Returns (opp_id, customer_id, reason).
    Chooses most recent Active KBB opp for this email within this subscription.
    """
    kbb_sources = kbb_sources or {"kbb instant cash offer", "kbb servicedrive"}

    customers = search_customers_by_email(shopper_email, token, subscription_id, page_size=10)
    if not customers:
        return None, None, "no_customers"

    candidates: list[tuple[datetime, dict, str]] = []
    for c in customers:
        cid = c.get("id")
        if not cid:
            continue
        try:
            opps = get_opps_by_customer_id(cid, token, subscription_id, page_size=100)
        except Exception:
            continue

        for o in opps or []:
            src = (o.get("source") or "").strip().lower()
            status = (o.get("status") or "").strip().lower()

            if src not in kbb_sources:
                continue
            if status != "active":
                continue

            # Most reliable “freshness” field here is usually dateIn (per example),
            # else fall back to created_at/updated_at if present.
            dt = _parse_dt(o.get("dateIn") or o.get("createdAt") or o.get("created_at") or o.get("updatedAt"))
            opp_id = o.get("id") or o.get("opportunityId")
            if opp_id:
                candidates.append((dt, o, cid))

    if not candidates:
        return None, None, "no_kbb_opps"

    # pick most recent
    candidates.sort(key=lambda t: t[0], reverse=True)
    best_dt, best_opp, best_cid = candidates[0]
    best_id = best_opp.get("id") or best_opp.get("opportunityId")
    return best_id, best_cid, f"picked_most_recent={best_dt.isoformat()}"


def normalize_activity_item(it: dict) -> dict:
    """
    Converts Sales v1 activityHistory items to Patti-safe format:
    - Always returns numeric activityType (or omits the field)
    - Avoids ES type errors
    """

    raw_type = it.get("activityType")
    mapped_type = None

    # Already numeric → good
    if isinstance(raw_type, int):
        mapped_type = raw_type
    # String → map using ACTIVITY_TYPE_MAP
    elif isinstance(raw_type, str):
        mapped_type = ACTIVITY_TYPE_MAP.get(raw_type.strip().lower())

    base = {
        "activityId": it.get("id"),
        "activityName": it.get("name"),
        "dueDate": it.get("dueDate"),
        "completedDate": it.get("completedDate"),
        "outcome": it.get("outcome"),
        "assignedTo": it.get("assignedTo"),
    }

    # Only include numeric activityType:
    if mapped_type is not None:
        base["activityType"] = mapped_type

    return base

# fortellis.py

def find_recent_kbb_opportunity_by_email(
    *,
    shopper_email: str,
    subscription_id: str,
    token: str,
    since_minutes: int = 60 * 48,   # last 48 hours
    page_size: int = 100,
    max_pages: int = 10,
):
    """
    Search recent opportunities in Fortellis for a KBB opp whose customer email matches shopper_email.
    Returns (opp_id, opp_obj_from_delta) or (None, None).
    """
    target = (shopper_email or "").strip().lower()
    if not target:
        return None, None

    page = 1
    while page <= max_pages:
        data = get_recent_opportunities(
            token,
            subscription_id,
            since_minutes=since_minutes,
            page=page,
            page_size=page_size,
        ) or {}

        items = (data.get("items") or [])
        if not items:
            return None, None

        for op in items:
            src = (op.get("source") or "").strip().lower()
            if src not in ("kbb instant cash offer", "kbb servicedrive"):
                continue

            cust = (op.get("customer") or {})
            emails = cust.get("emails") or []
            for e in emails:
                addr = (e.get("address") or "").strip().lower()
                if addr == target:
                    opp_id = op.get("opportunityId") or op.get("id")
                    return opp_id, op

            # fallback: some payloads have customerEmail
            addr2 = (op.get("customerEmail") or "").strip().lower()
            if addr2 == target:
                opp_id = op.get("opportunityId") or op.get("id")
                return opp_id, op

        page += 1

    return None, None


def get_activities(opportunity_id, customer_id, token, dealer_key):
    """
    Replace expensive CDK Activity History with Sales v1 activities.history/byOpportunityId.
    Returns:
        {
            "scheduledActivities": [...],
            "completedActivities": [...]
        }
    """

    url = f"{BASE_URL}/sales/v1/elead/activities/history/byOpportunityId/{opportunity_id}"
    headers = _headers(dealer_key, token)

    resp = requests.get(url, headers=headers, timeout=30)

    # Retry once on 401
    if resp.status_code == 401:
        fresh = get_token(dealer_key, force_refresh=True)
        resp = requests.get(url, headers=_headers(dealer_key, fresh), timeout=30)

    resp.raise_for_status()
    data = resp.json() or {}

    items = data.get("items", []) or []

    scheduled = []
    completed = []

    for it in items:
        norm = normalize_activity_item(it)

        category = (it.get("category") or "").lower()
        outcome = (it.get("outcome") or "").lower()

        if category == "scheduled" or outcome in ("open", "in progress"):
            scheduled.append(norm)
        else:
            completed.append(norm)

    return {
        "scheduledActivities": scheduled,
        "completedActivities": completed,
    }

def find_recent_opportunity_by_email(
    *,
    shopper_email: str,
    subscription_id: str,
    token: str,
    since_minutes: int = 60 * 24 * 14,  # 14 days
    page_size: int = 100,
    max_pages: int = 20,
):
    """
    General version of find_recent_kbb_opportunity_by_email:
    Searches recent opportunities via searchDelta and returns the most likely ACTIVE opp for shopper_email.
    Returns (opp_id, opp_obj) or (None, None).
    """
    target = (shopper_email or "").strip().lower()
    if not target:
        return None, None

    page = 1
    while page <= max_pages:
        data = get_recent_opportunities(
            token,
            subscription_id,
            since_minutes=since_minutes,
            page=page,
            page_size=page_size,
        ) or {}

        items = (data.get("items") or [])
        if not items:
            return None, None

        for op in items:
            # Prefer Active opps when that field exists
            status = (op.get("status") or "").strip().lower()
            if status and status != "active":
                continue

            cust = (op.get("customer") or {})
            emails = cust.get("emails") or []
            for e in emails:
                addr = (e.get("address") or "").strip().lower()
                if addr == target:
                    opp_id = op.get("opportunityId") or op.get("id")
                    return opp_id, op

            # fallback: some payloads have customerEmail
            addr2 = (op.get("customerEmail") or "").strip().lower()
            if addr2 == target:
                opp_id = op.get("opportunityId") or op.get("id")
                return opp_id, op

        page += 1

    return None, None


def set_customer_do_not_email(token, subscription_id, customer_id, email_address, do_not=True):
    """
    Marks the given customer email as DoNotEmail=True via Fortellis Customers API.
    Works with existing fortellis._headers() for auth.
    """

    import requests

    # Use our existing Fortellis header builder (no dependency on 'common')
    headers = _headers(subscription_id, token)

    url = f"https://api.fortellis.io/sales/v2/elead/customers/{customer_id}"

    # --- STEP 1: Fetch full customer record ---
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    cust = resp.json()

    # --- STEP 2: Update DoNotEmail flag appropriately ---
    emails = cust.get("emails") or []
    found = False

    for e in emails:
        if e.get("address", "").lower() == email_address.lower():
            e["doNotEmail"] = do_not
            found = True
            break

    if not found:
        emails.append({
            "address": email_address,
            "emailType": "Personal",
            "doNotEmail": do_not,
            "isPreferred": True
        })

    cust["emails"] = emails

    # --- STEP 3: PUT updated customer record back ---
    patch_resp = requests.put(url, headers=headers, json=cust, timeout=30)
    print(f"set_customer_do_not_email({email_address}) status={patch_resp.status_code}")

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

def complete_read_email_activity(
    *,
    token: str,
    subscription_id: str,
    opportunity_id: str,
    completed_dt_iso_utc: str,
    comments: str = "",
):
    # If caller passes a valid ISO already, keep it; otherwise they should pass Z.
    return complete_activity(
        token,
        subscription_id,
        opportunity_id,
        due_dt_iso_utc=completed_dt_iso_utc,
        completed_dt_iso_utc=completed_dt_iso_utc,
        activity_name="Read Email",
        activity_type=20,
        comments=comments,
    )


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
    """
    Legacy wrapper. Now mapped to Sales v1 activities history.
    """

    url = f"{BASE_URL}/sales/v1/elead/activities/history/byOpportunityId/{opportunity_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Subscription-Id": subscription_id,
        "Accept": "application/json"
    }

    resp = requests.get(url, headers=headers, timeout=30)
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
      - We don't hardcode 'from'. Caller must pass a rooftop-specific sender (or alias).
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

    # Build v2 /sendEmail payload using "message" envelope
    payload = {
        "opportunityId": opportunity_id,
        "message": {
            "from": sender,
            "recipients": recipients,
            "carbonCopies": carbon_copies,
            "subject": subj,
            "body": body,
            "isHtml": True,
        },
    }

    # Thread the reply if we have the inbound activity id
    if reply_to_activity_id:
        # Fortellis/eLead threading key; keep both top-level and nested for compatibility
        #payload["message"]["replyToActivityId"] = reply_to_activity_id
        payload["replyToActivityId"] = reply_to_activity_id
        payload["inReplyToActivityId"] = reply_to_activity_id

    if reply_to_activity_id:
        log.info(
            "Fortellis: sending REPLY in-thread to activity_id=%s dealer_key=%s opp=%s sender=%s recipients=%s",
            reply_to_activity_id,
            dealer_key,
            opportunity_id,
            sender,
            recipients,
        )
    else:
        log.info(
            "Fortellis: sending NEW outbound (no reply_to_activity_id) dealer_key=%s opp=%s sender=%s recipients=%s",
            dealer_key,
            opportunity_id,
            sender,
            recipients,
        )

    try:
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
    Accepts an int, a numeric string, or one of the known Elead
    activity names and returns the numeric activityType ID.

    Known names/IDs (from Elead for this group):
      1  -> Attend Meeting
      2  -> Phone Call
      3  -> Send Email
      5  -> Other
      7  -> Appointment
      12 -> Send Letter
      14 -> Send Email/Letter
      20 -> Inbound Email
      21 -> LeadLink Up
      27 -> Service Reminder
      28 -> Lease Reminder
      34 -> Confirm Appointment
      36 -> Auto Response
      37 -> Note
      38 -> Inbound Call
      40 -> Duplicate Lead
      41 -> Brochure
      42 -> Parts Up
      43 -> Service Appt Up
      44 -> Miscellaneous Sold
      45 -> DirectCall
      46 -> IVR
      47 -> DirectMail
      48 -> Text Message
      49 -> Service Appointment
      50 -> Delivery Appointment
      51 -> Miscellaneous Appointment
      53 -> Credit Application
      54 -> Appraisal Integration
      56 -> Birthday/Anniv Survey
      57 -> ACE Email
      59 -> Alert Sent
      60 -> Data Enrichment
      63 -> In Market Notification
      65 -> Appraisal Appointment
      66 -> Service Plus Survey
      67 -> Livestream
      68 -> Credit Soft Pull
      69 -> Bulk Text Message
    """

    # Already a numeric ID? Just return it.
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)

    label = (value or "").strip()

    # Map the names we care about to their proper numeric IDs.
    BUILTIN = {
        "Attend Meeting": 1,
        "Phone Call": 2,
        "Send Email": 3,
        "Other": 5,
        "Appointment": 7,
        "Send Letter": 12,
        "Send Email/Letter": 14,
        "Inbound Email": 20,
        "LeadLink Up": 21,
        "Service Reminder": 27,
        "Lease Reminder": 28,
        "Confirm Appointment": 34,
        "Auto Response": 36,
        "Note": 37,
        "Inbound Call": 38,
        "Duplicate Lead": 40,
        "Brochure": 41,
        "Parts Up": 42,
        "Service Appt Up": 43,
        "Miscellaneous Sold": 44,
        "DirectCall": 45,
        "IVR": 46,
        "DirectMail": 47,
        "Text Message": 48,
        "Service Appointment": 49,
        "Delivery Appointment": 50,
        "Miscellaneous Appointment": 51,
        "Credit Application": 53,
        "Appraisal Integration": 54,
        "Birthday/Anniv Survey": 56,
        "ACE Email": 57,
        "Alert Sent": 59,
        "Data Enrichment": 60,
        "In Market Notification": 63,
        "Appraisal Appointment": 65,
        "Service Plus Survey": 66,
        "Livestream": 67,
        "Credit Soft Pull": 68,
        "Bulk Text Message": 69,
    }

    if label in BUILTIN:
        return BUILTIN[label]

    raise ValueError(
        f"Unrecognized activityType: {value!r}. "
        "Use a numeric code or one of the supported labels."
    )



def schedule_activity(
    token,
    dealer_key,
    opportunity_id,
    *,
    due_dt_iso_utc,
    activity_name,
    activity_type,
    comments="",
):
    """
    Create a scheduled activity on an opportunity.

    `activity_type` can be:
      - an int (Elead code from /activity-types),
      - a numeric string, or
      - one of the known names (e.g. "Appointment", "Phone Call", "Send Email").
    """
    url = f"{BASE_URL}{ACTIVITIES_BASE}/schedule"
    payload = {
        "opportunityId": opportunity_id,
        "dueDate": due_dt_iso_utc,
        "activityName": activity_name,
        "activityType": _coerce_activity_type(activity_type),
        "comments": comments or "",
    }
    return post_and_wrap(
        "POST",
        url,
        headers=_headers(dealer_key, token),
        json=payload,
    )
  
def schedule_appointment_with_notify(
    token,
    dealer_key,
    opportunity_id,
    *,
    due_dt_iso_utc,
    activity_name,
    activity_type,
    comments="",
    # notify context
    opportunity=None,
    fresh_opp=None,
    rooftop_name="",
    appt_human="",
    customer_reply="",
):
    """
    Schedule an appointment activity and notify staff ONCE.

    - Mirrors schedule_activity() exactly for Fortellis
    - Sends staff notification only the first time per opportunity
    - Uses opportunity['patti']['appt_notify_sent'] as the idempotency guard
    """

    # --- 1) Schedule in Fortellis (authoritative) ---
    resp = schedule_activity(
        token,
        dealer_key,
        opportunity_id,
        due_dt_iso_utc=due_dt_iso_utc,
        activity_name=activity_name,
        activity_type=activity_type,
        comments=comments or "",
    )

    # --- 2) Notify staff ONCE (guarded) ---
    try:
        # Only notify for Appointment activities
        type_id = int(_coerce_activity_type(activity_type))
        if type_id != 7:
            return resp

        if not opportunity:
            return resp

        patti_meta = opportunity.get("patti") or {}
        if patti_meta.get("appt_notify_sent"):
            log.info(
                "APPT NOTIFY skipped (already sent) opp=%s",
                opportunity_id,
            )
            return resp

        from patti_triage import notify_staff_patti_scheduled_appt

        notify_staff_patti_scheduled_appt(
            opportunity=opportunity,
            fresh_opp=fresh_opp or {},
            subscription_id=dealer_key,
            rooftop_name=rooftop_name or "",
            appt_human=appt_human or due_dt_iso_utc,
            customer_reply=customer_reply or "",
        )

        # mark AFTER successful send attempt
        patti_meta["appt_notify_sent"] = True
        opportunity["patti"] = patti_meta

        log.info(
            "APPT NOTIFY sent opp=%s when=%s",
            opportunity_id,
            appt_human or due_dt_iso_utc,
        )

    except Exception as e:
        # Never block scheduling on notify failure
        log.warning(
            "APPT NOTIFY failed opp=%s: %s",
            opportunity_id,
            e,
        )

    return resp



from requests.exceptions import HTTPError

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
    """
    Complete an activity.
    Tries known-good (name,type) combos to avoid InvalidActivityType per-rooftop issues.
    """
    url = f"{BASE_URL}{ACTIVITIES_BASE}/complete"

    def _post(name: str, type_id: int):
        log.info("complete_activity payload: name=%s type=%s due=%s completed=%s activity_id=%s",
                 name, type_id, due_dt_iso_utc, completed_dt_iso_utc, activity_id)

        payload = {
            "opportunityId": opportunity_id,
            "dueDate": due_dt_iso_utc,
            "completedDate": completed_dt_iso_utc,
            "activityName": name,
            "activityType": type_id,
            "comments": comments or "",
        }
        if activity_id:
            payload["activityId"] = activity_id

        return post_and_wrap(
            "POST",
            url,
            headers=_headers(dealer_key, token),
            json=payload,
        )

    # Always try the caller's requested combo first, then known fallback.
    # If caller passes Send Email/3 already, this still works.
    combos = [
        (activity_name, _coerce_activity_type(activity_type)),
        ("Send Email", 3),
        ("Send Email/Letter", 14),
    ]

    # de-dupe while preserving order
    seen = set()
    ordered = []
    for n, t in combos:
        key = (n, int(t))
        if key not in seen:
            seen.add(key)
            ordered.append(key)

    last_err = None
    for name, type_id in ordered:
        try:
            resp = _post(name, type_id)
            log.info(
                "Completed CRM activity: %s (%s) dealer_key=%s opp=%s",
                name,
                type_id,
                dealer_key,
                opportunity_id,
            )
            return resp
        except HTTPError as e:
            # Identify InvalidActivityType
            try:
                err_json = e.response.json()
                err_code = err_json.get("code")
            except Exception:
                err_code = None
    
            msg = str(e)
            is_invalid_type = (err_code == "InvalidActivityType") or ("InvalidActivityType" in msg)
    
            if not is_invalid_type:
                raise  # not a type issue, bubble it up

            msg = str(e)
            is_invalid_type = (err_code == "InvalidActivityType") or ("InvalidActivityType" in msg)

            if not is_invalid_type:
                raise  # not a type issue, bubble it up

            last_err = e

    # If we exhausted combos, raise the last InvalidActivityType error
    raise last_err

from datetime import datetime, timezone

def _iso_z(dt: datetime) -> str:
    # Fortellis examples commonly accept Zulu timestamps
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def complete_send_email_activity(
    *,
    token: str,
    subscription_id: str,
    opportunity_id: str,
    to_addr: str,
    subject: str,
    body_html: str = "",
    comments_extra: str = "",
):
    now = _iso_z(datetime.now(timezone.utc))

    # ✅ strip footer
    body_no_footer = body_html or ""
    footer_marker = "<!-- PATTI_FOOTER_START -->"
    if footer_marker in body_no_footer:
        body_no_footer = body_no_footer.split(footer_marker, 1)[0]

    full_comments = (
        f"Patti Outlook: sent to {to_addr} | subject={subject}"
        + (f" | {comments_extra}" if comments_extra else "")
        + ("\n\n--- EMAIL BODY ---\n" + (body_no_footer or "") if (body_no_footer or "").strip() else "")
    )

    return complete_activity(
        token,
        subscription_id,
        opportunity_id,
        due_dt_iso_utc=now,
        completed_dt_iso_utc=now,
        activity_name="Send Email",
        activity_type=3,
        comments=full_comments,
    )


def search_activities_by_opportunity(opportunity_id, token, dealer_key, page=1, page_size=10, customer_id=None):
    """
    Search activities for an opportunity.

    NOTE:
    - This has been switched from the expensive CDK Activity History endpoint
      to the cheaper Sales v1 activities history endpoint:
        /sales/v1/elead/activities/history/byOpportunityId/{opportunity_id}
    - We preserve the original function's behavior as much as possible:
        * same arguments
        * 401 retry with fresh token
        * correlation-id logging on failure
        * returns a list of activities
        * simple paging via `page` and `page_size` (now done client-side)
    """

    if not opportunity_id:
        raise ValueError("opportunity_id is required")

    # New cheaper endpoint (no server-side paging or customerId filter)
    url = f"{BASE_URL}/sales/v1/elead/activities/history/byOpportunityId/{opportunity_id}"

    # First attempt
    resp = requests.get(url, headers=_headers_get(dealer_key, token), timeout=30)

    # If the token is stale or wrong for this rooftop, refresh once and retry
    if resp.status_code == 401:
        try:
            from fortellis import get_token
            try:
                fresh = get_token(dealer_key, force_refresh=True)
            except TypeError:
                fresh = get_token(dealer_key)
            resp = requests.get(url, headers=_headers_get(dealer_key, fresh), timeout=30)
        except Exception:
            # don't hide the original 401; just log it if you have a logger
            pass

    # Helpful diagnostics on failure (corr id mirrors Postman)
    if resp.status_code >= 400:
        corr = resp.headers.get("x-correlation-id")
        try:
            log.error(
                "ActivityHistory(search_activities_by_opportunity) failed: %s corr=%s body=%s",
                resp.status_code,
                corr,
                (resp.text or "")[:400],
            )
        except Exception:
            pass

    resp.raise_for_status()

    data = resp.json() or {}

    # Sales v1 returns a flat list under "items"
    items = data.get("items") or []

    # Emulate paging client-side so callers can keep using `page` and `page_size`
    try:
        page = int(page) if page is not None else 1
        page_size = int(page_size) if page_size is not None else 10
    except (TypeError, ValueError):
        page = 1
        page_size = 10

    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 10

    start = (page - 1) * page_size
    end = start + page_size

    return items[start:end]



def get_activities(opportunity_id, customer_id, token, dealer_key):
    """
    Replace CDK Activity History with Sales v1 activities.history/byOpportunityId,
    but preserve the same return shape:
        {
            "scheduledActivities": [...],
            "completedActivities": [...]
        }
    so that existing logic in processNewData continues to work.
    """

    url = f"{BASE_URL}/sales/v1/elead/activities/history/byOpportunityId/{opportunity_id}"
    headers = _headers(dealer_key, token)

    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code == 401:
        fresh = get_token(dealer_key, force_refresh=True)
        resp = requests.get(url, headers=_headers(dealer_key, fresh), timeout=30)

    resp.raise_for_status()
    data = resp.json() or {}
    items = data.get("items", []) or []

    # minimal map to keep ES happy; you can expand this with your full table
    TYPE_MAP = {
        "send email": 3,
        "appointment": 7,
        "internet up": 13,
        "read email": 20,
        "inbound email": 20,  # official type name in ref data
    }

    scheduled = []
    completed = []

    for it in items:
        raw_type = it.get("activityType")
        mapped_type = None

        if isinstance(raw_type, int):
            mapped_type = raw_type
        elif isinstance(raw_type, str):
            mapped_type = TYPE_MAP.get(raw_type.strip().lower())

        base = {
            "activityId": it.get("id"),
            "activityName": it.get("name"),
            "dueDate": it.get("dueDate"),
            "completedDate": it.get("completedDate"),
            "outcome": it.get("outcome"),
            "assignedTo": it.get("assignedTo"),
            # NOTE: comments are NOT available here (CDK-only field)
        }

        # Only include activityType if it's numeric so ES mapping doesn't explode
        if isinstance(mapped_type, int):
            base["activityType"] = mapped_type

        category = (it.get("category") or "").lower()
        outcome  = (it.get("outcome") or "").lower()

        # map into the buckets your code already expects
        if category == "scheduled" or outcome in ("open", "in progress"):
            scheduled.append(base)
        else:
            completed.append(base)

    return {
        "scheduledActivities": scheduled,
        "completedActivities": completed,
    }


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

