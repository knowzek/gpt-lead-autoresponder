#airtable_store.py
import os, json, uuid
from datetime import datetime, timedelta, timezone
import requests

AIRTABLE_API_TOKEN = os.getenv("AIRTABLE_API_TOKEN")
AIRTABLE_BASE_ID   = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE     = os.getenv("AIRTABLE_TABLE_NAME", "Leads")

if not AIRTABLE_API_TOKEN or not AIRTABLE_BASE_ID:
    raise RuntimeError("Missing AIRTABLE_API_TOKEN or AIRTABLE_BASE_ID")

BASE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"
HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_TOKEN}",
    "Content-Type": "application/json",
}

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def find_by_customer_email(email: str):
    email = (email or "").strip().lower()
    if not email:
        return None

    # If you have a dedicated column like customer_email, use it (fastest):
    # formula = f"LOWER({{customer_email}})='{email}'"

    # Otherwise search inside opp_json (works fine for Phase 2):
    safe = email.replace("'", "\\'")
    formula = f"FIND('{safe}', LOWER({{opp_json}}))"

    params = {"filterByFormula": formula, "maxRecords": 1}
    data = _request("GET", BASE_URL, params=params)
    recs = data.get("records") or []
    return recs[0] if recs else None


def _iso(dt: datetime | str | None) -> str | None:
    if dt is None:
        return None
    if isinstance(dt, str):
        return dt
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def _safe_json_dumps(obj) -> str:
    return json.dumps(obj if obj is not None else {}, ensure_ascii=False)

def _safe_json_loads(s: str | None):
    if not s:
        return {}
    try:
        return json.loads(s)
    except Exception:
        return {}

def _request(method: str, url: str, **kwargs):
    r = requests.request(method, url, headers=HEADERS, timeout=30, **kwargs)
    if r.status_code >= 400:
        raise RuntimeError(f"Airtable {method} failed {r.status_code}: {r.text[:800]}")
    return r.json()

def find_by_opp_id(opp_id: str) -> dict | None:
    params = {"filterByFormula": f'{{opp_id}}="{opp_id}"', "pageSize": 1}
    data = _request("GET", BASE_URL, params=params)
    recs = data.get("records", [])
    return recs[0] if recs else None

def upsert_lead(opp_id: str, fields: dict) -> dict:
    existing = find_by_opp_id(opp_id)
    payload = {"fields": {"opp_id": opp_id, **fields}}
    if existing:
        return _request("PATCH", f"{BASE_URL}/{existing['id']}", json=payload)
    return _request("POST", BASE_URL, json=payload)

def patch_by_id(rec_id: str, fields: dict) -> dict:
    return _request("PATCH", f"{BASE_URL}/{rec_id}", json={"fields": fields})

def query_view(view: str, max_records: int = 200) -> list[dict]:
    out = []
    offset = None
    while True:
        params = {"view": view, "pageSize": 100}
        if offset:
            params["offset"] = offset
        data = _request("GET", BASE_URL, params=params)
        out.extend(data.get("records", []))
        if len(out) >= max_records:
            return out[:max_records]
        offset = data.get("offset")
        if not offset:
            return out

def acquire_lock(rec_id: str, lock_minutes: int = 10) -> str | None:
    """
    Best-effort lease lock (prevents overlapping cron runs from double-sending).
    """
    rec = _request("GET", f"{BASE_URL}/{rec_id}")
    f = rec.get("fields", {})
    now = _now_utc()

    lock_until = f.get("lock_until")
    if lock_until:
        try:
            lu = datetime.fromisoformat(lock_until.replace("Z", "+00:00"))
            if lu > now:
                return None
        except Exception:
            pass

    token = uuid.uuid4().hex
    patch_by_id(rec_id, {
        "lock_until": _iso(now + timedelta(minutes=lock_minutes)),
        "lock_token": token,
    })
    return token

def release_lock(rec_id: str, token: str):
    rec = _request("GET", f"{BASE_URL}/{rec_id}")
    if rec.get("fields", {}).get("lock_token") != token:
        return
    patch_by_id(rec_id, {"lock_until": None, "lock_token": ""})

def opp_from_record(rec: dict) -> dict:
    """
    Return the opportunity dict from Airtable record (opp_json).
    Also attaches the record id for persistence.
    Hydrates key identity fields from Airtable columns so downstream code
    can rely on them even if opp_json is partial.
    """
    fields = rec.get("fields", {}) or {}
    opp = _safe_json_loads(fields.get("opp_json")) or {}

    # Always attach Airtable record id
    opp["_airtable_rec_id"] = rec.get("id")

    # --- Hydrate canonical opp id ---
    airtable_opp_id = (fields.get("opp_id") or fields.get("opportunityId") or fields.get("id") or "").strip()
    if airtable_opp_id:
        opp.setdefault("opportunityId", airtable_opp_id)
        opp.setdefault("id", airtable_opp_id)

    # --- Hydrate subscription id for Fortellis calls ---
    airtable_sub = (fields.get("subscription_id") or fields.get("_subscription_id") or "").strip()
    if airtable_sub:
        opp.setdefault("_subscription_id", airtable_sub)

    # Optional: hydrate useful display fields (won’t hurt anything)
    if fields.get("source") and not opp.get("source"):
        opp["source"] = fields.get("source")

    if fields.get("customer_email") and not opp.get("customer_email"):
        opp["customer_email"] = fields.get("customer_email")

        # --- Hydrate human review flags from Airtable columns ---
    # Airtable checkbox returns True/False when present.
    if "Needs Human Review" in fields:
        opp["needs_human_review"] = bool(fields.get("Needs Human Review"))
    else:
        # default if column missing in this base/table
        opp.setdefault("needs_human_review", False)

    # hydrate reason + timestamp for logging/debugging
    if fields.get("Human Review Reason") and not opp.get("human_review_reason"):
        opp["human_review_reason"] = fields.get("Human Review Reason")

    if fields.get("Human Review At") and not opp.get("human_review_at"):
        opp["human_review_at"] = fields.get("Human Review At")


    return opp

def get_by_id(rec_id: str) -> dict:
    return _request("GET", f"{BASE_URL}/{rec_id}")



def save_opp(opp: dict, *, extra_fields: dict | None = None):
    """
    Persist the full opportunity dict back to Airtable as opp_json,
    plus key index fields used for filtering.

    IMPORTANT: always preserve identity fields (opportunityId/_subscription_id)
    even if this in-memory opp dict is a slim "state blob".
    """
    rec_id = opp.get("_airtable_rec_id")
    if not rec_id:
        raise RuntimeError("Missing opp['_airtable_rec_id']; cannot save to Airtable")

    # --- Re-hydrate identity from Airtable record fields when missing ---
    try:
        # If you have a "get by id" helper, use it.
        # Otherwise, patch_by_id works without fetching — so we need a fetch helper.
        # Assuming you have _request or get_by_id; if not, add it (see below).
        rec = get_by_id(rec_id)  # <-- add this helper in airtable_store if you don't have it
        fields = (rec or {}).get("fields", {}) or {}

        airtable_opp_id = (fields.get("opp_id") or "").strip()
        airtable_sub_id = (fields.get("subscription_id") or "").strip()

        if airtable_opp_id:
            opp.setdefault("opportunityId", airtable_opp_id)
            opp.setdefault("id", airtable_opp_id)

        if airtable_sub_id:
            opp.setdefault("_subscription_id", airtable_sub_id)

    except Exception:
        # Don't block save_opp if fetch fails; we'll still write what we have.
        pass

    # Normalize common fields
    is_active = bool(opp.get("isActive", True))
    follow_up_at = opp.get("followUP_date") or opp.get("follow_up_at")

    # Some flows store meta in _kbb_state or patti.mode; pick the best available
    mode = None
    if isinstance(opp.get("_kbb_state"), dict):
        mode = opp["_kbb_state"].get("mode")
    if not mode and isinstance(opp.get("patti"), dict):
        mode = opp["patti"].get("mode")

    patch = {
        "is_active": is_active,
        "follow_up_at": _iso(follow_up_at),
        "mode": (mode or ""),
        "opp_json": _safe_json_dumps(opp),
    }
    if extra_fields:
        patch.update(extra_fields)

    return patch_by_id(rec_id, patch)
