#airtable_store.py
import os, json, uuid
from datetime import datetime, timedelta, timezone
import requests
import hashlib
from datetime import datetime as _dt, timezone as _tz

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

def _now_iso_utc():
    return datetime.now(timezone.utc).isoformat()

def mark_customer_reply(opp: dict, *, when_iso: str | None = None):
    when_iso = when_iso or _now_iso_utc()
    m = opp.setdefault("patti_metrics", {})

    if not m.get("first_customer_reply_at"):
        m["first_customer_reply_at"] = when_iso
    m["last_customer_reply_at"] = when_iso
    m["customer_replied"] = True

    return save_opp(opp, extra_fields={
        "Customer Replied": True,
        "First Customer Reply At": m["first_customer_reply_at"],
        "Last Customer Reply At": m["last_customer_reply_at"],
    })

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _iso_now() -> str:
    return _dt.now(_tz.utc).isoformat().replace("+00:00", "Z")

def _extract_compliance(opp: dict) -> dict:
    """
    Canonical: opp["compliance"] (or opp["patti"]["compliance"] fallback).
    Always returns a dict with at least {"suppressed": bool}.
    """
    comp = None
    if isinstance(opp.get("compliance"), dict):
        comp = opp.get("compliance")
    elif isinstance(opp.get("patti"), dict) and isinstance(opp["patti"].get("compliance"), dict):
        comp = opp["patti"].get("compliance")

    if not isinstance(comp, dict):
        return {"suppressed": False}

    suppressed = bool(comp.get("suppressed"))
    if not suppressed:
        return {"suppressed": False}

    return {
        "suppressed": True,
        "reason": (comp.get("reason") or "").strip() or "unsubscribe",
        "channel": (comp.get("channel") or "email").strip() or "email",
        "at": (comp.get("at") or _iso_now()),
    }

def _build_patti_snapshot(opp: dict) -> dict:
    patti = opp.get("patti") if isinstance(opp.get("patti"), dict) else {}
    metrics = opp.get("patti_metrics") if isinstance(opp.get("patti_metrics"), dict) else {}

    cust = opp.get("customer") if isinstance(opp.get("customer"), dict) else {}
    veh  = opp.get("vehicle") if isinstance(opp.get("vehicle"), dict) else {}

    return {
        "opportunityId": opp.get("opportunityId") or opp.get("id"),
        "subscription_id": opp.get("_subscription_id") or opp.get("subscription_id"),
        "source": opp.get("source") or "",
        "customer": {
            "firstName": cust.get("firstName") or opp.get("customer_first_name") or "",
            "lastName":  cust.get("lastName")  or opp.get("customer_last_name")  or "",
            "email":     opp.get("customer_email") or cust.get("email") or "",
            "phone":     opp.get("customer_phone") or "",
        },
        "vehicle": {
            "year":  veh.get("year") or opp.get("year") or "",
            "make":  veh.get("make") or opp.get("make") or "",
            "model": veh.get("model") or opp.get("model") or "",
            "vin":   veh.get("vin") or opp.get("vin") or "",
        },
        "patti": {
            "mode": patti.get("mode") or "",
            "salesai_email_idx": patti.get("salesai_email_idx"),
            "last_template_day_sent": patti.get("last_template_day_sent"),
            "last_customer_msg_at": patti.get("last_customer_msg_at"),
            "handoff": patti.get("handoff") if isinstance(patti.get("handoff"), dict) else None,
        },
        "patti_metrics": {
            "customer_replied": metrics.get("customer_replied"),
            "first_customer_reply_at": metrics.get("first_customer_reply_at"),
            "last_customer_reply_at": metrics.get("last_customer_reply_at"),
        },
        "compliance": _extract_compliance(opp),
    }


def mark_unsubscribed(opp: dict, *, when_iso: str | None = None, reason: str = ""):
    when_iso = when_iso or _now_iso_utc()
    m = opp.setdefault("patti_metrics", {})
    m["unsubscribed"] = True
    m["unsubscribed_at"] = when_iso
    if reason:
        m["unsubscribed_reason"] = reason[:500]

    opp["isActive"] = False  # stop future sends

    return save_opp(opp, extra_fields={
        "Unsubscribed": True,
        "is_active": False,
    })

def mark_customer_reply(opp: dict, *, when_iso: str | None = None):
    when_iso = when_iso or _now_iso_utc()

    m = opp.setdefault("patti_metrics", {})

    # First reply only gets set once
    if not m.get("first_customer_reply_at"):
        m["first_customer_reply_at"] = when_iso

    # Last reply always updates
    m["last_customer_reply_at"] = when_iso
    m["customer_replied"] = True

    save_opp(opp, extra_fields={
        "Customer Replied": True,
        "First Customer Reply At": m["first_customer_reply_at"],
        "Last Customer Reply At": m["last_customer_reply_at"],
    })


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
    """
    Airtable Long text has practical limits; also opp blobs can grow huge.
    Keep opp_json stable by removing very large fields and hard-capping size.
    """
    def _slim(o: dict) -> dict:
        if not isinstance(o, dict):
            return o or {}

        o = dict(o)

        # Drop the biggest / noisiest fields
        for k in (
            "completedActivities",
            "scheduledActivities",
            "activities",
            "alreadyProcessedActivities",
        ):
            if k in o:
                o.pop(k, None)

        # Messages can explode; keep only last N and clip bodies
        msgs = o.get("messages")
        if isinstance(msgs, list) and msgs:
            keep = []
            for m in msgs[-25:]:
                if not isinstance(m, dict):
                    continue
                mm = dict(m)

                # clip heavy fields
                for body_key in ("body", "body_html", "bodyHtml", "raw", "html"):
                    if body_key in mm and isinstance(mm[body_key], str):
                        mm[body_key] = mm[body_key][:2000]

                keep.append(mm)
            o["messages"] = keep

        return o

    try:
        slim_obj = _slim(obj if obj is not None else {})
        s = json.dumps(slim_obj, ensure_ascii=False)

        # Hard cap (Airtable rejects oversize text)
        MAX = 95000
        if len(s) > MAX:
            # Keep the front (schema/ids) and signal truncation
            s = s[:MAX] + '"__TRUNCATED__":true}'
        return s
    except Exception:
        return "{}"


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
    Return the opportunity dict from Airtable record (patti_json snapshot).
    Also attaches the record id for persistence.
    Hydrates key identity fields from Airtable columns so downstream code
    can rely on them even if patti_json is partial.
    """
    fields = rec.get("fields", {}) or {}

    # ✅ NEW: load snapshot JSON instead of full opp_json blob
    opp = _safe_json_loads(fields.get("patti_json")) or {}

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

    if fields.get("customer_email"):
        opp["customer_email"] = fields.get("customer_email")

    aph = (fields.get("Phone") or "").strip()
    if aph:
        opp["customer_phone"] = aph

    # ✅ Hydrate customer first/last name from Airtable columns
    afn = (fields.get("Customer First Name") or "").strip()
    aln = (fields.get("Customer Last Name") or "").strip()
    if afn or aln:
        cust = opp.get("customer")
        if not isinstance(cust, dict):
            cust = {}
            opp["customer"] = cust

        if afn and not (cust.get("firstName") or "").strip():
            cust["firstName"] = afn
        if aln and not (cust.get("lastName") or "").strip():
            cust["lastName"] = aln

        # optional convenience copies
        opp.setdefault("customer_first_name", afn)
        opp.setdefault("customer_last_name", aln)

    # --- Hydrate human review flags from Airtable columns ---
    if "Needs Human Review" in fields:
        opp["needs_human_review"] = bool(fields.get("Needs Human Review"))
    else:
        opp.setdefault("needs_human_review", False)

    if fields.get("Human Review Reason") and not opp.get("human_review_reason"):
        opp["human_review_reason"] = fields.get("Human Review Reason")

    if fields.get("Human Review At") and not opp.get("human_review_at"):
        opp["human_review_at"] = fields.get("Human Review At")

    # ✅ NEW: Hydrate suppression/compliance from Airtable columns (authoritative for gating)
    if fields.get("Suppressed") is True:
        opp["compliance"] = {
            "suppressed": True,
            "reason": fields.get("Suppression Reason") or "unsubscribe",
            "channel": "email",
            "at": fields.get("Suppressed At") or "",
        }
    else:
        # If snapshot already has compliance, keep it; otherwise default false.
        if not isinstance(opp.get("compliance"), dict):
            opp["compliance"] = {"suppressed": False}

    return opp


def get_by_id(rec_id: str) -> dict:
    return _request("GET", f"{BASE_URL}/{rec_id}")



def save_opp(opp: dict, *, extra_fields: dict | None = None):
    rec_id = opp.get("_airtable_rec_id")
    if not rec_id:
        raise RuntimeError("Missing opp['_airtable_rec_id']; cannot save to Airtable")

    fields = {}
    # (optional) re-hydrate identity — keep (and we also use fields for hash compare)
    try:
        rec = get_by_id(rec_id)
        fields = (rec or {}).get("fields", {}) or {}

        airtable_opp_id = (fields.get("opp_id") or "").strip()
        airtable_sub_id = (fields.get("subscription_id") or "").strip()

        if airtable_opp_id:
            opp.setdefault("opportunityId", airtable_opp_id)
            opp.setdefault("id", airtable_opp_id)
        if airtable_sub_id:
            opp.setdefault("_subscription_id", airtable_sub_id)
    except Exception:
        fields = {}

    is_active = bool(opp.get("isActive", True))
    follow_up_at = opp.get("followUP_date") or opp.get("follow_up_at")

    mode = None
    if isinstance(opp.get("_kbb_state"), dict):
        mode = opp["_kbb_state"].get("mode")
    if not mode and isinstance(opp.get("patti"), dict):
        mode = opp["patti"].get("mode")

    # ✅ Snapshot JSON + hash to avoid rewriting every time
    snapshot = _build_patti_snapshot(opp)
    snapshot_str = json.dumps(snapshot, ensure_ascii=False)
    snapshot_hash = _sha1(snapshot_str)
    prev_hash = (fields.get("patti_hash") or "").strip()

    patch = {
        "is_active": is_active,
        "follow_up_at": _iso(follow_up_at),
        "mode": (mode or ""),
    }

    if snapshot_hash != prev_hash:
        patch["patti_json"] = snapshot_str
        patch["patti_hash"] = snapshot_hash

    # ✅ Mirror compliance into columns for filtering/reporting
    comp = snapshot.get("compliance") or {"suppressed": False}
    patch["Suppressed"] = bool(comp.get("suppressed"))
    if comp.get("suppressed"):
        patch["Suppression Reason"] = comp.get("reason") or ""
        patch["Suppressed At"] = comp.get("at") or ""

    if extra_fields:
        patch.update(extra_fields)

    # ✅ Never PATCH computed/formula/rollup fields in Airtable
    COMPUTED_FIELDS = {
        "customer_email_lower",
    }
    for k in list(patch.keys()):
        if k in COMPUTED_FIELDS:
            patch.pop(k, None)

    return patch_by_id(rec_id, patch)
