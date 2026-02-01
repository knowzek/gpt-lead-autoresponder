#airtable_store.py
import os, json, uuid
from datetime import datetime, timedelta, timezone
import requests
import hashlib

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

import re

def _as_str(v) -> str:
    """
    Convert common Airtable/Fortellis shapes to a clean string.
    - None -> ""
    - str -> stripped
    - dict -> try common keys, else ""
    - list -> join str parts
    - other -> str(v)
    """
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, dict):
        # common patterns: {"name": "..."}, {"value": "..."}
        for k in ("display", "name", "value", "label", "text"):
            if isinstance(v.get(k), str):
                return v[k].strip()
        return ""
    if isinstance(v, list):
        parts = []
        for item in v:
            s = _as_str(item)
            if s:
                parts.append(s)
        return " ".join(parts).strip()
    return str(v).strip()


def canonicalize_opp(opp: dict, fields: dict) -> dict:
    """
    Canonicalize opp so downstream code can safely rely on these keys:
      opportunityId, _subscription_id, rooftop_name,
      customer_first_name/last_name/email/phone,
      salesperson_name, vehicle, patti, source
    """
    opp = opp or {}
    fields = fields or {}

    # Ensure canonical keys always exist (prevents KeyError)
    for k in (
        "rooftop_name",
        "customer_first_name",
        "customer_last_name",
        "customer_email",
        "customer_phone",
        "salesperson_name",
        "vehicle",
    ):
        opp.setdefault(k, "")

    # ---- IDs ----
    opp_id = _as_str(
        opp.get("opportunityId")
        or opp.get("id")
        or fields.get("opp_id")
        or fields.get("opportunityId")
    )
    if opp_id:
        opp["opportunityId"] = opp_id
        opp["id"] = opp_id  # keep both in sync

    sub = _as_str(
        opp.get("_subscription_id")
        or fields.get("subscription_id")
        or fields.get("_subscription_id")
    )
    if sub:
        opp["_subscription_id"] = sub

    # ---- Rooftop ----
    # Airtable (preferred) + common Fortellis variants
    opp["rooftop_name"] = _as_str(
        fields.get("rooftop_name")
        or fields.get("Rooftop Name")
        or opp.get("rooftop_name")
        or opp.get("rooftopName")
        or (opp.get("dealer") or {}).get("name")
        or (opp.get("dealer") or {}).get("rooftopName")
        or (opp.get("dealerInfo") or {}).get("name")
    )

    # ---- Customer (prefer Airtable, but fallback to Fortellis/common shapes) ----
    cust = opp.get("customer") or {}
    opp["customer_first_name"] = _as_str(
        fields.get("Customer First Name")
        or opp.get("customer_first_name")
        or cust.get("firstName")
        or opp.get("firstName")
    )
    opp["customer_last_name"] = _as_str(
        fields.get("Customer Last Name")
        or opp.get("customer_last_name")
        or cust.get("lastName")
        or opp.get("lastName")
    )
    opp["customer_email"] = _as_str(
        fields.get("customer_email")
        or opp.get("customer_email")
        or cust.get("email")
        or opp.get("email")
    )
    opp["customer_phone"] = _as_str(
        fields.get("customer_phone")
        or opp.get("customer_phone")
        or cust.get("mobilePhone")
        or cust.get("phone")
        or opp.get("phone")
    )

    # ---- Salesperson ----
    sp = (
        fields.get("Assigned Sales Rep")
        or opp.get("salesperson_name")
        or (opp.get("salesperson") or {}).get("name")
        or opp.get("salesperson")
        or opp.get("Assigned Sales Rep")
    )
    opp["salesperson_name"] = _as_str(sp)

    # ---- Vehicle ----
    voi = opp.get("vehicleOfInterest")
    opp["vehicle"] = _as_str(
        fields.get("vehicle")
        or fields.get("Vehicle")
        or opp.get("vehicle")
        or (voi or {}).get("display")            # if dict
        or (voi or {}).get("name")
        or (voi or {}).get("value")
        or voi
    )

    # ---- Patti dict always exists ----
    if not isinstance(opp.get("patti"), dict):
        opp["patti"] = {}

    # ---- Source ----
    if not opp.get("source"):
        src = _as_str(fields.get("source") or opp.get("source"))
        if src:
            opp["source"] = src

    return opp



def _digits(phone: str) -> str:
    return re.sub(r"\D+", "", phone or "")

def find_by_customer_phone_loose(phone_any: str):
    """
    Matches Airtable customer_phone stored as 714-xxx-xxxx against
    incoming E164 (+1714...) by comparing last 10 digits.
    """
    d = _digits(phone_any)
    if len(d) < 10:
        return None
    last10 = d[-10:]

    # Airtable formula: compare last 10 digits by stripping non-digits isn't possible natively,
    # so we do a broader contains search on common patterns.
    # Best approach: fetch small batch and compare in Python.
    # But to keep it simple: query records where customer_phone contains last4 and then filter.
    last4 = last10[-4:]

    # you likely already have a "search" helper; if not, use filterByFormula on FIND()
    formula = f"FIND('{last4}', {{customer_phone}}) > 0"
    params = {"filterByFormula": formula, "maxRecords": 25}
    data = _request("GET", BASE_URL, params=params)
    recs = data.get("records") or []

    for rec in recs:
        f = rec.get("fields") or {}
        if _digits(f.get("customer_phone"))[-10:] == last10:
            return rec
    return None


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

    # ✅ enforce convo mode + clear follow_up_at immediately
    p = opp.setdefault("patti", {})
    if isinstance(p, dict):
        p["mode"] = "convo"
        p["last_customer_msg_at"] = when_iso

    return save_opp(opp, extra_fields={
        "Customer Replied": True,
        "First Customer Reply At": m["first_customer_reply_at"],
        "Last Customer Reply At": m["last_customer_reply_at"],
        "mode": "convo",
        "follow_up_at": None,   # ✅ critical
    })


def _truthy(x) -> bool:
    return bool(x) and str(x).strip() not in ("", "0", "False", "false", "None", "null")

def already_contacted_airtable(opp: dict) -> bool:
    """
    Airtable is the brain:
    Treat as contacted if ANY of these Airtable-backed fields indicate prior AI send.
    """
    # These are first-class Airtable columns you already have
    if _truthy(opp.get("first_email_sent_at")):
        return True
    if _truthy(opp.get("AI First Message Sent At")):
        return True
    if _truthy(opp.get("Last AI Message At")):
        return True

    # Numeric columns
    try:
        if int(opp.get("AI Messages Sent") or 0) > 0:
            return True
    except Exception:
        pass

    return False

def is_customer_replied_airtable(opp: dict) -> bool:
    # Checkbox + derived metrics
    if opp.get("Customer Replied") is True:
        return True
    m = opp.get("patti_metrics") if isinstance(opp.get("patti_metrics"), dict) else {}
    return bool(m.get("customer_replied"))

def get_mode_airtable(opp: dict) -> str:
    mode = ""
    if isinstance(opp.get("patti"), dict):
        mode = (opp["patti"].get("mode") or "")
    # also allow top-level column hydration (opp_from_record adds it)
    if not mode:
        mode = (opp.get("mode") or "")
    return (mode or "").strip().lower()

def should_suppress_all_sends_airtable(opp: dict, *, now_utc: datetime | None = None) -> tuple[bool, str]:
    """
    Hard stop gates, Airtable truth only.
    Returns (stop, reason).
    """
    now_utc = now_utc or _now_utc()

    # Compliance
    comp = opp.get("compliance") if isinstance(opp.get("compliance"), dict) else {}
    if comp.get("suppressed"):
        return True, "suppressed"

    if opp.get("Suppressed") is True:
        return True, "suppressed"

    if opp.get("opted_out") is True or opp.get("Unsubscribed") is True:
        return True, "opted_out/unsubscribed"

    if opp.get("is_active") is False or opp.get("isActive") is False:
        return True, "inactive"

    # Lease lock
    lu = opp.get("lock_until")
    if lu:
        try:
            lock_until = datetime.fromisoformat(str(lu).replace("Z", "+00:00"))
            if lock_until.tzinfo is None:
                lock_until = lock_until.replace(tzinfo=timezone.utc)
            if lock_until > now_utc:
                return True, "locked"
        except Exception:
            pass

    return False, ""

def pause_cadence_on_customer_reply(opp: dict, *, when_iso: str | None = None):
    """
    Single place to enforce:
    - mode=convo
    - Customer Replied timestamps
    - clear follow_up_at so cron can't cadence-spam
    """
    when_iso = when_iso or _now_iso_utc()

    # in-memory canonical fields
    p = opp.setdefault("patti", {})
    if isinstance(p, dict):
        p["mode"] = "convo"
        p["last_customer_msg_at"] = when_iso

    opp["Customer Replied"] = True  # convenience copy

    # persist Airtable truth columns
    extra = {
        "Customer Replied": True,
        "mode": "convo",
        "follow_up_at": None,              # ✅ crucial
        "Last Customer Reply At": when_iso,
    }
    # Don't overwrite first reply if it exists
    if not _truthy(opp.get("First Customer Reply At")):
        extra["First Customer Reply At"] = when_iso

    return save_opp(opp, extra_fields=extra)

def mark_ai_email_sent(
    opp: dict,
    *,
    when_iso: str | None = None,
    next_follow_up_at: str | None = None,
    force_mode: str | None = None,
    template_day: int | None = None,
    ab_variant: str | None = None,
):
    """
    Call this immediately after a successful outbound email send.

    Updates Airtable "brain" fields:
    - AI Messages Sent (+1)
    - AI Emails Sent (+1) if present
    - Last AI Message At = when
    - first_email_sent_at / AI First Message Sent At if empty
    - follow_up_at: set to next_follow_up_at ONLY if cadence mode and no customer reply
                 else clear it (None)
    """
    when_iso = when_iso or _now_iso_utc()

    # Local copies (so downstream code sees it in-memory too)
    opp["Last AI Message At"] = when_iso
    if not _truthy(opp.get("first_email_sent_at")):
        opp["first_email_sent_at"] = when_iso
    if not _truthy(opp.get("AI First Message Sent At")):
        opp["AI First Message Sent At"] = when_iso

    # Increment counts in-memory (best effort)
    try:
        opp["AI Messages Sent"] = int(opp.get("AI Messages Sent") or 0) + 1
    except Exception:
        opp["AI Messages Sent"] = 1

    try:
        # Only if you actually have this column; harmless if you don't pass it to patch
        opp["AI Emails Sent"] = int(opp.get("AI Emails Sent") or 0) + 1
    except Exception:
        pass

    # Decide mode
    mode = force_mode or get_mode_airtable(opp) or ""
    if isinstance(opp.get("patti"), dict) and force_mode:
        opp["patti"]["mode"] = force_mode

    customer_replied = is_customer_replied_airtable(opp)
    cadence_allowed = (mode in ("", "cadence")) and not customer_replied

    patch = {
        "Last AI Message At": when_iso,
        "first_email_sent_at": opp.get("first_email_sent_at") or when_iso,
        "AI First Message Sent At": opp.get("AI First Message Sent At") or when_iso,
        "AI Messages Sent": int(opp.get("AI Messages Sent") or 0),
        "mode": (force_mode or (mode or "")),
    }

    # ✅ persist A/B if provided
    if ab_variant:
        patch["ab_variant"] = ab_variant
        opp["ab_variant"] = ab_variant

    # ✅ persist last_template_day_sent if provided
    if template_day is not None:
        patch["last_template_day_sent"] = int(template_day)
        opp["last_template_day_sent"] = int(template_day)

    # If you have AI Emails Sent column, keep it updated too
    if "AI Emails Sent" in opp:
        patch["AI Emails Sent"] = int(opp.get("AI Emails Sent") or 0)

    # follow_up_at is ONLY cadence and ONLY if no customer reply
    if cadence_allowed and next_follow_up_at:
        patch["follow_up_at"] = _iso(next_follow_up_at)
        opp["follow_up_at"] = next_follow_up_at
        opp["followUP_date"] = next_follow_up_at
    else:
        patch["follow_up_at"] = None
        opp["follow_up_at"] = None
        opp["followUP_date"] = None

    return save_opp(opp, extra_fields=patch)


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

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

def is_opp_suppressed(opp_id: str) -> tuple[bool, str]:
    """
    Returns (suppressed, reason). Uses Airtable checkbox + reason field.
    """
    oid = (opp_id or "").strip()
    if not oid:
        return False, ""

    # Find the record for this opp_id
    rec = find_by_opp_id(oid)  # you already have this in airtable_store
    if not rec:
        return False, ""

    fields = (rec.get("fields") or {})
    if fields.get("Suppressed") is True:
        return True, (fields.get("Suppression Reason") or "suppressed")
    return False, ""


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

    # stop future sends + stop showing as due
    opp["isActive"] = False
    opp["followUP_date"] = None
    opp["follow_up_at"] = None

    # keep snapshot + columns aligned
    opp["compliance"] = {
        "suppressed": True,
        "reason": reason or "unsubscribe",
        "channel": "email",
        "at": when_iso,
    }

    return save_opp(opp, extra_fields={
        "Unsubscribed": True,
        "is_active": False,
        "follow_up_at": None,          # ✅ important
        "Suppressed": True,
        "Suppression Reason": reason or "unsubscribe",
        "Suppressed At": when_iso,
    })

def find_by_customer_email(email: str):
    email = (email or "").strip().lower()
    if not email:
        return None

    formula = f"LOWER({{customer_email}})='{email}'"

    params = {"filterByFormula": formula, "maxRecords": 1}
    data = _request("GET", BASE_URL, params=params)
    recs = data.get("records") or []
    return recs[0] if recs else None

def find_by_customer_phone(phone_e164: str):
    phone_e164 = (phone_e164 or "").strip()
    if not phone_e164:
        return None

    # Exact match (works if you store +1E164 consistently)
    formula = f"{{customer_phone}}='{phone_e164}'"
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
"""

def acquire_lock(rec_id: str, lock_minutes: int = 10) -> str | None:
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

"""

def acquire_lock(rec_or_id, lock_minutes: int = 10) -> str | None:
    """
    Best-effort lease lock.
    Accepts either:
      - Airtable record dict (preferred: saves 1 GET)
      - Airtable record id string (falls back to 1 GET)
    """
    # Case 1: caller passed the whole record (preferred)
    if isinstance(rec_or_id, dict):
        rec = rec_or_id
        rec_id = rec.get("id")
        f = rec.get("fields", {}) or {}
        if not rec_id:
            return None
    else:
        # Case 2: caller passed rec_id string (fallback: requires 1 GET)
        rec_id = str(rec_or_id or "").strip()
        if not rec_id:
            return None
        rec = _request("GET", f"{BASE_URL}/{rec_id}")
        f = rec.get("fields", {}) or {}

    now = _now_utc()

    lock_until = f.get("lock_until")
    if lock_until:
        try:
            lu = datetime.fromisoformat(str(lock_until).replace("Z", "+00:00"))
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
    """
    Release without a GET (saves 1 GET per record).
    This is *best effort* — if another worker stole the lock, it may clear theirs.
    If you truly need strict safety, keep the GET+token-check version.
    """
    rec_id = (rec_id or "").strip()
    token = (token or "").strip()
    if not rec_id or not token:
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
    if not opp:
        opp = _safe_json_loads(fields.get("opp_json")) or {}
    # Always attach Airtable record id
    opp["_airtable_rec_id"] = rec.get("id")
    opp = canonicalize_opp(opp, fields)

    # --- Hydrate Assigned Sales Rep from Airtable column ---
    asr = fields.get("Assigned Sales Rep")
    if asr:
        if isinstance(asr, list):
            asr = asr[0] if asr else ""
        if isinstance(asr, dict):
            asr = asr.get("name") or asr.get("value") or ""
        opp["Assigned Sales Rep"] = str(asr).strip()

    # ✅ Hydrate KBB offer memo from Airtable field (authoritative)
    kbb_ctx_raw = fields.get("kbb_offer_ctx")
    if kbb_ctx_raw:
        try:
            if isinstance(kbb_ctx_raw, str):
                parsed = json.loads(kbb_ctx_raw)
            else:
                parsed = kbb_ctx_raw
            if isinstance(parsed, dict):
                opp["_kbb_offer_ctx"] = parsed
        except Exception:
            pass


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

    aph = (fields.get("customer_phone") or "").strip()
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
            "reason": (fields.get("Suppression Reason") or "unsubscribe"),
            "channel": "email",
            "at": (fields.get("Suppressed At") or ""),
        }
    else:
        if not isinstance(opp.get("compliance"), dict):
            opp["compliance"] = {"suppressed": False}

    # ✅ Cadence anchor (authoritative from Airtable)
    anchor = (
        fields.get("salesai_created_iso")
        or fields.get("Lead Created At")
        or fields.get("Created At")
    )
    if anchor:
        p = opp.setdefault("patti", {})
        if isinstance(p, dict):
            # do NOT overwrite if snapshot already has one
            p.setdefault("salesai_created_iso", anchor)
            
    # ✅ Normalize cadence state if snapshot has nulls
    p = opp.setdefault("patti", {})
    if isinstance(p, dict):
        if p.get("salesai_email_idx") is None:
            p["salesai_email_idx"] = -1
        if p.get("last_template_day_sent") is None:
            p["last_template_day_sent"] = 0


    # ✅ Hydrate first-touch + routing flag (authoritative for cron routing)
    fes = (
        fields.get("first_email_sent_at")
        or fields.get("First Email Sent At")
        or fields.get("AI First Message Sent At")
    )

    if fes:
        # persist into opp so downstream checks based on timestamp also work
        opp.setdefault("first_email_sent_at", fes)

        # critical: this is what most routers use to skip first-touch
        checked = opp.setdefault("checkedDict", {})
        if isinstance(checked, dict):
            checked["patti_already_contacted"] = True

    # ✅ Airtable-brain fields (authoritative gates)
    if "Customer Replied" in fields:
        opp["Customer Replied"] = bool(fields.get("Customer Replied"))

    # Timestamps used for gating decisions
    for k in ("first_email_sent_at", "follow_up_at", "First Customer Reply At", "Last Customer Reply At",
              "AI First Message Sent At", "Last AI Message At"):
        if fields.get(k) and not opp.get(k):
            opp[k] = fields.get(k)

    # Counters
    if "AI Messages Sent" in fields:
        opp["AI Messages Sent"] = int(fields.get("AI Messages Sent") or 0)
    if "AI Emails Sent" in fields:
        opp["AI Emails Sent"] = int(fields.get("AI Emails Sent") or 0)

    # If Airtable says convo, reflect it in patti mode
    mode_col = (fields.get("mode") or "").strip().lower()
    if mode_col:
        p = opp.setdefault("patti", {})
        if isinstance(p, dict):
            p["mode"] = mode_col


    # ✅ Normalize cadence state if snapshot has nulls
    p = opp.setdefault("patti", {})
    if isinstance(p, dict):
        if p.get("salesai_email_idx") is None:
            p["salesai_email_idx"] = -1
        if p.get("last_template_day_sent") is None:
            p["last_template_day_sent"] = 0
    
    # ✅ TK GM Day 2 Sent (authoritative Airtable checkbox) - read back from Airtable
    tk_gm_day2_sent = bool(fields.get("TK GM Day 2 Sent"))
    opp["TK GM Day 2 Sent"] = tk_gm_day2_sent  # Map to actual Airtable field name for downstream checks
    opp["tk_gm_day2_sent"] = tk_gm_day2_sent
    
    tk_gm_day2_sent_at = fields.get("TK GM Day 2 Sent At")
    if tk_gm_day2_sent_at:
        opp["TK GM Day 2 Sent At"] = tk_gm_day2_sent_at
        opp["tk_gm_day2_sent_at"] = tk_gm_day2_sent_at
    
    # ✅ TK Day 3 Walkaround Sent (authoritative Airtable checkbox) - read back from Airtable
    tk_day3_sent = bool(fields.get("TK Day 3 Walkaround Sent"))
    opp["TK Day 3 Walkaround Sent"] = tk_day3_sent
    
    tk_day3_sent_at = fields.get("TK Day 3 Walkaround Sent At")
    if tk_day3_sent_at:
        opp["TK Day 3 Walkaround Sent At"] = tk_day3_sent_at
    
    # Optional derived state (only if other cadence code still relies on it)
    if tk_gm_day2_sent and isinstance(p, dict):
        try:
            p["last_template_day_sent"] = max(int(p.get("last_template_day_sent") or 0), 2)
        except Exception:
            p["last_template_day_sent"] = 2
    
    if tk_day3_sent and isinstance(p, dict):
        try:
            p["last_template_day_sent"] = max(int(p.get("last_template_day_sent") or 0), 3)
        except Exception:
            p["last_template_day_sent"] = 3
    
    return opp



def get_by_id(rec_id: str) -> dict:
    return _request("GET", f"{BASE_URL}/{rec_id}")



def save_opp(opp: dict, *, extra_fields: dict | None = None):
    """
    Drop-in replacement: safe, deterministic, never references undefined vars.
    - Airtable is the brain (mode + follow_up_at rules)
    - Keeps patti_json + patti_hash change-detection (but fail-open if snapshot can't serialize)
    - Never PATCH computed/formula/rollup fields
    """
    import json
    import hashlib

    rec_id = opp.get("_airtable_rec_id")
    if not rec_id:
        raise RuntimeError("Missing opp['_airtable_rec_id']; cannot save to Airtable")

    extra_fields = extra_fields or {}

    # Re-hydrate identity + grab existing fields (for prev_hash + ids)
    fields = {}
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

    # is_active (truthy default True)
    is_active = bool(opp.get("isActive", True))

    # ---------------------------
    # mode resolution (NEVER reference bare `mode`)
    # caller extra_fields wins
    # ---------------------------
    patti = opp.get("patti") if isinstance(opp.get("patti"), dict) else {}
    mode_value = (
        extra_fields.get("mode")
        or patti.get("mode")
        or opp.get("mode")
        or ""
    )
    mode_norm = str(mode_value).strip().lower()

    # ---------------------------
    # customer replied (Airtable brain signals)
    # ---------------------------
    customer_replied = (
        (opp.get("Customer Replied") is True)
        or (isinstance(opp.get("patti_metrics"), dict) and bool(opp["patti_metrics"].get("customer_replied")))
    )

    # ---------------------------
    # follow_up_at brain rules
    # - if caller explicitly passed follow_up_at (even None) => use it
    # - else: only allow follow_up_at in cadence mode and only if no customer reply
    # ---------------------------
    if "follow_up_at" in extra_fields:
        follow_up_at_value = extra_fields.get("follow_up_at")  # may be None on purpose
    else:
        if (mode_norm in ("", "cadence")) and not customer_replied:
            follow_up_at_value = opp.get("follow_up_at") or opp.get("followUP_date")
        else:
            follow_up_at_value = None

    # Base patch (caller can still override after this)
    patch = {
        "is_active": is_active,
        "follow_up_at": _iso(follow_up_at_value),
        "mode": mode_norm,  # normalized storage ("cadence", "convo", "scheduled", etc.)
    }

    # ---------------------------
    # Snapshot hashing for patti_json (safe)
    # ---------------------------
    prev_hash = (fields.get("patti_hash") or "").strip()
    
    try:
        snapshot_obj = _build_patti_snapshot(opp)  # ✅ tiny snapshot only
        snapshot_str = json.dumps(snapshot_obj, sort_keys=True, ensure_ascii=False, default=str)
    
        MAX = 95_000
        if len(snapshot_str) > MAX:
            # Fail-open: don't write it at all
            # (optional) log.warning("patti_json snapshot too large; skipping write len=%s opp=%s", len(snapshot_str), opp.get("opportunityId"))
            pass
        else:
            snapshot_hash = hashlib.sha256(snapshot_str.encode("utf-8")).hexdigest()
            if snapshot_hash and snapshot_hash != prev_hash:
                patch["patti_json"] = snapshot_str
                patch["patti_hash"] = snapshot_hash
    
        # Absolute safety: never allow oversize patti_json into the patch
        if "patti_json" in patch and len(patch["patti_json"]) > MAX:
            patch.pop("patti_json", None)
            patch.pop("patti_hash", None)
    
    except Exception:
        # Fail-open: don't block saving brain fields if snapshot serialization fails
        pass


    # ---------------------------
    # Mirror compliance into columns (safe)
    # ---------------------------
    comp = opp.get("compliance")
    if not isinstance(comp, dict):
        comp = {"suppressed": False}

    patch["Suppressed"] = bool(comp.get("suppressed"))
    if comp.get("suppressed"):
        patch["Suppression Reason"] = comp.get("reason") or ""
        patch["Suppressed At"] = comp.get("at") or ""

    # ---------------------------
    # Apply caller overrides last (caller wins)
    # ---------------------------
    if extra_fields:
        patch.update(extra_fields)

    # ---------------------------
    # Never PATCH computed/formula/rollup fields
    # ---------------------------
    COMPUTED_FIELDS = {
        "customer_email_lower",
        # add others here if Airtable complains (formula/rollup/lookup fields)
    }
    for k in list(patch.keys()):
        if k in COMPUTED_FIELDS:
            patch.pop(k, None)

    return patch_by_id(rec_id, patch)
