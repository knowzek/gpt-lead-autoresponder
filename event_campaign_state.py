# event_campaign_state.py
from __future__ import annotations

import os
import re
import logging
from datetime import datetime, timezone
from typing import Any, Iterable

import requests

from patti_mailer import send_via_sendgrid
from goto_sms import send_sms
from rooftops import SUBSCRIPTION_TO_ROOFTOP
from patti_triage import get_human_review_config
from outlook_email import send_email_via_outlook
from event_campaign_brain import generate_event_reply

log = logging.getLogger("patti.event_campaign_state")

AIRTABLE_API_TOKEN = (os.getenv("AIRTABLE_API_TOKEN") or "").strip()
AIRTABLE_BASE_ID = (os.getenv("AIRTABLE_BASE_ID") or "").strip()

EVENTS_TABLE = (os.getenv("EVENTS_TABLE_NAME") or "Events").strip()
GUESTS_TABLE = (os.getenv("EVENT_GUESTS_TABLE_NAME") or "Guests").strip()
INVITES_TABLE = (os.getenv("EVENT_INVITES_TABLE_NAME") or "Event Invites").strip()

if not AIRTABLE_API_TOKEN or not AIRTABLE_BASE_ID:
    raise RuntimeError("Missing AIRTABLE_API_TOKEN or AIRTABLE_BASE_ID")

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_TOKEN}",
    "Content-Type": "application/json",
}

STORE_TO_SMS_FROM = {
    (rec.get("name") or "").strip().lower(): (rec.get("sms_number") or "").strip()
    for rec in SUBSCRIPTION_TO_ROOFTOP.values()
    if rec.get("name") and rec.get("sms_number")
}

STOP_RE = re.compile(r"(?i)\b(stop|unsubscribe|cancel|end|quit|remove me|do not contact|don't contact|dont contact)\b")
RSVP_YES_RE = re.compile(
    r"(?i)^\s*(yes|y|yes please|i(?:'| a)?m in|i(?:'| a)?ll be there|count me in|we(?:'| a)?ll be there|attending|coming)\s*[!.]?\s*$"
)
RSVP_MAYBE_RE = re.compile(
    r"(?i)\b(maybe|might|possibly|i think so|should be able to|probably)\b"
)
RSVP_NO_RE = re.compile(
    r"(?i)\b(no|can'?t make it|cannot make it|won't make it|will not make it|not coming)\b"
)


def _table_url(table_name: str) -> str:
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_name}"


def _request(method: str, url: str, **kwargs) -> dict:
    r = requests.request(method, url, headers=AIRTABLE_HEADERS, timeout=30, **kwargs)
    if r.status_code >= 400:
        raise RuntimeError(f"Airtable {method} failed {r.status_code}: {r.text[:800]}")
    return r.json()

def _patch_event_reply_activity(
    invite_rec: dict,
    channel: str,
    body_text: str,
    *,
    stop_nudges: bool = True,
    status: str = "Responded",
) -> None:
    fields = {
        "Last Response At": _now_iso(),
        "Last Response Channel": channel,
        "Last Response Text": body_text[:1000],
    }
    if stop_nudges:
        fields["Stop Event Nudges"] = True
    if status:
        fields["Invite Status"] = status

    _patch_record(INVITES_TABLE, invite_rec["id"], fields)

def _fetch_all_records(table_name: str, *, formula: str = "", max_records: int = 1000) -> list[dict]:
    records: list[dict] = []
    offset = None

    while True:
        params: dict[str, Any] = {"pageSize": 100}
        if formula:
            params["filterByFormula"] = formula
        if offset:
            params["offset"] = offset

        data = _request("GET", _table_url(table_name), params=params)
        batch = data.get("records") or []
        records.extend(batch)

        if len(records) >= max_records:
            return records[:max_records]

        offset = data.get("offset")
        if not offset:
            return records


def _fetch_record_map(table_name: str, record_ids: Iterable[str]) -> dict[str, dict]:
    ids = [rid for rid in record_ids if rid]
    if not ids:
        return {}

    out: dict[str, dict] = {}
    chunk = 10
    for i in range(0, len(ids), chunk):
        ids_chunk = ids[i:i + chunk]
        formula = "OR(" + ",".join(f"RECORD_ID()='{rid}'" for rid in ids_chunk) + ")"
        for rec in _fetch_all_records(table_name, formula=formula, max_records=chunk):
            out[rec["id"]] = rec
    return out


def _patch_record(table_name: str, rec_id: str, fields: dict[str, Any]) -> None:
    _request("PATCH", f"{_table_url(table_name)}/{rec_id}", json={"fields": fields})


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _s(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _digits(v: str) -> str:
    return re.sub(r"\D+", "", v or "")


def _parse_dt(v: Any) -> datetime | None:
    if not v:
        return None
    s = _s(v).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _extract_email(v: Any) -> str:
    s = _s(v).lower()
    m = re.search(r'([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})', s, re.I)
    return (m.group(1) if m else s).strip().lower()


def _strip_reply_text(body_text: str) -> str:
    text = _s(body_text)
    if not text:
        return ""

    for sep in [
        "\r\n________________________________",
        "\n________________________________",
        "\r\nFrom:",
        "\nFrom:",
        "\r\nOn ",
        "\nOn ",
        " From:",
        " Sent:",
        " Subject:",
        " To:",
    ]:
        idx = text.find(sep)
        if idx != -1:
            text = text[:idx].strip()
            break

    # keep first non-empty line if it's a tiny RSVP-style response
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if lines and len(lines[0]) <= 120:
        return lines[0]
    return text[:500]


def _normalize_phone_e164_us(raw: str) -> str:
    digits = _digits(raw)
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    if raw.startswith("+") and 10 <= len(digits) <= 15:
        return "+" + digits
    return ""


def _find_guest_by_email(email: str) -> dict | None:
    email = _extract_email(email)
    if not email:
        return None

    formula = (
        "OR("
        f"LOWER({{Email}})='{email}',"
        f"LOWER({{email}})='{email}',"
        f"LOWER({{customer_email}})='{email}'"
        ")"
    )
    recs = _fetch_all_records(GUESTS_TABLE, formula=formula, max_records=5)
    return recs[0] if recs else None


def _find_guest_by_phone(phone: str) -> dict | None:
    target = _digits(phone)
    if len(target) < 10:
        return None

    last10 = target[-10:]
    last4 = last10[-4:]

    # broad query, exact compare in Python
    formulas = [
        f"FIND('{last4}', {{Phone}}) > 0",
        f"FIND('{last4}', {{phone}}) > 0",
        f"FIND('{last4}', {{customer_phone}}) > 0",
    ]

    candidates: list[dict] = []
    for formula in formulas:
        try:
            candidates.extend(_fetch_all_records(GUESTS_TABLE, formula=formula, max_records=25))
        except Exception:
            pass

    seen = set()
    for rec in candidates:
        if rec["id"] in seen:
            continue
        seen.add(rec["id"])
        f = rec.get("fields") or {}
        for key in ("Phone", "phone", "customer_phone"):
            cand = _digits(_s(f.get(key)))
            if cand[-10:] == last10:
                return rec
    return None


def _invite_is_blocked(invite_fields: dict) -> bool:
    status = _s(invite_fields.get("Invite Status")).lower()
    return status in {"opted out", "attended", "cancelled", "do not send"}


def _score_invite(event_fields: dict, *, subject: str = "", inbound_to_phone: str = "") -> tuple[int, float]:
    score = 0
    subj = _s(subject).lower()

    event_name = _s(event_fields.get("Event Name")).lower()
    model = _s(event_fields.get("Model")).lower()
    store = _s(event_fields.get("Store")).lower()

    if event_name and event_name in subj:
        score += 50
    if model and model in subj:
        score += 20
    if store and store in subj:
        score += 10

    if inbound_to_phone:
        from_map = STORE_TO_SMS_FROM.get(store, "")
        if from_map and _normalize_phone_e164_us(from_map) == _normalize_phone_e164_us(inbound_to_phone):
            score += 100

    event_dt = _parse_dt(event_fields.get("Event Date"))
    ts = event_dt.timestamp() if event_dt else 9999999999.0
    return score, ts


def _find_best_invite_for_guest(guest_id: str, *, subject: str = "", inbound_to_phone: str = "") -> tuple[dict | None, dict | None]:
    invites = _fetch_all_records(INVITES_TABLE, max_records=2000)
    guest_invites = []

    for inv in invites:
        f = inv.get("fields") or {}
        linked_guests = f.get("Guest") or []
        if guest_id not in linked_guests:
            continue
        if _invite_is_blocked(f):
            continue
        guest_invites.append(inv)

    if not guest_invites:
        return None, None

    event_ids = []
    for inv in guest_invites:
        ev = (inv.get("fields") or {}).get("Event") or []
        if ev:
            event_ids.append(ev[0])

    event_map = _fetch_record_map(EVENTS_TABLE, event_ids)

    best_inv = None
    best_event = None
    best_key = None

    for inv in guest_invites:
        ev_ids = (inv.get("fields") or {}).get("Event") or []
        if not ev_ids:
            continue
        ev = event_map.get(ev_ids[0])
        if not ev:
            continue

        ev_fields = ev.get("fields") or {}
        key = _score_invite(ev_fields, subject=subject, inbound_to_phone=inbound_to_phone)
        if best_key is None or key > best_key:
            best_key = key
            best_inv = inv
            best_event = ev

    return best_inv, best_event


def _patch_rsvp_yes(invite_rec: dict, channel: str, body_text: str) -> None:
    _patch_record(
        INVITES_TABLE,
        invite_rec["id"],
        {
            "RSVP": "Yes",
            "Invite Status": "RSVP",
            "Stop Event Nudges": True,
            "Last Response At": _now_iso(),
            "Last Response Channel": channel,
            "Last Response Text": body_text[:1000],
        },
    )


def _patch_rsvp_maybe(invite_rec: dict, channel: str, body_text: str) -> None:
    _patch_record(
        INVITES_TABLE,
        invite_rec["id"],
        {
            "RSVP": "Maybe",
            "Invite Status": "RSVP",
            "Stop Event Nudges": True,
            "Last Response At": _now_iso(),
            "Last Response Channel": channel,
            "Last Response Text": body_text[:1000],
        },
    )


def _patch_rsvp_no(invite_rec: dict, channel: str, body_text: str) -> None:
    _patch_record(
        INVITES_TABLE,
        invite_rec["id"],
        {
            "RSVP": "No",
            "Invite Status": "Cancelled",
            "Stop Event Nudges": True,
            "Last Response At": _now_iso(),
            "Last Response Channel": channel,
            "Last Response Text": body_text[:1000],
        },
    )


def _patch_opt_out_guest(guest_rec: dict, channel: str) -> None:
    patch = {
        "Suppressed": True,
        "Do Not Contact": True,
    }
    if channel == "sms":
        patch["SMS Opt Out"] = True
    else:
        patch["Email Opt Out"] = True
    _patch_record(GUESTS_TABLE, guest_rec["id"], patch)


def _patch_opt_out_invite(invite_rec: dict, channel: str, body_text: str) -> None:
    _patch_record(
        INVITES_TABLE,
        invite_rec["id"],
        {
            "Invite Status": "Opted Out",
            "Stop Event Nudges": True,
            "Last Response At": _now_iso(),
            "Last Response Channel": channel,
            "Last Response Text": body_text[:1000],
        },
    )


def _event_title(event_fields: dict) -> str:
    year = _s(event_fields.get("Model Year"))
    brand = _s(event_fields.get("Brand"))
    model = _s(event_fields.get("Model"))
    if year and brand and model:
        return f"{year} {brand} {model}"
    if brand and model:
        return f"{brand} {model}"
    return _s(event_fields.get("Event Name")) or "the event"


def _event_time(event_fields: dict) -> str:
    start = _s(event_fields.get("Event Start Time"))
    end = _s(event_fields.get("Event End Time"))
    if start and end:
        return f"{start}-{end}"
    return start or end or ""


def _send_sms_confirmation(*, to_number: str, from_number: str, body: str, rooftop_name: str) -> None:
    try:
        send_sms(from_number=from_number, to_number=to_number, body=body, rooftop_name=rooftop_name)
    except Exception:
        log.exception("Event SMS confirmation failed to=%s", to_number)


def _send_email_confirmation(*, to_email: str, subject: str, body_text: str) -> None:
    try:
        send_via_sendgrid(
            to_email=to_email,
            subject=subject,
            body_text=body_text,
            body_html=body_text.replace("\n", "<br>"),
        )
    except Exception:
        log.exception("Event email confirmation failed to=%s", to_email)

def _guest_first_name(guest_fields: dict) -> str:
    return (
        _s(guest_fields.get("First Name"))
        or _s(guest_fields.get("first_name"))
        or _s(guest_fields.get("customer_first_name"))
    )

def _event_handoff_subject(event_fields: dict, guest_fields: dict, reason: str) -> str:
    store = _s(event_fields.get("Store")) or "the store"
    title = _event_title(event_fields)
    guest_name = (
        f"{_guest_first_name(guest_fields)} {_s(guest_fields.get('Last Name'))}".strip()
        or _s(guest_fields.get("Full Name"))
        or "Guest"
    )
    return f"[Patti Event Handoff] {store} — {title} — {guest_name} — {reason}"

def _event_handoff_html(
    *,
    event_fields: dict,
    guest_fields: dict,
    invite_fields: dict,
    channel: str,
    inbound_text: str,
    reason: str,
) -> str:
    store = _s(event_fields.get("Store"))
    title = _event_title(event_fields)
    date_display = _s(event_fields.get("Event Date Display")) or _s(event_fields.get("Event Date"))
    event_time = _event_time(event_fields)
    location = _s(event_fields.get("Event Location"))
    guest_name = (
        f"{_guest_first_name(guest_fields)} {_s(guest_fields.get('Last Name'))}".strip()
        or _s(guest_fields.get("Full Name"))
        or "Guest"
    )
    guest_email = _s(guest_fields.get("Email")) or _s(guest_fields.get("customer_email"))
    guest_phone = _s(guest_fields.get("Phone")) or _s(guest_fields.get("customer_phone"))

    return f"""
    <p><strong>Patti event handoff needed</strong></p>
    <p>
      <strong>Reason:</strong> {reason}<br>
      <strong>Channel:</strong> {channel}<br>
      <strong>Store:</strong> {store}<br>
      <strong>Event:</strong> {title}<br>
      <strong>Date:</strong> {date_display}<br>
      <strong>Time:</strong> {event_time}<br>
      <strong>Location:</strong> {location}
    </p>
    <p>
      <strong>Guest:</strong> {guest_name}<br>
      <strong>Email:</strong> {guest_email or "unknown"}<br>
      <strong>Phone:</strong> {guest_phone or "unknown"}
    </p>
    <p><strong>Guest message:</strong><br>{(inbound_text or "")[:2000]}</p>
    <p><strong>Invite record:</strong> {invite_fields.get("Invite ID") or ""}</p>
    """.strip()

def _notify_event_handoff(
    *,
    event_fields: dict,
    guest_fields: dict,
    invite_rec: dict,
    channel: str,
    inbound_text: str,
    reason: str,
) -> None:
    store = _s(event_fields.get("Store"))
    cfg = get_human_review_config("")
    to_addr = (
        _s(event_fields.get("Handoff Email"))
        or _s(event_fields.get("Salesperson Email"))
        or _s(guest_fields.get("salesperson_email"))
        or cfg.get("fallback_to")
        or (os.getenv("HUMAN_REVIEW_FALLBACK_TO") or "").strip()
    )
    if not to_addr:
        log.warning("Event handoff: no recipient configured")
        return

    cc_addrs = list(cfg.get("cc") or [])
    subject = _event_handoff_subject(event_fields, guest_fields, reason)
    html = _event_handoff_html(
        event_fields=event_fields,
        guest_fields=guest_fields,
        invite_fields=invite_rec.get("fields") or {},
        channel=channel,
        inbound_text=inbound_text,
        reason=reason,
    )

    try:
        send_email_via_outlook(
            to_addr=to_addr,
            subject=subject,
            html_body=html,
            cc_addrs=cc_addrs,
            headers={"X-Patti-Flow": "event_handoff"},
            enforce_compliance=False,
        )
    except Exception:
        log.exception("Event handoff email failed")

def _patch_event_handoff(invite_rec: dict, channel: str, body_text: str, reason: str) -> None:
    _patch_record(
        INVITES_TABLE,
        invite_rec["id"],
        {
            "Needs Human Review": True,
            "Human Review Reason": f"Event handoff: {reason}",
            "Invite Status": "Human Review",
            "Stop Event Nudges": True,
            "Last Response At": _now_iso(),
            "Last Response Channel": channel,
            "Last Response Text": body_text[:1000],
        },
    )

def _reply_subject(subject: str, fallback: str) -> str:
    s = _s(subject)
    if not s:
        return fallback
    if s.lower().startswith("re:"):
        return s
    return f"Re: {s}"

def handle_event_sms_reply(payload_json: dict | None, raw_text: str = "") -> dict:
    payload_json = payload_json or {}

    body = _s(payload_json.get("body") or payload_json.get("text") or "")
    if not body and isinstance(payload_json.get("message"), dict):
        body = _s(payload_json["message"].get("body") or payload_json["message"].get("text"))

    from_phone = _normalize_phone_e164_us(
        _s(payload_json.get("authorPhoneNumber") or payload_json.get("from"))
        or _s((payload_json.get("message") or {}).get("from"))
    )
    to_phone = _normalize_phone_e164_us(
        _s(payload_json.get("ownerPhoneNumber") or payload_json.get("to"))
        or _s((payload_json.get("message") or {}).get("to"))
    )

    if not from_phone or not body:
        return {"handled": False, "reason": "missing_from_or_body"}

    guest_rec = _find_guest_by_phone(from_phone)
    if not guest_rec:
        return {"handled": False, "reason": "no_event_guest_match"}

    invite_rec, event_rec = _find_best_invite_for_guest(
        guest_rec["id"],
        inbound_to_phone=to_phone,
    )
    if not invite_rec or not event_rec:
        return {"handled": False, "reason": "no_open_invite"}

    event_fields = event_rec.get("fields") or {}
    store = _s(event_fields.get("Store")) or "the store"
    title = _event_title(event_fields)
    event_time = _event_time(event_fields)
    event_date = _s(event_fields.get("Event Date Display")) or _s(event_fields.get("Event Date"))
    from_number = os.getenv("EVENT_SMS_FROM_NUMBER") or STORE_TO_SMS_FROM.get(store.lower(), to_phone)

    clean = _strip_reply_text(body)

    if STOP_RE.search(clean):
        _patch_opt_out_guest(guest_rec, "sms")
        _patch_opt_out_invite(invite_rec, "sms", clean)
        _send_sms_confirmation(
            to_number=from_phone,
            from_number=from_number,
            body="Got it — we won't text you again about this event.",
            rooftop_name=store,
        )
        return {"handled": True, "action": "opt_out"}

    if RSVP_YES_RE.search(clean):
        _patch_rsvp_yes(invite_rec, "sms", clean)
        _send_sms_confirmation(
            to_number=from_phone,
            from_number=from_number,
            body=f"Perfect — thanks for letting us know. We look forward to seeing you at {store} for the {title} on {event_date} from {event_time}.",
            rooftop_name=store,
        )
        return {"handled": True, "action": "rsvp_yes"}

    if RSVP_MAYBE_RE.search(clean):
        _patch_rsvp_maybe(invite_rec, "sms", clean)
        _send_sms_confirmation(
            to_number=from_phone,
            from_number=from_number,
            body=f"Thanks — we've marked you as maybe for the {title} at {store}. If your plans firm up, just text YES.",
            rooftop_name=store,
        )
        return {"handled": True, "action": "rsvp_maybe"}

    if RSVP_NO_RE.search(clean):
        _patch_rsvp_no(invite_rec, "sms", clean)
        _send_sms_confirmation(
            to_number=from_phone,
            from_number=from_number,
            body="Understood — thanks for letting us know.",
            rooftop_name=store,
        )
        return {"handled": True, "action": "rsvp_no"}

    guest_fields = guest_rec.get("fields") or {}
    invite_fields = invite_rec.get("fields") or {}

    _patch_event_reply_activity(
        invite_rec,
        "sms",
        clean,
        stop_nudges=True,
        status="Responded",
    )

    decision = generate_event_reply(
        first_name=_guest_first_name(guest_fields),
        event_fields=event_fields,
        guest_message=clean,
        channel="sms",
    )

    reply = (decision.get("reply") or "").strip()
    if not reply:
        return {"handled": False, "reason": "empty_event_reply"}

    _send_sms_confirmation(
        to_number=from_phone,
        from_number=from_number,
        body=reply,
        rooftop_name=store,
    )

    if decision.get("needs_handoff"):
        reason = (decision.get("handoff_reason") or "event_unknown").strip().lower()
        _patch_event_handoff(invite_rec, "sms", clean, reason)
        _notify_event_handoff(
            event_fields=event_fields,
            guest_fields=guest_fields,
            invite_rec=invite_rec,
            channel="sms",
            inbound_text=clean,
            reason=reason,
        )
        return {"handled": True, "action": "handoff", "reason": reason}

    return {"handled": True, "action": "qa_reply"}


def handle_event_email_reply(inbound: dict) -> dict:
    sender_email = _extract_email(inbound.get("from"))
    if not sender_email:
        return {"handled": False, "reason": "missing_sender_email"}

    guest_rec = _find_guest_by_email(sender_email)
    if not guest_rec:
        return {"handled": False, "reason": "no_event_guest_match"}

    subject = _s(inbound.get("subject"))
    body_text = _strip_reply_text(_s(inbound.get("body_text")))
    if not body_text:
        body_text = _strip_reply_text(_s(inbound.get("body_html")))

    invite_rec, event_rec = _find_best_invite_for_guest(
        guest_rec["id"],
        subject=subject,
    )
    if not invite_rec or not event_rec:
        return {"handled": False, "reason": "no_open_invite"}

    event_fields = event_rec.get("fields") or {}
    store = _s(event_fields.get("Store")) or "the store"
    title = _event_title(event_fields)
    event_time = _event_time(event_fields)
    event_date = _s(event_fields.get("Event Date Display")) or _s(event_fields.get("Event Date"))

    if STOP_RE.search(body_text):
        _patch_opt_out_guest(guest_rec, "email")
        _patch_opt_out_invite(invite_rec, "email", body_text)
        _send_email_confirmation(
            to_email=sender_email,
            subject=_reply_subject(subject, "[Event] Opt-out confirmed"),
            body_text="Understood — we won't email you again about this event.",
        )
        return {"handled": True, "action": "opt_out"}

    if RSVP_YES_RE.search(body_text):
        _patch_rsvp_yes(invite_rec, "email", body_text)
        _send_email_confirmation(
            to_email=sender_email,
            subject=f"Re: {subject}" if subject else f"[Event] RSVP confirmed",
            body_text=f"Perfect — thanks for letting us know. We look forward to seeing you at {store} for the {title} on {event_date} from {event_time}.",
        )
        return {"handled": True, "action": "rsvp_yes"}

    if RSVP_MAYBE_RE.search(body_text):
        _patch_rsvp_maybe(invite_rec, "email", body_text)
        _send_email_confirmation(
            to_email=sender_email,
            subject=f"Re: {subject}" if subject else f"[Event] RSVP noted",
            body_text=f"Thanks — we've marked you as maybe for the {title} at {store}. If your plans firm up, just reply YES.",
        )
        return {"handled": True, "action": "rsvp_maybe"}

    if RSVP_NO_RE.search(body_text):
        _patch_rsvp_no(invite_rec, "email", body_text)
        _send_email_confirmation(
            to_email=sender_email,
            subject=f"Re: {subject}" if subject else f"[Event] RSVP updated",
            body_text="Understood — thanks for letting us know.",
        )
        return {"handled": True, "action": "rsvp_no"}

    guest_fields = guest_rec.get("fields") or {}
    invite_fields = invite_rec.get("fields") or {}

    _patch_event_reply_activity(
        invite_rec,
        "email",
        body_text,
        stop_nudges=True,
        status="Responded",
    )

    decision = generate_event_reply(
        first_name=_guest_first_name(guest_fields),
        event_fields=event_fields,
        guest_message=body_text,
        channel="email",
    )

    reply = (decision.get("reply") or "").strip()
    if not reply:
        return {"handled": False, "reason": "empty_event_reply"}

    _send_email_confirmation(
        to_email=sender_email,
        subject=f"Re: {subject}" if subject else f"[Event] {title}",
        body_text=reply,
    )

    if decision.get("needs_handoff"):
        reason = (decision.get("handoff_reason") or "event_unknown").strip().lower()
        _patch_event_handoff(invite_rec, "email", body_text, reason)
        _notify_event_handoff(
            event_fields=event_fields,
            guest_fields=guest_fields,
            invite_rec=invite_rec,
            channel="email",
            inbound_text=body_text,
            reason=reason,
        )
        return {"handled": True, "action": "handoff", "reason": reason}

    return {"handled": True, "action": "qa_reply"}
