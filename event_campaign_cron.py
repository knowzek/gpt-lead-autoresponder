# event_campaign_cron.py
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from html import escape
from typing import Any, Iterable

import requests
from zoneinfo import ZoneInfo

from patti_mailer import send_via_sendgrid
from goto_sms import send_sms
from rooftops import SUBSCRIPTION_TO_ROOFTOP
from patti_common import build_patti_footer


# =========================================================
# CONFIG
# =========================================================
LOG_LEVEL = os.getenv("APP_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("patti.event_campaign")

AIRTABLE_API_TOKEN = (os.getenv("AIRTABLE_API_TOKEN") or "").strip()
AIRTABLE_BASE_ID = (os.getenv("AIRTABLE_BASE_ID") or "").strip()

EVENTS_TABLE = (os.getenv("EVENTS_TABLE_NAME") or "Events").strip()
GUESTS_TABLE = (os.getenv("EVENT_GUESTS_TABLE_NAME") or "Guests").strip()
INVITES_TABLE = (os.getenv("EVENT_INVITES_TABLE_NAME") or "Event Invites").strip()

STORE_TIMEZONE = (os.getenv("STORE_TIMEZONE") or "America/Los_Angeles").strip()
EVENT_CAMPAIGN_DRY_RUN = (os.getenv("EVENT_CAMPAIGN_DRY_RUN") or "0").strip() == "1"
EVENT_CAMPAIGN_MAX_INVITES = int(os.getenv("EVENT_CAMPAIGN_MAX_INVITES") or "1000")
EVENT_CAMPAIGN_VIEW = (os.getenv("EVENT_CAMPAIGN_VIEW") or "").strip()  # optional Airtable view

if not AIRTABLE_API_TOKEN or not AIRTABLE_BASE_ID:
    raise RuntimeError("Missing AIRTABLE_API_TOKEN or AIRTABLE_BASE_ID")

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_TOKEN}",
    "Content-Type": "application/json",
}

DEFAULT_EMAIL_OFFSETS = {1: 14, 2: 2, 3: 1}
DEFAULT_SMS_OFFSETS = {1: 14, 2: 2, 3: 1, 4: 0}

# one-time CX-5 correction + override
CX5_FIX_STORE = "tustin mazda"
CX5_FIX_BRAND = "mazda"
CX5_FIX_MODEL = "cx-5"
CX5_FIX_EVENT_DATE = "2026-03-21"
CX5_FIX_CORRECTION_TAG = "event_correction_20260319_sent"

# Build store -> SMS number map from your existing rooftop config
STORE_TO_SMS_FROM = {
    (rec.get("name") or "").strip().lower(): (rec.get("sms_number") or "").strip()
    for rec in SUBSCRIPTION_TO_ROOFTOP.values()
    if rec.get("name") and rec.get("sms_number")
}


# =========================================================
# AIRTABLE HELPERS
# =========================================================
def _table_url(table_name: str) -> str:
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_name}"


def _request(method: str, url: str, **kwargs) -> dict:
    response = requests.request(method, url, headers=AIRTABLE_HEADERS, timeout=30, **kwargs)
    if response.status_code >= 400:
        raise RuntimeError(f"Airtable {method} failed {response.status_code}: {response.text[:800]}")
    return response.json()


def _fetch_all_records(
    table_name: str,
    *,
    view: str = "",
    formula: str = "",
    max_records: int = 1000,
) -> list[dict]:
    records: list[dict] = []
    offset = None

    while True:
        params: dict[str, Any] = {"pageSize": 100}
        if view:
            params["view"] = view
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
    chunk_size = 10

    for i in range(0, len(ids), chunk_size):
        chunk = ids[i:i + chunk_size]
        formula = "OR(" + ",".join(f"RECORD_ID()='{rid}'" for rid in chunk) + ")"
        for rec in _fetch_all_records(table_name, formula=formula, max_records=chunk_size):
            out[rec["id"]] = rec
    return out


def _patch_record(table_name: str, rec_id: str, fields: dict[str, Any]) -> None:
    _request("PATCH", f"{_table_url(table_name)}/{rec_id}", json={"fields": fields})


# =========================================================
# GENERIC HELPERS
# =========================================================
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now_utc().replace(microsecond=0).isoformat()


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    s = str(value).strip()
    if not s:
        return None

    s = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _to_e164_us(raw: str) -> str:
    raw = (raw or "").strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    if raw.startswith("+") and 10 <= len(digits) <= 15:
        return "+" + digits
    return ""


def _linked_id(value: Any) -> str:
    if isinstance(value, list) and value:
        return str(value[0])
    if isinstance(value, str):
        return value.strip()
    return ""


def _field(record: dict, name: str, default: Any = "") -> Any:
    return (record.get("fields") or {}).get(name, default)


def _boolish(value: Any) -> bool:
    return value is True or str(value).strip().lower() in {"1", "true", "yes"}


def _fmt_time_window(start_time: str, end_time: str) -> str:
    start_time = (start_time or "").strip()
    end_time = (end_time or "").strip()
    if start_time and end_time:
        return f"{start_time}-{end_time}"
    return start_time or end_time or ""


def _event_date_iso(event_fields: dict) -> str:
    raw = str(event_fields.get("Event Date") or "").strip()
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", raw)
    return m.group(1) if m else ""


def _is_date_only_event_value(value: Any) -> bool:
    s = str(value or "").strip()
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", s))


def _event_local_datetime(event_fields: dict) -> datetime | None:
    """
    Treat date-only Event Date values as STORE_TIMEZONE local midnight,
    not UTC midnight.
    """
    raw = event_fields.get("Event Date")
    tz = ZoneInfo(STORE_TIMEZONE)

    if _is_date_only_event_value(raw):
        y, m, d = map(int, str(raw).strip().split("-"))
        return datetime(y, m, d, 0, 0, 0, tzinfo=tz)

    dt = _parse_dt(raw)
    if not dt:
        return None

    return dt.astimezone(tz)


def _is_target_cx5_event(event_fields: dict) -> bool:
    store = str(event_fields.get("Store") or "").strip().lower()
    brand = str(event_fields.get("Brand") or "").strip().lower()
    model = str(event_fields.get("Model") or "").strip().lower()
    event_date = _event_date_iso(event_fields)

    return (
        store == CX5_FIX_STORE
        and brand == CX5_FIX_BRAND
        and model == CX5_FIX_MODEL
        and event_date == CX5_FIX_EVENT_DATE
    )


def _hardcoded_cx5_day_of_send_at_utc() -> datetime:
    local_dt = datetime(2026, 3, 21, 8, 30, 0, tzinfo=ZoneInfo(STORE_TIMEZONE))
    return local_dt.astimezone(timezone.utc)


def _append_result_tag(existing: str, tag: str) -> str:
    existing = (existing or "").strip()
    if tag in existing:
        return existing
    return f"{existing} | {tag}".strip(" |")


def _send_cx5_correction_now_if_needed(invite_id: str, invite_fields: dict, event_fields: dict, guest_fields: dict) -> bool:
    """
    One-time correction SMS for people who already got the wrong 'tomorrow' text
    on Thursday 3/19 for the Saturday 3/21 event.
    Uses Last Send Result as the idempotency marker so repeated cron runs today
    do not resend it.
    """
    if not _is_target_cx5_event(event_fields):
        return False

    now_local = _now_utc().astimezone(ZoneInfo(STORE_TIMEZONE))
    if now_local.date() != date(2026, 3, 19):
        return False

    invite_status = str(invite_fields.get("Invite Status") or "").strip().lower()
    if invite_status in {"opted out", "attended", "cancelled", "do not send"}:
        return False

    if _boolish(guest_fields.get("Suppressed")) or _boolish(guest_fields.get("Do Not Contact")):
        return False

    if _boolish(guest_fields.get("SMS Opt Out")):
        return False

    if not invite_fields.get("SMS 3 Sent At"):
        return False

    if CX5_FIX_CORRECTION_TAG in str(invite_fields.get("Last Send Result") or ""):
        return False

    phone = _to_e164_us((guest_fields.get("Phone") or guest_fields.get("customer_phone") or "").strip())
    if not phone:
        return False

    store_name = (event_fields.get("Store") or guest_fields.get("Store") or "").strip()
    override_from = (os.getenv("EVENT_SMS_FROM_NUMBER") or "").strip()
    if override_from:
        from_number = override_from
    else:
        from_number = STORE_TO_SMS_FROM.get(store_name.lower(), "")

    if not from_number:
        log.warning("Correction SMS skipped invite=%s reason=missing_store_sms_number store=%r", invite_id, store_name)
        return False

    title = _event_title(event_fields)
    date_display = (event_fields.get("Event Date Display") or "Saturday, March 21").strip()
    time_window = _fmt_time_window(event_fields.get("Event Start Time", ""), event_fields.get("Event End Time", ""))
    poster_url = (event_fields.get("Poster Image URL") or event_fields.get("Hero Image URL") or "").strip()
    url_part = f" Details: {poster_url}" if poster_url else ""

    body = (
        f"Quick correction: our {title} event is "
        f"{date_display} from {time_window}, not tomorrow. Sorry for the confusion. "
        f"Reply YES if you plan to attend.{url_part}"
    )

    if EVENT_CAMPAIGN_DRY_RUN:
        log.info("DRY RUN correction sms invite=%s to=%s body=%r", invite_id, phone, body)
        ok = True
        result = "dry_run"
    else:
        resp = send_sms(
            from_number=from_number,
            to_number=phone,
            body=body,
            rooftop_name=store_name,
        )
        ok = not resp.get("blocked")
        result = "sent" if ok else (resp.get("reason") or "sms_blocked")

    patch = {
        "Last Send Attempt At": _now_iso(),
        "Last Send Channel": "sms",
        "Last Send Result": _append_result_tag(str(invite_fields.get("Last Send Result") or ""), CX5_FIX_CORRECTION_TAG if ok else f"{CX5_FIX_CORRECTION_TAG}_failed"),
        "Last Send Error": "" if ok else result,
    }
    if ok:
        patch["Last SMS Sent At"] = _now_iso()

    _patch_record(INVITES_TABLE, invite_id, patch)
    log.info("CX5 correction sms invite=%s ok=%s result=%s", invite_id, ok, result)
    return ok
    
# =========================================================
# COPY / TEMPLATES
# =========================================================
def _event_title(event: dict) -> str:
    brand = (event.get("Brand") or "").strip()
    model = (event.get("Model") or "").strip()
    model_year = (event.get("Model Year") or "").strip()

    if model_year and brand and model:
        return f"{model_year} {brand} {model}"
    if brand and model:
        return f"{brand} {model}"
    return (event.get("Event Name") or "our event").strip()


def _event_benefits(event: dict) -> list[str]:
    custom = [
        (event.get("Benefit 1") or "").strip(),
        (event.get("Benefit 2") or "").strip(),
        (event.get("Benefit 3") or "").strip(),
        (event.get("Benefit 4") or "").strip(),
    ]
    custom = [x for x in custom if x]
    if custom:
        return custom

    model = (event.get("Model") or "").strip().lower()
    brand = (event.get("Brand") or "").strip().lower()

    if brand == "mazda" and "cx-5" in model:
        return [
            "See the all-new CX-5 before most shoppers do",
            "Get familiar with the redesign, technology, and size updates",
            "Take a test drive while launch inventory is fresh",
            "Enjoy Chick-fil-A breakfast or lunch while you're here",
        ]

    if brand == "kia" and "telluride" in model:
        return [
            "Get an early look at the all-new Telluride",
            "Compare the new styling, features, and hybrid updates",
            "Take a drive and get your questions answered in person",
            "Enjoy food on us while you’re here",
        ]

    return [
        "Get an early look at the latest model",
        "Take a test drive and see it in person",
        "Enjoy food on us while you’re here",
    ]


def build_event_email(event: dict, guest: dict, template_no: int) -> dict[str, str]:
    first_name = (guest.get("First Name") or "there").strip()
    store = (event.get("Store") or "Patterson Autos").strip()
    title = _event_title(event)
    date_display = (event.get("Event Date Display") or event.get("Event Date") or "").strip()
    time_window = _fmt_time_window(event.get("Event Start Time", ""), event.get("Event End Time", ""))
    location = (event.get("Event Location") or store).strip()
    poster_url = (event.get("Poster Image URL") or event.get("Hero Image URL") or "").strip()
    rsvp_url = (
        (event.get("RSVP URL") or "").strip()
        or (event.get("Calendly URL") or "").strip()
        or (event.get("Landing Page URL") or "").strip()
    )
    benefits = _event_benefits(event)

    subject_override = (event.get(f"Email Subject {template_no}") or "").strip()
    preheader_override = (event.get(f"Email Preheader {template_no}") or "").strip()

    defaults = {
        1: {
            "subject": f"Be among the first to drive the {title}",
            "preheader": f"Join us at {store} on {date_display} for an exclusive launch event.",
            "opener": (
                f"We're excited to invite you to an exclusive preview of the {title} at {store}. "
                "This is a Patterson customer event, and we'd love to have you there before the general rush starts."
            ),
            "closer": (
                "This is a relaxed drop-in event, but if you plan to attend it helps us plan food and vehicles. "
                "Just reply YES to this email and we'll mark you down."
            ),
        },
        2: {
            "subject": f"You're invited: {title} launch event at {store}",
            "preheader": f"Reminder: see and drive the {title} on {date_display}.",
            "opener": (
                f"Just a quick reminder that our {title} launch event is coming up at {store}. "
                "If this is a model you've been curious about, this is the easiest time to come see it, drive it, and ask questions in person."
            ),
            "closer": "If you think you'll stop by, just reply YES so we can expect you.",
        },
        3: {
            "subject": f"Tomorrow morning: experience the {title} at {store}",
            "preheader": "Final reminder for tomorrow's Patterson customer event.",
            "opener": (
                f"Our {title} event at {store} is tomorrow, and I wanted to send one last reminder in case you meant to stop by. "
                "It's a simple, low-pressure way to see the vehicle in person and take a drive while inventory is still arriving."
            ),
            "closer": "If you plan to come, just reply YES and we'll be ready for you.",
        },
    }

    chosen = defaults[template_no]
    subject = subject_override or chosen["subject"]
    preheader = preheader_override or chosen["preheader"]
    opener = chosen["opener"]
    closer = chosen["closer"]

    bullet_html = "".join(f"<li style='margin:0 0 8px 0;'>{escape(item)}</li>" for item in benefits)
    hero_html = (
        f"<img src='{escape(poster_url)}' alt='{escape(title)}' style='width:100%;max-width:640px;display:block;border:0;border-radius:10px;'>"
        if poster_url else ""
    )

    signature_html = build_patti_footer(store)

    body_html = f"""
    <!doctype html>
    <html>
      <body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,Helvetica,sans-serif;color:#111111;">
        <div style="display:none;max-height:0;overflow:hidden;opacity:0;">{escape(preheader)}</div>
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f4f4f4;">
          <tr>
            <td align="center" style="padding:24px 12px;">
              <table role="presentation" width="640" cellspacing="0" cellpadding="0" style="width:100%;max-width:640px;background:#ffffff;border-radius:12px;overflow:hidden;">
                <tr><td style="padding:0;">{hero_html}</td></tr>
                <tr>
                  <td style="padding:28px 32px;">
                    <div style="font-size:12px;letter-spacing:1.2px;text-transform:uppercase;color:#b01d24;font-weight:700;margin-bottom:10px;">A Patterson customer exclusive</div>
                    <h1 style="margin:0 0 14px 0;font-size:30px;line-height:1.15;">Be among the first to experience the {escape(title)}</h1>
                    <div style="width:60px;height:4px;background:#b01d24;margin:0 0 18px 0;border-radius:2px;"></div>
                    <p style="margin:0 0 18px 0;font-size:16px;line-height:1.6;">Hi {escape(first_name)},</p>
                    <p style="margin:0 0 18px 0;font-size:16px;line-height:1.6;">{escape(opener)}</p>
                    <table role="presentation" cellspacing="0" cellpadding="0" style="margin:0 0 22px 0;background:#f7f7f7;border-radius:10px;width:100%;border:1px solid #ececec;">
                      <tr>
                        <td style="padding:20px 22px;font-size:15px;line-height:1.7;">
                          <strong>{escape(title)}</strong><br>
                          <strong>{escape(date_display)}</strong>{('<br>' + escape(time_window)) if time_window else ''}<br>
                          {escape(location)}
                        </td>
                      </tr>
                    </table>
                    <ul style="padding-left:18px;margin:0 0 22px 0;font-size:16px;line-height:1.6;">{bullet_html}</ul>
                    <p style="margin:0 0 22px 0;font-size:16px;line-height:1.6;">{escape(closer)}</p>
                  </td>
                </tr>
                <tr>
                  <td style="padding:0 32px 32px 32px;">
                    {signature_html}
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>
      </body>
    </html>
    """.strip()

    bullet_text = "\n".join(f"- {item}" for item in benefits)
    body_text = (
        f"Hi {first_name},\n\n"
        f"{opener}\n\n"
        f"{title}\n{date_display}\n{time_window}\n{location}\n\n"
        f"{bullet_text}\n\n"
        f"{closer}\n"
        + (f"\nReserve here: {rsvp_url}\n" if rsvp_url else "")
    )

    return {
        "subject": subject,
        "body_html": body_html,
        "body_text": body_text,
    }


def build_event_sms(event: dict, guest: dict, template_no: int) -> str:
    custom = (event.get(f"SMS Copy {template_no}") or "").strip()
    if custom:
        return custom

    first_name = (guest.get("First Name") or "").strip()
    prefix = f"Hi {first_name}, " if first_name else "Hi, "
    store = (event.get("Store") or "Patterson Autos").strip()
    title = _event_title(event)
    date_display = (event.get("Event Date Display") or event.get("Event Date") or "").strip()
    time_window = _fmt_time_window(event.get("Event Start Time", ""), event.get("Event End Time", ""))
    poster_url = (event.get("Poster Image URL") or event.get("Hero Image URL") or "").strip()
    url_part = f" Details: {poster_url}" if poster_url else ""

    defaults = {
        1: f"{prefix}this is Patti from {store}. You're invited to our {title} launch at {store} on {date_display} from {time_window}. Stop by anytime to see it and take a drive. Reply YES if you plan to attend.{url_part}",
        2: f"{prefix}this is Patti from {store}. Quick reminder about our {title} event at {store} on {date_display} from {time_window}. Food is on us and vehicles will be ready. Reply YES if you think you'll stop by.{url_part}",
        3: f"{prefix}Patti from {store}. Our {title} event at {store} is tomorrow from {time_window}. If you plan to come by, reply YES and we'll be ready for you.{url_part}",
        4: f"{prefix}can't wait to see you at the {title} event today at {store}. We're here from {time_window}. Stop by anytime — the vehicle, food, and team will be ready for you.{url_part}",
    }
    return defaults[template_no]


# =========================================================
# SEND LOGIC
# =========================================================
class SendPlan:
    def __init__(self, channel: str, step_no: int, sent_field: str, explicit_send_field: str, offset_field: str, default_offset_days: int):
        self.channel = channel
        self.step_no = step_no
        self.sent_field = sent_field
        self.explicit_send_field = explicit_send_field
        self.offset_field = offset_field
        self.default_offset_days = default_offset_days


SEND_PLANS = [
    SendPlan("email", 1, "Email 1 Sent At", "Email 1 Send At", "Email 1 Offset Days", DEFAULT_EMAIL_OFFSETS[1]),
    SendPlan("email", 2, "Email 2 Sent At", "Email 2 Send At", "Email 2 Offset Days", DEFAULT_EMAIL_OFFSETS[2]),
    SendPlan("email", 3, "Email 3 Sent At", "Email 3 Send At", "Email 3 Offset Days", DEFAULT_EMAIL_OFFSETS[3]),
    SendPlan("sms", 1, "SMS 1 Sent At", "SMS 1 Send At", "SMS 1 Offset Days", DEFAULT_SMS_OFFSETS[1]),
    SendPlan("sms", 2, "SMS 2 Sent At", "SMS 2 Send At", "SMS 2 Offset Days", DEFAULT_SMS_OFFSETS[2]),
    SendPlan("sms", 3, "SMS 3 Sent At", "SMS 3 Send At", "SMS 3 Offset Days", DEFAULT_SMS_OFFSETS[3]),
    SendPlan("sms", 4, "SMS Day Of Sent At", "SMS Day Of Send At", "SMS Day Of Offset Days", DEFAULT_SMS_OFFSETS[4]),
]


def _effective_send_at(event_fields: dict, explicit_field: str, offset_field: str, default_offset_days: int) -> datetime | None:
    explicit_dt = _parse_dt(event_fields.get(explicit_field))
    if explicit_dt:
        return explicit_dt

    # hardcode this one specific Saturday CX-5 day-of text
    if explicit_field == "SMS Day Of Send At" and _is_target_cx5_event(event_fields):
        return _hardcoded_cx5_day_of_send_at_utc()

    event_local_dt = _event_local_datetime(event_fields)
    if not event_local_dt:
        return None

    try:
        offset_days = int(event_fields.get(offset_field) or default_offset_days)
    except Exception:
        offset_days = default_offset_days

    send_local_dt = event_local_dt - timedelta(days=offset_days)

    # If Event Date is date-only, anchor to local send time on the intended local day.
    if _is_date_only_event_value(event_fields.get("Event Date")):
        hour = 7 if default_offset_days == 0 else 9
        minute = 30 if default_offset_days == 0 else 0
        send_local_dt = send_local_dt.replace(
            hour=hour,
            minute=minute,
            second=0,
            microsecond=0,
        )
        return send_local_dt.astimezone(timezone.utc)

    # existing fallback for timestamp-ish values that land at midnight
    if send_local_dt.hour == 0 and send_local_dt.minute == 0:
        hour = 7 if default_offset_days == 0 else 9
        minute = 30 if default_offset_days == 0 else 0
        send_local_dt = send_local_dt.replace(
            hour=hour,
            minute=minute,
            second=0,
            microsecond=0,
        )
        return send_local_dt.astimezone(timezone.utc)

    return send_local_dt.astimezone(timezone.utc)


def _due_actions(invite_fields: dict, event_fields: dict) -> list[SendPlan]:
    now = _now_utc()
    due: list[SendPlan] = []

    invite_status = str(invite_fields.get("Invite Status") or "").strip().lower()
    rsvp = str(invite_fields.get("RSVP") or "").strip().lower()
    stop_nudges = _boolish(invite_fields.get("Stop Event Nudges"))

    # Hard skip statuses
    if invite_status in {"opted out", "attended", "cancelled", "do not send"}:
        return []

    for plan in SEND_PLANS:
        if invite_fields.get(plan.sent_field):
            continue

        # Step 4 = day-of reminder only to RSVP Yes
        if plan.channel == "sms" and plan.step_no == 4:
            if rsvp != "yes":
                continue
        else:
            # Once someone RSVPs yes/maybe/no or we explicitly stop nudges, stop the regular nudges
            if stop_nudges or rsvp in {"yes", "maybe", "no"}:
                continue

        send_at = _effective_send_at(
            event_fields,
            plan.explicit_send_field,
            plan.offset_field,
            plan.default_offset_days,
        )
        if send_at and send_at <= now:
            due.append(plan)

    return due


def _send_email(event_fields: dict, guest_fields: dict, step_no: int) -> tuple[bool, str]:
    to_email = (guest_fields.get("Email") or guest_fields.get("customer_email") or "").strip()
    if not to_email:
        return False, "missing_email"

    msg = build_event_email(event_fields, guest_fields, step_no)

    if EVENT_CAMPAIGN_DRY_RUN:
        log.info("DRY RUN email step=%s to=%s subject=%r", step_no, to_email, msg["subject"])
        return True, "dry_run"

    ok = send_via_sendgrid(
        to_email=to_email,
        subject=msg["subject"],
        body_html=msg["body_html"],
        body_text=msg["body_text"],
    )
    return (True, "sent") if ok else (False, "sendgrid_failed")


def _send_text(event_fields: dict, guest_fields: dict, step_no: int) -> tuple[bool, str]:
    phone = _to_e164_us((guest_fields.get("Phone") or guest_fields.get("customer_phone") or "").strip())
    if not phone:
        return False, "missing_phone"

    store_name = (event_fields.get("Store") or guest_fields.get("Store") or "").strip()

    override_from = (os.getenv("EVENT_SMS_FROM_NUMBER") or "").strip()
    if override_from:
        from_number = override_from
    else:
        from_number = STORE_TO_SMS_FROM.get(store_name.lower(), "")
    
    if not from_number:
        return False, f"missing_store_sms_number:{store_name}"

    body = build_event_sms(event_fields, guest_fields, step_no)

    if EVENT_CAMPAIGN_DRY_RUN:
        log.info("DRY RUN sms step=%s from=%s to=%s body=%r", step_no, from_number, phone, body)
        return True, "dry_run"

    resp = send_sms(
        from_number=from_number,
        to_number=phone,
        body=body,
        rooftop_name=store_name,
    )
    if resp.get("blocked"):
        return False, resp.get("reason") or "sms_blocked"
    return True, "sent"


def _mark_result(invite_id: str, plan: SendPlan, ok: bool, result: str) -> None:
    patch = {
        "Last Send Attempt At": _now_iso(),
        "Last Send Channel": plan.channel,
        "Last Send Result": result,
        "Last Send Error": "" if ok else result,
    }
    if ok:
        patch[plan.sent_field] = _now_iso()
        if plan.channel == "email":
            patch["Last Email Sent At"] = _now_iso()
        else:
            patch["Last SMS Sent At"] = _now_iso()

    _patch_record(INVITES_TABLE, invite_id, patch)


def _is_suppressed(guest_fields: dict) -> tuple[bool, str]:
    if _boolish(guest_fields.get("Suppressed")):
        return True, "suppressed"
    if _boolish(guest_fields.get("Do Not Contact")):
        return True, "do_not_contact"
    return False, ""


# =========================================================
# MAIN RUNNER
# =========================================================
def run_event_campaigns_once() -> None:
    invites = _fetch_all_records(
        INVITES_TABLE,
        view=EVENT_CAMPAIGN_VIEW,
        max_records=EVENT_CAMPAIGN_MAX_INVITES,
    )
    if not invites:
        log.info("No event invites found.")
        return

    event_ids = {_linked_id(_field(inv, "Event")) for inv in invites}
    guest_ids = {_linked_id(_field(inv, "Guest")) for inv in invites}

    event_map = _fetch_record_map(EVENTS_TABLE, event_ids)
    guest_map = _fetch_record_map(GUESTS_TABLE, guest_ids)

    processed = 0
    sent = 0
    failed = 0

    for invite in invites:
        processed += 1
        invite_id = invite["id"]
        invite_fields = invite.get("fields") or {}

        if _boolish(invite_fields.get("Cancelled")):
            continue
        if str(invite_fields.get("Invite Status") or "").strip().lower() in {"cancelled", "do not send"}:
            continue

        event_rec = event_map.get(_linked_id(invite_fields.get("Event")))
        guest_rec = guest_map.get(_linked_id(invite_fields.get("Guest")))

        if not event_rec or not guest_rec:
            _patch_record(INVITES_TABLE, invite_id, {
                "Last Send Attempt At": _now_iso(),
                "Last Send Error": "missing_linked_event_or_guest",
            })
            failed += 1
            continue

        event_fields = event_rec.get("fields") or {}
        guest_fields = guest_rec.get("fields") or {}

        suppressed, reason = _is_suppressed(guest_fields)
        if suppressed:
            _patch_record(INVITES_TABLE, invite_id, {
                "Invite Status": "Suppressed",
                "Last Send Attempt At": _now_iso(),
                "Last Send Error": reason,
            })
            continue

        event_fields = event_rec.get("fields") or {}
        guest_fields = guest_rec.get("fields") or {}

        suppressed, reason = _is_suppressed(guest_fields)
        if suppressed:
            _patch_record(INVITES_TABLE, invite_id, {
                "Invite Status": "Suppressed",
                "Last Send Attempt At": _now_iso(),
                "Last Send Error": reason,
            })
            continue

        # one-time correction for the bad "tomorrow" SMS sent on 3/19
        try:
            _send_cx5_correction_now_if_needed(
                invite_id=invite_id,
                invite_fields=invite_fields,
                event_fields=event_fields,
                guest_fields=guest_fields,
            )
        except Exception:
            log.exception("CX5 correction send failed invite=%s", invite_id)

        due = _due_actions(invite_fields, event_fields)
        if not due:
            continue

        for plan in due:
            if plan.channel == "email" and _boolish(guest_fields.get("Email Opt Out")):
                _mark_result(invite_id, plan, False, "email_opt_out")
                failed += 1
                continue

            if plan.channel == "sms" and _boolish(guest_fields.get("SMS Opt Out")):
                _mark_result(invite_id, plan, False, "sms_opt_out")
                failed += 1
                continue

            try:
                if plan.channel == "email":
                    ok, result = _send_email(event_fields, guest_fields, plan.step_no)
                else:
                    ok, result = _send_text(event_fields, guest_fields, plan.step_no)

                _mark_result(invite_id, plan, ok, result)

                if ok:
                    sent += 1
                else:
                    failed += 1

            except Exception as exc:
                log.exception("Event campaign send failed invite=%s channel=%s step=%s", invite_id, plan.channel, plan.step_no)
                _mark_result(invite_id, plan, False, str(exc)[:250])
                failed += 1

    log.info("Event campaign run complete processed=%s sent=%s failed=%s", processed, sent, failed)


if __name__ == "__main__":
    run_event_campaigns_once()
