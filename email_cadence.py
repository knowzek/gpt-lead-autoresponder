import os
import logging
from datetime import datetime, timezone, timedelta

from airtable_store import list_records_by_view, patch_by_id
from templates import build_mazda_loyalty_email
from patti_mailer import send_via_sendgrid

log = logging.getLogger("patti.email.cadence")

EMAIL_DUE_VIEW = os.getenv("EMAIL_DUE_VIEW", "Email Due")

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def send_email_cadence_once():
    records = list_records_by_view(EMAIL_DUE_VIEW, max_records=50)

    MAX_EMAIL_DAY = int(os.getenv("MAZDA_MAX_EMAIL_DAY", "10"))

    for r in records:
        rid = r.get("id")
        f = r.get("fields") or {}

        to_email = (f.get("customer_email") or f.get("email") or "").strip()
        if not to_email:
            try:
                patch_by_id(rid, {
                    "email_status": "paused",
                    "next_email_at": None,
                })
            except Exception:
                log.exception("Email cadence: failed missing-email pause rid=%s", rid)
            continue

        day = int(f.get("email_day") or 1)

        # Hard stop: if somehow this record already advanced past the final day,
        # do not send again.
        if day > MAX_EMAIL_DAY:
            try:
                patch_by_id(rid, {
                    "email_status": "complete",
                    "next_email_at": None,
                })
            except Exception:
                log.exception("Email cadence: failed terminal-complete patch rid=%s", rid)
            continue

        msg = build_mazda_loyalty_email(day=day, fields=f)
        ok = send_via_sendgrid(
            to_email=to_email,
            subject=msg["subject"],
            body_html=msg["body_html"],
            body_text=msg.get("body_text"),
        )
        if not ok:
            continue

        now_iso = _now_iso()
        next_day = day + 1

        # If this was the final scheduled email, stop cadence here.
        if day >= MAX_EMAIL_DAY:
            patch_by_id(rid, {
                "last_email_at": now_iso,
                "last_email_subject": msg["subject"],
                "last_email_body": msg.get("body_text") or "",
                "email_day": next_day,
                "next_email_at": None,
                "email_status": "complete",
            })
        else:
            next_iso = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat()
            patch_by_id(rid, {
                "last_email_at": now_iso,
                "last_email_subject": msg["subject"],
                "last_email_body": msg.get("body_text") or "",
                "email_day": next_day,
                "next_email_at": next_iso,
                "email_status": "ready",
            })
