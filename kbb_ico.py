# kbb_ico.py
from datetime import datetime as _dt, timezone as _tz, timedelta as _td
from kbb_templates import TEMPLATES, fill_merge_fields
from kbb_cadence import events_for_day
from fortellis import (
    add_opportunity_comment,
    send_opportunity_email_activity as _crm_send_opportunity_email_activity,
    schedule_activity,
)
from fortellis import complete_activity

from outlook_email import send_email_via_outlook
from fortellis import search_activities_by_opportunity
from helpers import build_calendar_links
import json, re
from crm_logging import log_email_to_crm
STATE_TAG = "[PATTI_KBB_STATE]"  # marker to find the state comment quickly

import os
TEST_TO = os.getenv("TEST_TO", "pattiautoresponder@gmail.com")
import logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
import re as _re
from textwrap import dedent as _dd
from rooftops import ROOFTOP_INFO

import textwrap as _tw
import zoneinfo as _zi

from html import unescape as _unesc

#from rooftops import ROOFTOP_INFO as ROOFTOP_INFO

# Only flip to Outlook for test opps / email mode on this branch
TEST_EMAIL_OPP_IDS = {
    "050a81e9-78d4-f011-814f-00505690ec8c",  # your current test
    "e7f79ae6-0cb9-f011-814f-00505690ec8c",
}
EMAIL_MODE = os.getenv("EMAIL_MODE", "crm")  # "crm" or "outlook"

def _crm_appt_set(opportunity: dict) -> bool:
    status = (opportunity.get("salesStatus") or opportunity.get("sales_status") or "")
    status = status.strip().lower()
    return status in {"appointment set", "appt set", "appointment scheduled"}


def expand_legacy_schedule_token_for_outlook(body_html: str, rooftop_name: str) -> str:
    """
    Fortellis expands <{LegacySalesApptSchLink}> inside the CRM.
    Outlook/email-outside-CRM will NOT, so we replace it with a real URL
    based on rooftop_name (ROOFTOP_INFO).
    """
    body_html = body_html or ""
    rt = (ROOFTOP_INFO.get(rooftop_name) or {})
    booking_link = (rt.get("booking_link") or rt.get("scheduler_url") or "").strip()

    # If we don't have a booking link, leave token alone (or pick a safe fallback).
    if not booking_link:
        return body_html

    # Replace the raw token wherever it appears
    return re.sub(
        r"(?i)<\{LegacySalesApptSchLink\}>",
        f'<a href="{booking_link}">Schedule Your Visit</a>',
        body_html,
    )

def wants_kbb_value(text: str) -> bool:
    """
    Heuristic: does the customer seem to be asking for their KBB estimate / offer amount?
    """
    if not text:
        return False

    t = text.lower()

    # Must be about KBB / offer context
    has_kbb_context = any(
        phrase in t
        for phrase in [
            "kbb",
            "kelley blue book",
            "instant cash offer",
            "cash offer",
            "offer",
        ]
    )

    # Must be about the amount / value / price
    has_amount_context = any(
        phrase in t
        for phrase in [
            "estimate",
            "amount",
            "value",
            "price",
            "how much",
            "what was",
            "what is",
        ]
    )

    return has_kbb_context and has_amount_context


def _clean_html(h: str) -> str:
    """
    Lightweight HTML → text for logging to CRM.
    """
    h = h or ""
    # strip tags
    h = _TAGS_RE.sub(" ", h) if "_TAGS_RE" in globals() else re.sub(r"<[^>]+>", " ", h)
    # unescape & collapse whitespace
    h = _unesc(h)
    h = re.sub(r"\s+", " ", h).strip()
    return h


def send_opportunity_email_activity(
    token,
    subscription_id,
    opp_id,
    sender,
    recipients,
    carbon_copies,
    subject,
    body_html,
    rooftop_name,
    reply_to_activity_id=None,
):
    """
    KBB wrapper:
      - If EMAIL_MODE == "outlook": send from Patti Outlook + log to CRM
        AND create a "Send Email" completed activity (stops response-time clock).
      - Else: send via Fortellis /sendEmail (original behavior).
    """
    from fortellis import complete_activity  # exists in your fortellis module (used elsewhere)

    # Default CRM behavior if not in Outlook mode
    if EMAIL_MODE != "outlook":
        return _crm_send_opportunity_email_activity(
            token,
            subscription_id,
            opp_id,
            sender=sender,
            recipients=recipients,
            carbon_copies=carbon_copies,
            subject=subject,
            body_html=body_html,
            rooftop_name=rooftop_name,
            reply_to_activity_id=reply_to_activity_id,
        )

    # 📨 Outlook path (NO Fortellis email send)
    to_addr = recipients[0] if recipients else None
    if not to_addr:
        return

    from datetime import datetime, timezone

# ✅ Replace CRM-only token with rooftop-specific schedule URL
body_html = expand_legacy_schedule_token_for_outlook(body_html, rooftop_name)

# 1) Send from Patti via Power Automate / Outlook
send_email_via_outlook(
    to_addr=to_addr,
    subject=subject,
    html_body=body_html,
    headers={"X-Opportunity-ID": opp_id},
)

# 2) Log back to CRM as a NOTE (visibility)
if token and subscription_id:
    try:
        preview = _clean_html(body_html)[:500]
        add_opportunity_comment(
            token,
            subscription_id,
            opp_id,
            f"Outbound email (Patti Outlook) to {to_addr}: {subject}\n\n{preview}",
        )
    except Exception as e:
        log.warning("Failed to add CRM comment for Outlook send opp=%s: %s", opp_id, e)

    # 3) ✅ Create a "Send Email" COMPLETED ACTIVITY (stops the response clock)
    # IMPORTANT: This does NOT send an email — it only records the activity in eLead.
    if token and subscription_id:
        try:
            # dealer_key must match what your _headers(dealer_key, token) expects.
            # If your _headers() expects a rooftop key (NOT subscription_id), do this:
            rt = get_rooftop_info(subscription_id) or {}
            dealer_key = rt.get("rooftop_key") or rt.get("key") or rt.get("rooftop")  # <-- pick the correct field
    
            if not dealer_key:
                raise RuntimeError("Missing dealer_key/rooftop_key for complete_activity()")
    
            now_z = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
            complete_activity(
                token,
                dealer_key,                 # ✅ NOT subscription_id (unless your _headers actually wants sub id)
                opp_id,
                due_dt_iso_utc=now_z,
                completed_dt_iso_utc=now_z,
                activity_name="Send Email",
                activity_type="send email",  # or 3; string is safer with your _coerce_activity_type()
                comments=f"Patti Outlook: sent to {to_addr} | subject={subject}",
            )
    
            log.info("Completed 'Send Email' activity in CRM opp=%s", opp_id)
    
        except Exception as e:
            # Make this loud while testing so we SEE why it's not creating the activity
            log.exception("Failed to complete 'Send Email' activity opp=%s: %s", opp_id, e)



def _es_debug_update(esClient, index, id, doc, tag=""):
    try:
        log.debug("[ES%s] update start id=%s doc_keys=%s", f":{tag}" if tag else "", id, list(doc.keys()))
        from es_resilient import es_update_with_retry as _esu
        r = _esu(esClient, index=index, id=id, doc=doc)
        log.debug("[ES%s] update via es_update_with_retry OK id=%s", f":{tag}" if tag else "", id)
        return r
    except Exception as e1:
        log.warning("[ES%s] es_update_with_retry failed, falling back: %s", f":{tag}" if tag else "", e1)
        try:
            r2 = esClient.update(index=index, id=id, body={"doc": doc, "doc_as_upsert": True})
            log.debug("[ES%s] direct update OK id=%s result=%s", f":{tag}" if tag else "", id, getattr(r2, "result", "n/a"))
            return r2
        except Exception as e2:
            log.error("[ES%s] direct update failed id=%s error=%s", f":{tag}" if tag else "", id, e2)
            raise


def _can_email(state: dict) -> bool:
    return not state.get("email_blocked_do_not_email") and state.get("mode") not in {"closed_declined"}

def _patch_address_placeholders(html: str, rooftop_name: str) -> str:
    addr = ((ROOFTOP_INFO.get(rooftop_name) or {}).get("address") or "").strip()
    if not addr:
        return html
    # catch common placeholder variants
    pat = _re.compile(r'\[\s*(?:insert\s+)?address\s*\]|\{address\}|\<address\>', _re.I)
    return pat.sub(addr, html)


# === Decline detection ==========================================================

_DECLINE_RE = _re.compile(
    r'(?i)\b('
    r'not\s+interested|no\s+longer\s+interested|not\s+going\s+to\s+sell|'
    r'stop\s+email|do\s+not\s+contact|please\s+stop|unsubscribe|'
    r'take\s+me\s+off|remove\s+me|leave me alone|bought elsewhere|already purchased'
    r')\b'
)
def _is_decline(text: str) -> bool:
    return bool(_DECLINE_RE.search(text or ""))



def _is_optout_text(t: str) -> bool:
    t = (t or "").lower()
    return any(kw in t for kw in (
        "stop emailing me", "stop email", "do not email", "don't email",
        "unsubscribe", "remove me", "no further contact",
        "stop contacting", "opt out", "opt-out", "optout", "cease and desist"
    ))

def _latest_customer_optout(opportunity):
    """
    Return (found: bool, ts_iso: str|None, txt: str|None) for the newest customer msg
    that contains an opt-out phrase, regardless of what came after.
    """
    msgs = (opportunity.get("messages") or [])
    latest = None
    for m in reversed(msgs):
        if m.get("msgFrom") == "customer" and _is_optout_text(m.get("body")):
            # use message date if present, else None
            latest = (True, m.get("date"), m.get("body"))
            break
    return latest or (False, None, None)


_GMAIL_QUOTE_RE = _re.compile(r'(?is)<div[^>]*class="gmail_quote[^"]*"[^>]*>.*$', _re.M)
_BLOCKQUOTE_RE  = _re.compile(r'(?is)<blockquote[^>]*>.*$', _re.M)
_TAGS_RE        = _re.compile(r'(?is)<[^>]+>')

import re as _re2

# Detect any existing booking token/link so we don't double insert
_SCHED_ANY_RE = _re2.compile(r'(?is)(LegacySalesApptSchLink|Schedule\s+Your\s+Visit</a>)')

def _is_agent_send(act: dict) -> bool:
    nm = (act.get("activityName") or act.get("name") or "").strip().lower()
    at = str(act.get("activityType") or "").strip().lower()
    return ("send email" in nm) or (at == "14" or act.get("activityType") == 14)

def _last_agent_send_dt(acts: list[dict]):
    latest = None
    for a in acts or []:
        if not _is_agent_send(a):
            continue
        dt = _activity_dt(a)
        if dt and (latest is None or dt > latest):
            latest = dt
    return latest


def append_soft_schedule_sentence(body_html: str, rooftop_name: str) -> str:
    """
    Ensures we add exactly one polite schedule sentence with a real link or Legacy token.
    Will not add if a booking token/link already exists.
    """
    body_html = body_html or ""

    # If they already have a token or a Schedule Your Visit link, skip
    if _SCHED_ANY_RE.search(body_html):
        return body_html

    # Resolve the href (real scheduler if configured; else Legacy token so CRM swaps it)
    rt = (ROOFTOP_INFO.get(rooftop_name) or {})
    href = rt.get("booking_link") or rt.get("scheduler_url") or "<{LegacySalesApptSchLink}>"

    soft_line = (
        '<p>Let me know a time that works for you, or schedule directly here: '
        '<{LegacySalesApptSchLink}></p>'
    )

    # If body has paragraphs, append after them; else wrap
    if _re2.search(r'(?is)<p[^>]*>.*?</p>', body_html):
        return body_html.rstrip() + soft_line
    return f"<p>{body_html.strip()}</p>{soft_line}" if body_html.strip() else soft_line


def _norm_iso_utc(x):
    try:
        dt = _dt.fromisoformat(str(x).replace("Z", "+00:00")).astimezone(_tz.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None

def _short_circuit_if_booked(opportunity, acts_live, state,
                             *, token, subscription_id, rooftop_name, SAFE_MODE, rooftop_sender):
    """
    If we see a 'Customer Scheduled Appointment':
      - NEW: send confirmation + flip subStatus, persist state → (True, True)
      - SAME: do nothing (no mode flip) → (True, False)
    If none found: (False, False)
    """
    opp_id      = opportunity.get("opportunityId") or opportunity.get("id")
    customer_id = (opportunity.get("customer") or {}).get("id")

    appt_id, appt_due_iso = _find_new_customer_scheduled_appt(
        acts_live, state,
        token=token, subscription_id=subscription_id,
        opp_id=opp_id, customer_id=customer_id
    )
                                 
    state = state or {}
    action_taken = False
    did_send = False
    # Build a quick index of current appointment activity ids returned by CRM
    def _appt_ids_from(acts):
        items = []
        if isinstance(acts, dict):
            for key in ("scheduledActivities", "completedActivities", "items", "activities"):
                items.extend(acts.get(key) or [])
        else:
            items = acts or []
    
        out = set()
        for a in items:
            raw_type = a.get("activityType") or a.get("type")
            raw_name = a.get("activityName") or a.get("name")
            t = str(raw_type).strip().lower() if raw_type is not None else ""
            n = str(raw_name).strip().lower() if raw_name is not None else ""
    
            if ("appointment" in t) or ("appointment" in n) or (raw_type == 7) or (t == "7"):
                aid = a.get("activityId") or a.get("id")
                if aid:
                    out.add(str(aid))
        return out


    
    current_appt_ids = _appt_ids_from(acts_live)
    
    prev_id  = (state or {}).get("last_appt_activity_id")
    prev_due = (state or {}).get("appt_due_utc")
    
    # If ES points to an appointment that no longer exists in CRM, reconcile silently.
    # If ES points to an appointment id that no longer exists in CRM, reconcile silently.
    if prev_id and prev_id not in current_appt_ids and appt_id and appt_due_iso:
        new_due_norm = _norm_iso_utc(appt_due_iso)
        if prev_due and new_due_norm and prev_due == new_due_norm:
            log.info("KBB ICO: reconciling stale ES appt id (%s → %s) for same due; no resend.",
                     prev_id, appt_id)
            state["last_appt_activity_id"] = appt_id
            opportunity["_kbb_state"] = state
            try:
                from esQuerys import esClient
                from es_resilient import es_update_with_retry
                es_update_with_retry(esClient, index="opportunities", id=opp_id,
                                     doc={"_kbb_state": state})
            except Exception as e:
                log.warning("ES persist of _kbb_state failed (reconcile): %s", e)
            return (True, False)



    # 1) Nothing booked → no short-circuit and DO NOT touch state
    if not (appt_id and appt_due_iso):
        return (False, False)
    
    # Normalize due time once
    new_due_norm = _norm_iso_utc(appt_due_iso)
    
    # Capture previous markers BEFORE mutating state
    prev_id   = state.get("last_appt_activity_id")
    prev_due  = state.get("appt_due_utc")
    last_conf = state.get("last_confirmed_due_utc")
    
    # 🔒 HARD RULE: appointment exists → scheduled mode immediately
    state["last_appt_activity_id"] = appt_id
    state["appt_due_utc"] = new_due_norm
    state["mode"] = "scheduled"
    
    log.info(
        "Idempotency check → prev_id=%r prev_due=%r last_confirmed_due=%r :: new_id=%r new_due=%r",
        prev_id, prev_due, last_conf, appt_id, new_due_norm
    )
    
    same_id   = prev_id == appt_id
    same_due  = prev_due == new_due_norm
    same_conf = last_conf == new_due_norm
    
    def _parse_utc(x):
        try:
            return _dt.fromisoformat(str(x).replace("Z", "+00:00")).astimezone(_tz.utc)
        except Exception:
            return None
    
    within_2m = False
    pdt, ndt = _parse_utc(prev_due), _parse_utc(new_due_norm)
    if pdt and ndt:
        within_2m = abs((ndt - pdt).total_seconds()) <= 120
    
    already_done = same_id or same_due or same_conf or within_2m
    
    if already_done:
        log.info("KBB ICO: already acknowledged → skip resend")
        try:
            from esQuerys import esClient
            from es_resilient import es_update_with_retry
            es_update_with_retry(
                esClient,
                index="opportunities",
                id=opp_id,
                doc={"_kbb_state": state}
            )
        except Exception as e:
            log.warning("ES persist failed (already_done): %s", e)
        return (True, False)


    # Format time
    try:
        dt_local = _dt.fromisoformat(str(appt_due_iso).replace("Z", "+00:00"))
    except Exception:
        dt_local = None

    if dt_local and dt_local.tzinfo:
        appt_human     = _fmt_local_human(dt_local, tz_name="America/Los_Angeles")
        due_dt_iso_utc = dt_local.astimezone(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        appt_human     = str(appt_due_iso)
        due_dt_iso_utc = _norm_iso_utc(appt_due_iso) or _dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Deterministic thanks email
    cust_first = (opportunity.get('customer', {}) or {}).get('firstName') or "there"
    subject = f"Re: Appointment confirmed for {appt_human}"

    rt = (ROOFTOP_INFO.get(rooftop_name) or {})
    summary     = f"{rooftop_name} – KBB Inspection"
    location    = rt.get("address") or rooftop_name
    description = "15–20 minute in-person inspection to finalize your Kelley Blue Book® Instant Cash Offer."
    links       = build_calendar_links(summary, description, location, due_dt_iso_utc, duration_min=30)

    add_to_cal_html = f"""
      <p style="margin:16px 0 8px 0;">Add to calendar:</p>
      <p>
        <a href="{links['google']}">Google</a> &nbsp;|&nbsp;
        <a href="{links['outlook']}">Outlook</a> &nbsp;|&nbsp;
        <a href="{links['yahoo']}">Yahoo</a>
      </p>
    """.strip()

    body_html = f"""
        <p>Hi {cust_first},</p>
        <p>Thanks for booking — we’ll see you on <strong>{appt_human}</strong> at {rooftop_name}.</p>
        {add_to_cal_html}
        <p>Please bring your title, ID, and keys. If you need to change your time, use this link: <{{LegacySalesApptSchLink}}></p>
    """.strip()

    body_html = normalize_patti_body(body_html)
    body_html = _patch_address_placeholders(body_html, rooftop_name)
    body_html = _PREFS_RE.sub("", body_html).strip()
    body_html = body_html + build_patti_footer(rooftop_name)
    if not subject.lower().startswith("re:"):
        subject = "Re: " + subject

    # Resolve recipient
    cust = (opportunity.get("customer") or {})
    email = cust.get("emailAddress") or ((cust.get("emails") or [{}])[0].get("address"))
    if not email:
        email = (opportunity.get("_lead", {}) or {}).get("email_address")
    recipients = [email] if (email and not SAFE_MODE) else [TEST_TO]
    if not recipients:
        log.warning("No recipient; skip send for opp=%s", opp_id)
        return (True, False)  # handled, but no send

    # --- PERSIST FIRST so re-runs short-circuit even if send fails ---
    now_iso = _dt.now(_tz.utc).isoformat()
    state["mode"]                  = "scheduled"
    state["last_appt_activity_id"] = appt_id
    state["appt_due_utc"]          = due_dt_iso_utc
    state["appt_due_local"]        = appt_human
    state["nudge_count"]           = 0
    state["last_agent_msg_at"]     = now_iso
    # (do NOT set last_confirm* until send succeeds)
    opportunity["_kbb_state"]      = state
    
    # Persist state to ES (pre-send) — MAKE THIS FAIL-HARD
    try:
        from esQuerys import esClient
        from es_resilient import es_update_with_retry
        es_update_with_retry(
            esClient,
            index="opportunities",
            id=opp_id,
            doc={"_kbb_state": state}
        )
        log.info("Persisted _kbb_state to ES for opp=%s (pre-send confirm)", opp_id)
    except Exception as e:
        log.error(
            "❌ Pre-send persist FAILED for opp=%s: %s — aborting confirmation send to avoid loops",
            opp_id, e
        )
        # Do NOT send the email if we couldn't persist idempotency markers
        return (True, False)



    # Flip CRM subStatus → Appointment Set (appointment exists regardless of email)
    try:
        from fortellis import set_opportunity_substatus
        resp = set_opportunity_substatus(token, subscription_id, opp_id, sub_status="Appointment Set")
        log.info("SubStatus update response: %s", getattr(resp, "status_code", "n/a"))
        action_taken = True  # we did update CRM even if we don’t send an email
    except Exception as e:
        log.warning("set_opportunity_substatus failed: %s", e)

    # --- SEND with DoNotEmail handling ---
    did_send = False
    try:
        if not _can_email(state):
            log.info("Email suppressed by state for opp=%s", opp_id)
            opportunity["_kbb_state"] = state
            return (True, False)


        send_opportunity_email_activity(
            token, subscription_id, opp_id,
            sender=rooftop_sender,
            recipients=recipients,
            carbon_copies=[],
            subject=subject, body_html=body_html, rooftop_name=rooftop_name
        )
        did_send = True
        state["last_confirmed_due_utc"] = due_dt_iso_utc
        state["last_confirm_sent_at"]   = _dt.now(_tz.utc).isoformat()
        opportunity["_kbb_state"]       = state
        try:
            es_update_with_retry(
                esClient,
                index="opportunities",
                id=opp_id,
                doc={"_kbb_state": state},
            )
            log.info("Persisted _kbb_state to ES for opp=%s (post-send confirm)", opp_id)
        except Exception as e:
            log.warning("ES persist of _kbb_state failed (post-send confirm): %s", e)

    except Exception as e:
        # Detect Fortellis DoNotEmail 400 and swallow (state already persisted → no loop)
        resp = getattr(e, "response", None)
        body = ""
        if resp is not None:
            try:
                body = resp.json()
            except Exception:
                body = resp.text
        body_str = str(body)
        if "SendEmailInvalidRecipient" in body_str and "DoNotEmail" in body_str:
            log.warning("KBB ICO: DoNotEmail — skipping confirmation email for opp %s.", opp_id)
            try:
                from fortellis import add_opportunity_comment
                add_opportunity_comment(
                    token, subscription_id, opp_id,
                    comment="Auto-confirmation not sent: customer marked DoNotEmail."
                )
            except Exception as ee:
                log.warning("Failed to add DoNotEmail comment: %s", ee)
        else:
            log.error("Confirm send failed for opp %s: %s", opp_id, e)
            state["last_confirm_error"] = body_str[:500]
            state["last_confirm_attempt_at"] = _dt.now(_tz.utc).isoformat()
            try:
                es_update_with_retry(
                    esClient,
                    index="opportunities",
                    id=opp_id,
                    doc={"_kbb_state": state},
                )
            except Exception:
                pass

    return (True, did_send)


def _latest_read_email_id(acts: list[dict]) -> str | None:
    newest = None
    newest_dt = None
    for a in acts or []:
        if not _is_read_email(a):
            continue
        dt = _activity_dt(a)
        if dt and (newest_dt is None or dt > newest_dt):
            newest_dt = dt
            newest = str(a.get("activityId") or a.get("id") or "")
    return newest

def _top_reply_only(html: str) -> str:
    """Strip quoted thread and return the customer's fresh reply (first paragraph)."""
    if not html:
        return ""
    s = html
    s = _GMAIL_QUOTE_RE.sub("", s)   # remove Gmail quoted thread
    s = _BLOCKQUOTE_RE.sub("", s)    # remove generic blockquotes
    # keep just the first <div>/<p>…</div></p> or line before a double break
    s = s.split("<br><br>", 1)[0]
    # fallback: remove tags and trim
    s = _TAGS_RE.sub(" ", s)
    s = _re.sub(r'\s+', ' ', _unesc(s)).strip()
    # be conservative: cap length
    return s[:500]

def _has_upcoming_appt(acts_live, state: dict) -> bool:
    """
    Returns True if there is an Appointment activity due in the future (not completed),
    or if state['mode']=='scheduled' and appt_due_utc is still in the future.
    """
    now_utc = _dt.now(_tz.utc)

    # Normalize acts_live to a flat list
    buckets = []
    if isinstance(acts_live, dict):
        for key in ("scheduledActivities", "items", "activities", "completedActivities"):
            buckets.extend(acts_live.get(key) or [])
    else:
        buckets = acts_live or []

    for a in buckets:
        raw_name = a.get("activityName") or a.get("name")
        nm = str(raw_name).strip().lower() if raw_name is not None else ""
        raw_type = a.get("activityType") or a.get("type")
        t = str(raw_type).strip().lower() if raw_type is not None else ""
        cat = str(a.get("category") or "").strip().lower()
        due = a.get("dueDate") or a.get("completedDate") or a.get("activityDate")
        try:
            due_dt = _dt.fromisoformat(str(due).replace("Z", "+00:00"))
        except Exception:
            due_dt = None

        # be flexible: match "Appointment" anywhere in name or type, plus numeric 7 if present
        is_appt = ("appointment" in nm) or ("appointment" in t) or (t == "7") or (t == 7)
        not_completed = cat != "completed"

        if is_appt and due_dt and due_dt > now_utc and not_completed:
            return True

    # B) Fall back to state
    appt_due_utc = (state or {}).get("appt_due_utc")
    if appt_due_utc:
        try:
            d = _dt.fromisoformat(str(appt_due_utc).replace("Z","+00:00"))
            if d > now_utc:
                return True
        except Exception:
            pass

    return False


def _find_new_customer_scheduled_appt(acts_live, state, *, token=None, subscription_id=None,
                                      opp_id=None, customer_id=None):
    """
    Return (activity_id, due_iso) for a new 'Customer Scheduled Appointment'
    or any scheduled Appointment-type activity we haven't seen before.
    Falls back to fetching full activity-history if the provided object
    doesn't expose the scheduled bucket.
    """
    last_seen = (state or {}).get("last_appt_activity_id")

    def _matches(a: dict, bucket: str | None = None) -> bool:
        raw_name = a.get("activityName") or a.get("name") or ""
        nm = str(raw_name).strip().lower()
    
        raw_type = a.get("activityType") or a.get("type") or ""
        t = str(raw_type).strip().lower()
    
        # If this item came from scheduledActivities, it’s scheduled by definition.
        in_scheduled_bucket = (bucket or "").lower() == "scheduledactivities"
    
        # Appointment-ish detection (covers "KBB ICO Appointment" and future variants)
        is_apptish = ("appointment" in nm) or ("appointment" in t) or (str(a.get("activityType")) == "7")
    
        # Tenant-specific phrase (keep, but not required)
        is_customer_sched = "customer scheduled" in nm
    
        if in_scheduled_bucket and is_apptish:
            return True
    
        # Older payloads might include category (optional fallback)
        cat = str(a.get("category") or "").strip().lower()
        is_generic_appt = is_apptish and (cat == "scheduled")
    
        return is_customer_sched or is_generic_appt
    
    
    def _scan(items, bucket: str | None = None):
        for a in items or []:
            if not _matches(a, bucket=bucket):
                continue
            aid = str(a.get("activityId") or a.get("id") or "")
            if not aid:
                continue
            due = a.get("dueDate") or a.get("completedDate") or a.get("activityDate")
            return aid, due
        return None, None


    # 1) Scan the snapshot we were given
    if isinstance(acts_live, dict):
        for key in ("scheduledActivities", "items", "activities", "completedActivities"):
            appt_id, due = _scan(acts_live.get(key) or [], bucket=key)
            if appt_id:
                return appt_id, due
    elif isinstance(acts_live, list):
        appt_id, due = _scan(acts_live)
        if appt_id:
            return appt_id, due

    # 2) Fallback: pull a fresh activity-history so we can see scheduledActivities for sure
    if token and subscription_id and opp_id and customer_id:
        try:
            from fortellis import get_activities
            fresh = get_activities(opp_id, customer_id, token, subscription_id) or {}
            try:
                log.info(
                    "KBB ICO: get_activities fallback for opp=%s shape=%s keys=%s",
                    opp_id,
                    type(fresh).__name__,
                    list(fresh.keys()) if isinstance(fresh, dict) else None,
                )
                # 🔍 DEBUG: inspect scheduled vs completed activities from Fortellis
                sa = (fresh or {}).get("scheduledActivities") if isinstance(fresh, dict) else None
                ca = (fresh or {}).get("completedActivities") if isinstance(fresh, dict) else None

                
                log.info(
                    "KBB acts_raw scheduledActivities=%s completedActivities=%s",
                    len(sa or []) if sa is not None else "n/a",
                    len(ca or []) if ca is not None else "n/a"
                )
                
                if sa:
                    log.info("KBB acts_raw first scheduledActivity keys=%s", list((sa[0] or {}).keys()))
                    log.info("KBB acts_raw first scheduledActivity=%r", sa[0])

            except Exception:
                pass

            if isinstance(fresh, dict):
                for key in ("scheduledActivities", "items", "activities", "completedActivities"):
                    appt_id, due = _scan(fresh.get(key) or [], bucket=key)

                    if appt_id:
                        return appt_id, due
            elif isinstance(fresh, list):
                appt_id, due = _scan(fresh)
                if appt_id:
                    return appt_id, due
        except Exception as e:
            try:
                log.warning("KBB ICO: fallback get_activities failed for opp %s: %s", opp_id, e)
            except Exception:
                pass

    return None, None



def _fmt_local_human(dt: _dt, tz_name="America/Los_Angeles") -> str:
    """
    Return 'Friday, Nov 14 at 12:00 PM' in the rooftop's local timezone.
    """
    try:
        z = _zi.ZoneInfo(tz_name)
        local = dt.astimezone(z)
    except Exception:
        local = dt
    time_str = local.strftime("%I:%M %p").lstrip("0")
    return f"{local.strftime('%A')}, {local.strftime('%b')} {local.day} at {time_str}"


_PREFS_RE = _re.compile(r'(?is)\s*to stop receiving these messages.*?(?:</p>|$)')

# === Live activity helpers (Fortellis) =========================================
def _fetch_activities_live(opp_id: str, customer_id: str | None, token: str, subscription_id: str, page_size: int = 100) -> list[dict]:
    try:
        return search_activities_by_opportunity(
            opportunity_id=opp_id,
            token=token,
            dealer_key=subscription_id,
            page=1,
            page_size=page_size,
            customer_id=customer_id,
        ) or []
    except Exception as e:
        log.warning("Fortellis activities fetch failed: %s", e)
        return []

def _is_read_email(act: dict) -> bool:
    nm = (act.get("activityName") or act.get("name") or "").strip().lower()
    at = act.get("activityType")
    return (nm == "read email") or (at == 20)

def _activity_dt(act: dict):
    # Prefer completedDate; fall back to created/modified
    ts = (act.get("completedDate")
          or act.get("createdDate")
          or act.get("modifiedDate")
          or "")
    try:
        return _dt.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def _has_new_read_email_since(acts: list[dict], since_dt):
    # If we've never sent anything yet (since_dt is None), the first inbound counts.
    for a in acts:
        if not _is_read_email(a):
            continue
        adt = _activity_dt(a)
        if adt and (since_dt is None or adt > since_dt):
            return True
    return False

def build_patti_footer(rooftop_name: str) -> str:
    rt = (ROOFTOP_INFO.get(rooftop_name) or {})

    img_url      = rt.get("signature_img") or "https://www.pattersonautos.com/blogs/7684/wp-content/uploads/2025/11/image.png"
    patti_email  = rt.get("patti_email")   or "patti@pattersonautos.com"
    dealer_site  = (rt.get("website") or "https://www.pattersonautos.com").rstrip("/")
    dealer_addr  = rt.get("address")       or ""
    logo_alt     = f"Patti | {rooftop_name}"

    clean_site = dealer_site.replace("https://", "").replace("http://", "")

    return f"""
    <table width="650" border="0" cellspacing="0" cellpadding="0" style="margin-top:18px;border-collapse:collapse;">
      <tr>
        <td style="padding:12px 16px;border:1px solid #e2e2e2;border-radius:4px;background-color:#fafafa;">
          <table width="100%" border="0" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
            <tr>
              <!-- LEFT: image + text block -->
              <td width="260" valign="top" align="left" style="padding-right:16px;">
                <table border="0" cellspacing="0" cellpadding="0" width="100%" style="border-collapse:collapse;">
                  <tr>
                    <td align="left" valign="top" style="padding-bottom:8px;">
                      <img src="{img_url}"
                           alt="{logo_alt}"
                           width="240"
                           border="0"
                           style="display:block;height:auto;max-width:240px;">
                    </td>
                  </tr>
                  <tr>
                    <!-- Fallback text still shows even if image is blocked -->
                    <td align="left" valign="top" style="font-family:Arial, Helvetica, sans-serif;font-size:13px;line-height:18px;color:#333333;">
                      <strong>Patti</strong><br>
                      Virtual Assistant | {rooftop_name}
                    </td>
                  </tr>
                </table>
              </td>

              <!-- RIGHT: contact block -->
              <td width="360" valign="top" align="left" style="font-family:Arial, Helvetica, sans-serif; color:#222222;">
                <div style="font-size:13px; line-height:20px; font-weight:bold; margin-bottom:2px;">
                  Contact details
                </div>
                <div style="font-size:12px; line-height:18px; color:#666666; margin-bottom:8px;">
                  You can reply directly to this email or use the details below.
                </div>

                <div style="font-size:13px; line-height:20px; margin-bottom:8px;">
                  <div><strong>Email:</strong>
                    <a href="mailto:{patti_email}" style="color:#0066cc; text-decoration:none;">{patti_email}</a>
                  </div>
                  <div><strong>Website:</strong>
                    <a href="{dealer_site}" style="color:#0066cc; text-decoration:none;">{clean_site}</a>
                  </div>
                </div>

                <div style="font-size:13px; line-height:20px; color:#333333;">
                  <div>{rooftop_name}</div>
                  <div>{dealer_addr}</div>
                </div>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
    """.strip()



def normalize_patti_body(body_html: str) -> str:
    """Tidy GPT output: strip stray Patti signatures and collapse whitespace."""
    body_html = _re.sub(r'(?is)(?:\n\s*)?patti\s*(?:<br/?>|\r?\n)+.*?$', '', body_html.strip())
    # collapse double spaces around <p> boundaries
    body_html = _re.sub(r'\n{2,}', '\n', body_html)
    return body_html


def compose_kbb_convo_body(
    rooftop_name: str,
    cust_first: str,
    customer_message: str,
    booking_link_text: str = "Schedule Your Visit",
):
    return _tw.dedent(f"""
    You are Patti, the virtual assistant for {rooftop_name}. This thread is about a Kelley Blue Book® Instant Cash Offer (ICO).

    Write your reply as short, warm, human HTML using simple <p> paragraphs (no lists).

    Very important behavior rules:
    - Begin with: "Hi {cust_first}," (exactly) as the first line.
    - FIRST, understand what the customer actually asked or said.
    - DIRECTLY answer the customer's question or request in plain language.
      * If they ask things like "When can I come in?" or "What time works?" you must:
        - Explain how and when they can visit (e.g., "You can bring your vehicle in during our normal business hours; just tell me what day/time works best for you.")
        - Invite them to share a specific day and time that works for them.
        - You do NOT need to know exact store hours; if you don't know, say something like
          "Let me know a day and time that works for you and we'll confirm it."
      * If they ask you to remind them of their Kelley Blue Book® estimate (for example "What was the KBB estimate you gave me?"):
        - ONLY mention a specific dollar amount if you can actually see a real dollar amount (like "$12,345") in your context or KBB data.
        - If you do NOT see a real amount anywhere, DO NOT make one up. Instead say that you don’t see the exact figure yet and that the team will confirm the value when they inspect the vehicle at the visit.
        - Never invent or guess a dollar amount.
    - Mention the Kelley Blue Book® Instant Cash Offer only when it helps answer the question (don’t force it).
    - Avoid re-explaining general information you already covered earlier; if they repeat something, confirm briefly and move on.
    - Do NOT just send a generic "thank you for your interest" note. The reply should feel like you're responding to THIS message, not restarting the conversation.
    - You may remind them to bring title, ID, and keys if appropriate, and if you haven't already done it in an earlier message.
    - Respond ONLY to the customer's latest message shown below. Do not answer earlier questions unless the customer repeats them in this message.
    - Do not quote or restate the customer's message verbatim; just answer it.
    - No extra signatures; we will append yours.
    - Keep to 2–4 short paragraphs max. Do not stop after only a greeting.


    Customer said:
    \"\"\"{customer_message}\"\"\"

    Produce only the HTML body (no subject).
    """).strip()

_CTA_ANCHOR_RE = _re.compile(r'(?is)<a[^>]*>\s*Schedule\s+Your\s+Visit\s*</a>')
_RAW_TOKEN_RE  = _re.compile(r'(?i)<\{LegacySalesApptSchLink\}>')

_ANY_SCHED_LINE_RE = _re.compile(
    r"(?is)"
    r"(?:"
    r"(?:feel\s+free\s+to\s+)?let\s+me\s+know\s+(?:a\s+)?(?:day\s+and\s+)?time\s+that\s+works[^\.!\?]*[\.!\?]?"
    r"|please\s+let\s+us\s+know\s+a\s+convenient\s+time[^\.!\?]*[\.!\?]?"
    r"|schedule\s+directly[^\.!\?]*[\.!\?]?"
    r"|reserve\s+your\s+time[^\.!\?]*[\.!\?]?"
    r"|schedule\s+(?:an\s+)?appointment[^\.!\?]*[\.!\?]?"
    r"|schedule\s+your\s+visit[^\.!\?]*[\.!\?]?"
    r")"
)



def enforce_standard_schedule_sentence(body_html: str) -> str:
    """Ensure exactly one standard CTA appears above visit/closing lines."""
    if not body_html:
        body_html = ""

    # 0) Normalize whitespace a bit so paragraph regex works better
    body_html = re.sub(r'\s+', ' ', body_html).strip()

    standard_html = (
        '<p>Please let us know a convenient time for you, or you can instantly reserve your time here: '
        '<{LegacySalesApptSchLink}></p>'
    )

    # 1) Remove any <p> paragraphs that already contain scheduling verbiage or the CRM token
    #    (so we don't leave behind "instantly here: ." fragments)
    PARA = r'(?is)<p[^>]*>.*?</p>'
    SCHED_PAT = r'(?i)(LegacySalesApptSchLink|reserve your time|schedule (an )?appointment|schedule your visit)'
    def _kill_sched_paras(m):
        para = m.group(0)
        return '' if re.search(SCHED_PAT, para) else para

    body_html = re.sub(PARA, _kill_sched_paras, body_html).strip()

    # 2) Split into paragraphs to position the CTA
    parts = re.findall(PARA, body_html)  # list of <p>…</p>
    if not parts:
        parts = [f"<p>{body_html}</p>"]  # fallback if model didn't use <p> tags

    # Insert CTA before the first visit/closing paragraph; else prepend
    insert_at = None
    for i, p in enumerate(parts):
        if re.search(r'(?i)(ready to visit|bring|looking forward)', p):
            insert_at = i
            break
    if insert_at is None:
        insert_at = 0
    parts.insert(insert_at, standard_html)

    # 3) Join and ensure we don't have duplicate CTAs
    combined = ''.join(parts)
    combined = re.sub(r'(?is)(<p>[^<]*LegacySalesApptSchLink[^<]*</p>)(.*?)\1', r'\1\2', combined).strip()
    return combined



_LEGACY_TOKEN_RE = _re.compile(r"(?i)<\{LegacySalesApptSchLink\}>")

def render_booking_cta(rooftop_name: str, link_text: str = "Schedule Your Visit") -> str:
    rt = (ROOFTOP_INFO.get(rooftop_name) or {})
    booking_link = rt.get("booking_link") or rt.get("scheduler_url") or ""
    if booking_link:
        return f'<p><a href="{booking_link}">{link_text}</a></p>'
    # leave the CRM token so eLeads can expand it
    return f'<p><a href="<{{LegacySalesApptSchLink}}>">{link_text}</a></p>'


def replace_or_append_booking_cta(body_html: str, rooftop_name: str, channel: str = "fortellis") -> str:
    rt = (ROOFTOP_INFO.get(rooftop_name) or {})
    booking_link = rt.get("booking_link") or rt.get("scheduler_url") or ""

    # If Fortellis is the sender, do NOT expand the token yourself.
    if channel.lower() == "fortellis":
        return body_html  # leave <{LegacySalesApptSchLink}> intact

    # 1) Token present? Replace with a proper anchor if we’re sending directly.
    if _LEGACY_TOKEN_RE.search(body_html):
        if booking_link:
            return _LEGACY_TOKEN_RE.sub(
                f'<a href="{booking_link}">Schedule Your Visit</a>', body_html
            )
        return _LEGACY_TOKEN_RE.sub(
            '<a href="<{LegacySalesApptSchLink}>">Schedule Your Visit</a>', body_html
        )

    # 2) First plain "Schedule Your Visit" → wrap
    if ("Schedule Your Visit" in body_html and
        not re.search(r'(?i)<a[^>]*>\s*Schedule Your Visit\s*</a>', body_html)):
        href = booking_link or '<{LegacySalesApptSchLink}>'
        return re.sub(r"Schedule Your Visit", f'<a href="{href}">Schedule Your Visit</a>', body_html, count=1)

    # 3) Append a proper linked CTA block.
    return body_html.rstrip() + render_booking_cta(rooftop_name)



ALLOW_TEXTING = os.getenv("ALLOW_TEXTING","0").lower() in ("1","true","yes")

def _ico_offer_expired(created_iso: str, exclude_sunday: bool = True) -> bool:
    if not created_iso:
        return False
    try:
        created = _dt.fromisoformat(created_iso.replace("Z","+00:00")).astimezone(_tz.utc)
    except Exception:
        return False
    days = 7
    if exclude_sunday:
        # count 7 calendar days; Sunday still exists but your email copy says “excluding Sunday”
        pass
    return _dt.now(_tz.utc) > (created + _td(days=days))


def _load_state_from_comments(opportunity) -> dict:
    """
    Load KBB convo/cadence state. Prefer the ES-stored copy (_kbb_state),
    then merge any tagged comment state (if present) without replacing keys.
    """
    # A) Preferred: ES-backed state (no CRM noise)
    state = dict(opportunity.get("_kbb_state") or {})
    if not state:
        state = {"mode": "cadence", "last_customer_msg_at": None, "last_agent_msg_at": None}

    # B) Optional merge from a tagged activity comment (if one exists)
    comments = opportunity.get("messages") or opportunity.get("completedActivitiesTesting") or []
    for c in comments:
        txt = (c.get("comments") or c.get("notes") or "") or ""
        if STATE_TAG in txt:
            try:
                loaded = json.loads(_re.sub(r".*?\[PATTI_KBB_STATE\]\s*", "", txt, flags=_re.S))
                # merge, but don't blow away existing ES state
                for k, v in (loaded or {}).items():
                    state.setdefault(k, v)
            except Exception:
                pass
            break

    return state


def _save_state_comment(token, subscription_id, opportunity_id, state: dict):
    if not opportunity_id:
        log.warning("skip state comment: missing opportunity_id")
        return
    payload = f"{STATE_TAG} {json.dumps(state, ensure_ascii=False)}"
    #add_opportunity_comment(token, subscription_id, opportunity_id, payload)


def customer_has_replied(
    opportunity: dict,
    token: str,
    subscription_id: str,
    state: dict | None = None,
    acts: list[dict] | dict | None = None,   # NEW optional arg
):
    """
    Returns (has_replied, last_customer_ts_iso, last_inbound_activity_id)

    Only returns True for a *new* inbound since the state's last seen
    inbound id/timestamp.

    If `acts` is provided, it must be the same structure returned by
    search_activities_by_opportunity (list or dict); in that case we
    will NOT make an extra API call.
    """
    state = state or {}
    last_seen_ts = (state.get("last_customer_msg_at") or "").strip()
    last_seen_id = (state.get("last_inbound_activity_id") or "").strip()

    opportunity_id = opportunity.get("opportunityId") or opportunity.get("id")
    customer = (opportunity.get("customer") or {})
    customer_id = (customer.get("id") or "").strip()
    if not opportunity_id:
        log.error("customer_has_replied: missing opportunity_id")
        return False, None, None

    # ✅ Only hit Fortellis if caller didn’t pass a snapshot
    if acts is None:
        acts = search_activities_by_opportunity(
            opportunity_id=opportunity_id,
            token=token,
            dealer_key=subscription_id,
            page=1,
            page_size=200,
            customer_id=customer_id or None,
        ) or []

    # Some orgs return dict shapes; normalize to list if so
    if isinstance(acts, dict):
        # prefer completedActivities if present
        acts = (acts.get("completedActivities") or acts.get("items") or [])

    # newest → oldest by best available timestamp
    def _ts_str(a: dict) -> str:
        return (a.get("completedDate") or a.get("createdDate") or
                a.get("createdOn") or a.get("modifiedDate") or "").strip()

    acts = sorted(acts, key=_ts_str, reverse=True)

    def _is_inbound(a: dict) -> bool:
        # Fortellis logs customer email replies as "Read Email" (activityType 20)
        return _is_read_email(a)

    # Walk newest→oldest and pick the first inbound that is *newer* than what we've seen
    for a in acts:
        if not _is_inbound(a):
            continue

        aid = str(a.get("activityId") or a.get("id") or "")
        ats = _ts_str(a)

        # skip exact same inbound we've already processed
        if last_seen_id and aid == last_seen_id:
            continue

        # if we have a last_seen_ts, require strictly newer
        if last_seen_ts:
            try:
                ats_dt = _dt.fromisoformat(ats.replace("Z", "+00:00"))
                lst_dt = _dt.fromisoformat(last_seen_ts.replace("Z", "+00:00"))
                if ats_dt <= lst_dt:
                    continue
            except Exception:
                # if timestamp parsing fails, fall back to id mismatch only
                if last_seen_id and aid == last_seen_id:
                    continue

        # Found a new inbound
        return True, (ats or None), (aid or None)

    return False, None, None



def process_kbb_ico_lead(
    opportunity,
    lead_age_days,
    rooftop_name,
    inquiry_text,
    token,
    subscription_id,
    SAFE_MODE=False,
    rooftop_sender=None,
    trigger="cron",            # "cron" (old style) or "email_webhook"
    inbound_ts=None,           # timestamp string from webhook (ISO)
    inbound_msg_id=None,       # message id / synthetic id
    inbound_subject=None,      # subject of the inbound email (for reply subject)
):

    opp_id = opportunity.get("opportunityId") or opportunity.get("id")
    created_iso = opportunity.get("createdDate") or opportunity.get("created_on")

    # ES-only state (no comments)
    state = dict(opportunity.get("_kbb_state") or {})
    # normalize defaults without clobbering existing keys
    state.setdefault("mode", "cadence")
    state.setdefault("last_template_day_sent", None)
    state.setdefault("last_template_sent_at", None)
    state.setdefault("last_customer_msg_at", None)
    state.setdefault("last_agent_msg_at", None)
    state.setdefault("nudge_count", 0)
    state.setdefault("last_inbound_activity_id", None)
    state.setdefault("last_appt_activity_id", None)
    state.setdefault("appt_due_utc", None)
    state.setdefault("appt_due_local", None)
    state.setdefault("last_confirmed_due_utc", None)    # what we last confirmed to the customer
    state.setdefault("last_confirm_sent_at", None)       # when we last sent a confirmation
    
    # keep a single shared dict reference everywhere
    opportunity["_kbb_state"] = state
    
    # --- safety inits to avoid UnboundLocalError on non-scheduling paths ---
    created_appt_ok = False     # whether we created an appt this turn
    appt_human      = None      # nice human-readable time (only if parsed)
    due_dt_iso_utc  = None      # ISO UTC for scheduling, if parsed
    new_id          = None      # activityId returned by schedule_activity
    # -----------------------------------------------------------------------


    # --- detect customer opt-out message ---
    state.setdefault("last_optout_seen_at", None)
    found_optout, optout_ts, optout_txt = _latest_customer_optout(opportunity)
    
    # also consider the inquiry_text (sometimes we only get that)
    found_optout = found_optout or _is_optout_text(inquiry_text)

    from os import getenv as _getenv

    # Are we in Outlook mode overall?
    is_outlook_mode = (EMAIL_MODE == "outlook")

    # Is THIS invocation coming from an Outlook webhook?
    is_webhook = is_outlook_mode and (trigger == "email_webhook")

    
    declined = False
    if found_optout:
        # Trigger decline if we never processed an opt-out, OR if still active/unsuppressed
        last_seen = state.get("last_optout_seen_at")
        already_closed = state.get("mode") in {"closed_declined"} or state.get("email_blocked_do_not_email")
        if (not last_seen or not already_closed):
            declined = True
            state["last_optout_seen_at"] = optout_ts or last_seen
            log.info("Detected NEW or unresolved opt-out at %s: %r", optout_ts, optout_txt)
        else:
            log.debug("Opt-out previously handled (mode=%s); skipping reprocess.", state.get("mode"))


    log.debug(
        "KBB OPT-OUT check opp=%s found_optout=%s ts=%s declined=%s last_seen=%s",
        opp_id, bool(found_optout), optout_ts, bool(declined), state.get("last_optout_seen_at")
    )
    
    # ✅ Immediately honor opt-out before anything else
    if declined:
        log.info("DECLINE ENTER opp=%s mode_before=%s", opp_id, state.get("mode"))
        now_iso = _dt.now(_tz.utc).isoformat()
        state["mode"] = "closed_declined"
        state["nudge_count"] = 0
        state["last_agent_msg_at"] = now_iso
        state["email_blocked_do_not_email"] = True
        opportunity["_kbb_state"] = state

        log.debug("DECLINE ES-WRITE opp=%s payload_keys=%s", opp_id, ["_kbb_state","isActive","checkedDict"])

        # Persist to ES (also mark inactive + exit_type)
        try:
            from esQuerys import esClient
            from es_resilient import es_update_with_retry
            checked = dict(opportunity.get("checkedDict") or {})
            checked["exit_type"] = "customer_declined"
            checked["exit_reason"] = "Stop emailing me"
            es_update_with_retry(esClient, index="opportunities", id=opp_id,
                                 doc={"_kbb_state": state, "isActive": False, "checkedDict": checked})
            log.info("DECLINE ES-OK opp=%s set isActive=False", opp_id)

        except Exception as e:
            log.warning("ES persist failed (global decline): %s", e)
    
        # Flip CRM to Not In Market + DoNotEmail on the customer
        try:
            from fortellis import set_opportunity_inactive, add_opportunity_comment, set_customer_do_not_email
            log.debug("CRM INACTIVE CALL opp=%s sub_status=Not In Market", opp_id)

            set_opportunity_inactive(token, subscription_id, opp_id,
                                     sub_status="Not In Market",
                                     comments="Customer requested no further contact — set inactive by Patti")

            add_opportunity_comment(token, subscription_id, opp_id,
                                    "Patti: Customer requested NO FURTHER CONTACT. Email/SMS suppressed; set to Not In Market.")
            cust = (opportunity.get("customer") or {})
            customer_id = cust.get("id")
            emails = cust.get("emails") or []
            email_address = (next((e for e in emails if e.get("isPreferred")), emails[0]) if emails else {}).get("address")
            if customer_id and email_address:
                set_customer_do_not_email(token, subscription_id, customer_id, email_address, do_not=True)
        except Exception as e:
            log.warning("CRM inactive/DoNotEmail failed (global decline): %s", e)
    
        return state, True


    # [#1] HARD STOP: if this opp is declined/inactive/closed or email is blocked, do nothing
    checked = (opportunity.get("checkedDict") or {})
    already_declined    = (checked.get("exit_type") == "customer_declined")
    already_inactive    = (opportunity.get("isActive") is False)
    already_closed_mode = (state.get("mode") == "closed_declined")
    email_blocked       = bool(state.get("email_blocked_do_not_email"))

    if already_declined or already_inactive or already_closed_mode or email_blocked:
        log.info(
            "KBB ICO: hard-stop (declined=%s inactive=%s closed_mode=%s blocked=%s) opp=%s",
            already_declined, already_inactive, already_closed_mode, email_blocked, opp_id
        )
        # normalize the flag so future runs also short-circuit
        state["email_blocked_do_not_email"] = True
        opportunity["_kbb_state"] = state
        return state, False

    action_taken = False
    selected_inbound_id = None
    reply_subject = "Re:"
    from helpers import build_kbb_ctx
    
    kbb_ctx = build_kbb_ctx(opportunity)

    opp_id = opportunity.get("opportunityId") or opportunity.get("id")
    customer_id = (opportunity.get("customer") or {}).get("id")

    # Only hit Fortellis activities when we're NOT Outlook-webhook-driven.
    if is_webhook:
        acts_live = []   # rely on ES + webhook data, no API call
    else:
        acts_live = _fetch_activities_live(opp_id, customer_id, token, subscription_id)


    # Track if appointment is currently booked or upcoming
    scheduled_active_now = (
        bool(state.get("last_appt_activity_id")) or
        bool(state.get("appt_due_utc")) or
        state.get("mode") == "scheduled" or
        _has_upcoming_appt(acts_live, state)
    )
    log.info("KBB ICO: scheduled_active_now=%s (mode=%s, last_appt_id=%r, appt_due_utc=%r)",
             scheduled_active_now, state.get("mode"),
             state.get("last_appt_activity_id"), state.get("appt_due_utc"))


    # After: acts_live = _fetch_activities_live(...)
    try:
        if isinstance(acts_live, dict):
            sa = acts_live.get("scheduledActivities") or []
            ca = acts_live.get("completedActivities") or []
            log.info("KBB ICO: acts_live shape=dict sched=%d completed=%d", len(sa), len(ca))
            if sa[:1]:
                log.info("KBB ICO: first scheduled activity name=%r type=%r",
                         (sa[0].get("activityName")), (sa[0].get("activityType")))
        elif isinstance(acts_live, list):
            log.info("KBB ICO: acts_live shape=list total=%d", len(acts_live))
            if acts_live[:1]:
                log.info("KBB ICO: first item keys=%s", list(acts_live[0].keys()))
        else:
            log.info("KBB ICO: acts_live shape=%s", type(acts_live).__name__)
    except Exception as _e:
        log.warning("KBB ICO: acts_live logging failed: %s", _e)


    # Try to hydrate state from a saved state comment found among live acts (MERGE, don’t replace)
    for a in acts_live:
        txt = (a.get("comments") or a.get("notes") or "") or ""
        if STATE_TAG in txt:
            try:
                loaded = json.loads(_re.sub(r".*?\[PATTI_KBB_STATE\]\s*", "", txt, flags=_re.S))
                state.update(loaded or {})
            except Exception:
                pass
            break

    # --- DEBUG: state after merge + last send times ---
    try:
        log.info(
            "KBB day debug A → opp=%s mode=%s last_day=%s last_sent=%s last_agent=%s",
            opp_id,
            state.get("mode"),
            state.get("last_template_day_sent"),
            state.get("last_template_sent_at"),
            state.get("last_agent_msg_at"),
        )
    except Exception as _e:
        log.warning("KBB day debug A failed: %s", _e)
    # --- /DEBUG --

    # 🔁 HYDRATE last send time from live activities (BEFORE day math / duplicate guard)
    if not state.get("last_template_sent_at"):
        def _is_completed_send(act):
            name = (act.get("activityName") or act.get("name") or "")
            cat  = (act.get("category") or "").strip()
            return name.startswith("Fortellis - Send Email") and cat == "Completed"

        sent = [a for a in (acts_live or []) if _is_completed_send(a)]
        if sent:
            last_sent = max(sent, key=lambda x: x.get("completedDate") or "")
            # ensure at least Day 1 is recorded; don't downgrade if already >1
            state["last_template_day_sent"] = max(1, (state.get("last_template_day_sent") or 0))
            state["last_template_sent_at"]  = last_sent.get("completedDate")
            # keep agent timestamp aligned if missing
            state.setdefault("last_agent_msg_at", last_sent.get("completedDate"))


    # ✅ Always short-circuit if a new customer-booked appointment exists
    handled, did_send = _short_circuit_if_booked(
        opportunity, acts_live, state,
        token=token, subscription_id=subscription_id,
        rooftop_name=rooftop_name, SAFE_MODE=SAFE_MODE, rooftop_sender=rooftop_sender
    )
    if handled and did_send:
        action_taken = True
        opportunity["_kbb_state"] = state
        return state, action_taken

    # === If customer already declined earlier, stop everything ==============
    if state.get("mode") == "closed_declined":
        log.info("KBB ICO: declined → skip all outreach (opp=%s)", opp_id)
        opportunity["_kbb_state"] = state
        return state, action_taken
    
    # === If appointment is booked/upcoming, pause cadence but still watch for inbound ===
    if state.get("mode") == "scheduled" or _has_upcoming_appt(acts_live, state):
        if is_webhook:
            # Webhook invocation = we know this is a fresh inbound
            log.info("KBB ICO: inbound during scheduled/appt (webhook) → keep scheduled; reply in-thread")
            state["mode"] = "scheduled"          # ✅ do NOT switch to convo
            state["nudge_count"] = 0
            if inbound_ts:
                state["last_customer_msg_at"] = inbound_ts
            if inbound_msg_id:
                state["last_inbound_activity_id"] = inbound_msg_id
                        # fall through to convo handling
        else:
            # Legacy CRM-based detection for stores not yet on Outlook
            has_reply, last_cust_ts, last_inbound_activity_id = customer_has_replied(
                opportunity,
                token,
                subscription_id,
                state,
                acts=acts_live,
            )

            if (
                has_reply
                and last_inbound_activity_id
                and last_inbound_activity_id != state.get("last_inbound_activity_id")
            ):
                log.info("KBB ICO: true customer reply after appointment → switch to convo mode")
                state["mode"] = "convo"
                state["nudge_count"] = 0
                if last_cust_ts:
                    state["last_customer_msg_at"] = last_cust_ts
                state["last_inbound_activity_id"] = last_inbound_activity_id
                # fall through to convo handling below
            else:
                log.info("KBB ICO: appointment active, no *new* inbound → stay quiet")
                state["mode"] = "scheduled"
                opportunity["_kbb_state"] = state
                return state, action_taken
    # else: not scheduled and no upcoming appt → continue into normal detection/cadence logic

    # --- Detect whether we have a NEW inbound to respond to ---

    if is_webhook:
        # This invocation *is* a new inbound email from Outlook
        has_reply = True
        last_cust_ts = inbound_ts or _dt.now(_tz.utc).isoformat()
        last_inbound_activity_id = inbound_msg_id or f"esmsg:{last_cust_ts}"
    
    elif trigger == "kbb_adf":
        # ✅ ADF notifications are system-generated. Not a customer reply.
        has_reply = False
        last_cust_ts = None
        last_inbound_activity_id = None
    
    else:
        # Legacy CRM-based detection
        has_reply, last_cust_ts, last_inbound_activity_id = customer_has_replied(
            opportunity,
            token,
            subscription_id,
            state,
            acts=acts_live,
        )


    # === Compute last agent send time (prefer ES state; avoid extra CRM calls in webhook mode) ===
    last_agent_dt = None
    if state.get("last_agent_msg_at"):
        try:
            last_agent_dt = _dt.fromisoformat(
                str(state["last_agent_msg_at"]).replace("Z", "+00:00")
            )
        except Exception:
            last_agent_dt = None

    # Only fall back to Fortellis sends when we are NOT in webhook mode
    if (last_agent_dt is None) and (not is_webhook):
        last_agent_dt_live = _last_agent_send_dt(acts_live)
        if last_agent_dt_live is not None:
            last_agent_dt = last_agent_dt_live

    if is_webhook:
        has_new_inbound = True  # webhook itself is the signal
    elif trigger == "kbb_adf":
        has_new_inbound = False
    else:
        has_new_inbound = has_reply or _has_new_read_email_since(acts_live, last_agent_dt)
    
    if not has_new_inbound:
        log.info(
            "KBB ICO: no new inbound detected (mode=%s, webhook=%s)",
            state.get("mode"),
            is_webhook,
        )
    
        # ✅ Hard stop: never send a convo reply without new inbound
        if state.get("mode") == "convo":
            log.info("KBB ICO: convo mode but no new inbound → suppress reply (anti-duplicate)")
            opportunity["_kbb_state"] = state
            return state, action_taken
    
        # Let cadence logic later decide if a nudge should be sent.
        # We just don't drop into convo reply.
    
    else:
        # ✅ We have new inbound → reply, but don't clobber scheduled state
        if scheduled_active_now:
            state["mode"] = "scheduled"
        else:
            state["mode"] = "convo"
    
        state["nudge_count"] = 0
        if has_reply and last_cust_ts:
            state["last_customer_msg_at"] = last_cust_ts
        if last_inbound_activity_id:
            state["last_inbound_activity_id"] = last_inbound_activity_id


    # If we have new inbound, prepare reply subject/body context
    if has_new_inbound:
        # Only re-fetch activities if we are in CRM mode and actually sent something new
        if (not is_webhook) and action_taken:
            acts_now = _fetch_activities_live(opp_id, customer_id, token, subscription_id)
        else:
            acts_now = acts_live

        selected_inbound_id = None

        if is_webhook:
            # Outlook path: the inbound email *is* what we reply to
            selected_inbound_id = last_inbound_activity_id
        else:
            def _newest_read_after(acts, floor_dt):
                newest = None
                newest_dt = None
                for a in acts or []:
                    if not _is_read_email(a):
                        continue
                    adt = _activity_dt(a)
                    if not adt:
                        continue
                    if (floor_dt is None) or (adt > floor_dt):
                        if (newest_dt is None) or (adt > newest_dt):
                            newest_dt = adt
                            newest = a
                return newest, newest_dt

            newest_read, newest_dt = _newest_read_after(acts_now, last_agent_dt)

            # Fallback order: newest fresh read → detector id → prior snapshot newest
            if newest_read:
                selected_inbound_id = str(
                    newest_read.get("activityId") or newest_read.get("id") or ""
                )
            elif last_inbound_activity_id:
                selected_inbound_id = last_inbound_activity_id
            else:
                selected_inbound_id = _latest_read_email_id(acts_now)

        log.info(
            "KBB ICO: replying to inbound id=%s; snippet=%r",
            selected_inbound_id,
            (inquiry_text or "")[:120],
        )

        # Initialize a safe default subject
        reply_subject = "Re:"

        # Preserve earlier opt-out decision
        declined_optout = bool(declined)

        if is_outlook_mode:
            # Outlook path: we already have the inbound subject/body from the webhook
            def _clean_subject(s: str) -> str:
                s = _re.sub(r"^\s*\[.*?\]\s*", "", s or "", flags=_re.I)  # strip [CAUTION], etc.
                s = _re.sub(r"^\s*(re|fwd)\s*:\s*", "", s, flags=_re.I)   # strip leading RE:/FWD:
                return s.strip()

            subj_src = inbound_subject or ""
            subj_clean = _clean_subject(subj_src)
            reply_subject = f"Re: {subj_clean}" if subj_clean else "Re:"
            # `inquiry_text` is already the webhook body; no CRM fetch needed
        else:
            # Legacy CRM path: fetch inbound subject/body from Fortellis
            if selected_inbound_id:
                try:
                    from fortellis import get_activity_by_id_v1

                    full = get_activity_by_id_v1(selected_inbound_id, token, subscription_id)

                    thread_subject = (
                        (full.get("message") or {}).get("subject") or ""
                    ).strip()

                    def _clean_subject(s: str) -> str:
                        s = _re.sub(r"^\s*\[.*?\]\s*", "", s or "", flags=_re.I)
                        s = _re.sub(r"^\s*(re|fwd)\s*:\s*", "", s, flags=_re.I)
                        return s.strip()

                    reply_subject = (
                        f"Re: {_clean_subject(thread_subject)}"
                        if thread_subject
                        else "Re:"
                    )

                    latest_body_raw = (
                        (full.get("message") or {}).get("body") or ""
                    ).strip()
                    latest_body = _top_reply_only(latest_body_raw)

                    if not latest_body:
                        import re as _re2

                        _TAGS = _re2.compile(r"<[^>]+>")
                        _WS = _re2.compile(r"\s+")
                        light = _WS.sub(" ", _TAGS.sub(" ", latest_body_raw)).strip()
                        if light:
                            latest_body = light

                    if latest_body:
                        inquiry_text = latest_body
                        log.info(
                            "KBB ICO: using inbound body (len=%d): %r",
                            len(latest_body),
                            latest_body[:120],
                        )
                    else:
                        log.info(
                            "KBB ICO: inbound had no usable body; keeping prior inquiry_text."
                        )
                except Exception as e:
                    log.warning(
                        "Could not load inbound activity %s: %s",
                        selected_inbound_id,
                        e,
                    )
            else:
                log.info(
                    "KBB ICO: no selected inbound id; keeping prior inquiry_text and default subject."
                )
        
        # 🔐 Re-evaluate decline BUT never lose an earlier True
        declined_from_classifier = False
        try:
            # your old classifier, if present
            declined_from_classifier = _is_decline(inquiry_text)
        except NameError:
            pass
        
        declined = bool(
            declined_optout                           # from earlier _latest_customer_optout()
            or _is_optout_text(inquiry_text)          # raw text check on current body
            or declined_from_classifier               # legacy classifier
        )
        
        if declined:
            log.info("KBB ICO: decline detected in inbound: %r", (inquiry_text or "")[:120])


        if not declined:
            # === Attempt to auto-schedule if customer proposed a time ===
            from gpt import extract_appt_time
            proposed = extract_appt_time(inquiry_text or "", tz="America/Los_Angeles")

            appt_iso = (proposed.get("iso") or "").strip()
            conf = float(proposed.get("confidence") or 0)

            if appt_iso and conf >= 0.60:
                try:
                    try:
                        dt_local = _dt.fromisoformat(appt_iso.replace("Z", "+00:00"))
                    except Exception:
                        dt_local = None

                    if dt_local and dt_local.tzinfo:
                        due_dt_iso_utc = dt_local.astimezone(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    else:
                        # Fallback: assume now (you can improve by re-parsing with local TZ)
                        due_dt_iso_utc = _dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                        dt_local = _dt.fromisoformat(due_dt_iso_utc.replace("Z","+00:00")).astimezone(_tz.utc)

                    # Create the appointment activity
                    schedule_activity(
                        token, subscription_id, opp_id,
                        due_dt_iso_utc=due_dt_iso_utc,
                        activity_name="KBB ICO Appointment",
                        activity_type="Appointment",
                        comments=f"Auto-scheduled from customer email: {inquiry_text[:180]}"
                    )
                    created_appt_ok = True
                    appt_human = _fmt_local_human(dt_local, tz_name="America/Los_Angeles")
                    acts_after = _fetch_activities_live(opp_id, customer_id, token, subscription_id)
                    new_id, _ = _find_new_customer_scheduled_appt(acts_after, state, token=token,
                                                                  subscription_id=subscription_id,
                                                                  opp_id=opp_id, customer_id=customer_id)
                    if new_id:
                        state["last_appt_activity_id"] = new_id
                
                except Exception as e:
                    log.warning("KBB ICO: failed to auto-schedule proposed time: %s", e)


        # === COMPOSE + SEND ================================================
        cust_first = (opportunity.get('customer', {}) or {}).get('firstName') or "there"

        # ---------- DECLINED (opt-out) ----------
        if declined:
            log.info("KBB ICO: decline/opt-out detected; suppressing ALL future sends for opp=%s", opp_id)

            # Set local stop flags
            now_iso = _dt.now(_tz.utc).isoformat()
            state["mode"] = "closed_declined"
            state["nudge_count"] = 0
            state["last_agent_msg_at"] = now_iso
            state["email_blocked_do_not_email"] = True
            opportunity["_kbb_state"] = state

            # Persist to ES immediately so re-runs short-circuit deterministically
            try:
                from esQuerys import esClient
                from es_resilient import es_update_with_retry  # if you use a wrapper; else use your existing helper
            except Exception:
                es_update_with_retry = None
            
            try:
                if es_update_with_retry:
                    # also persist an explicit exit marker in checkedDict
                    checked = dict(opportunity.get("checkedDict") or {})
                    checked["exit_type"] = "customer_declined"
                    checked["exit_reason"] = "Stop emailing me"
            
                    es_update_with_retry(
                        esClient,
                        index="opportunities",
                        id=opp_id,
                        doc={"_kbb_state": state, "isActive": False, "checkedDict": checked}
                    )
            except Exception as e:
                log.warning("ES persist failed (decline): %s", e)
            
            # Flip CRM opp → Not In Market (no send)
            try:
                from fortellis import set_opportunity_inactive, add_opportunity_comment
                set_opportunity_inactive(
                    token,
                    subscription_id,
                    opp_id,
                    sub_status="Not In Market",
                    comments="Customer requested no further contact — set inactive by Patti"
                )
                # Add a clear CRM note for the team
                add_opportunity_comment(
                    token, subscription_id, opp_id,
                    "Patti: Customer requested NO FURTHER CONTACT. Email/SMS suppressed; "
                    "opportunity set to Not In Market."
                )
            except Exception as e:
                log.warning("CRM inactive/comment failed (decline): %s", e)
            
            # Also mark the customer record as DoNotEmail=True
            try:
                cust = (opportunity.get("customer") or {})
                customer_id = cust.get("id")
            
                # choose preferred email if present, else first, else None
                email_address = None
                emails = cust.get("emails") or []
                if emails:
                    preferred = next((e for e in emails if e.get("isPreferred")), None)
                    email_address = (preferred or emails[0]).get("address")
            
                if customer_id and email_address:
                    from fortellis import set_customer_do_not_email
                    set_customer_do_not_email(token, subscription_id, customer_id, email_address, do_not=True)
                    log.info("Customer marked DoNotEmail in CRM for opp=%s (email=%s)", opp_id, email_address)
                else:
                    log.warning("Cannot set DoNotEmail: missing customer_id or email (opp=%s)", opp_id)
            except Exception as e:
                log.warning("Failed to mark customer DoNotEmail in CRM: %s", e)
            
            action_taken = True
            return state, action_taken  # important: stop here (no email)



        # ---------- APPOINTMENT CONFIRMATION ----------
        elif created_appt_ok and appt_human:
            subject = f"Re: Your visit on {appt_human}"
        
            # Build Add-to-Calendar links
            rt = (ROOFTOP_INFO.get(rooftop_name) or {})
            summary     = f"{rooftop_name} – KBB Inspection"
            location    = rt.get("address") or rooftop_name
            description = "15–20 minute in-person inspection to finalize your Kelley Blue Book® Instant Cash Offer."
            links       = build_calendar_links(summary, description, location, due_dt_iso_utc, duration_min=30)
        
            add_to_cal_html = f"""
              <p style="margin:16px 0 8px 0;">Add to calendar:</p>
              <p>
                <a href="{links['google']}">Google</a> &nbsp;|&nbsp;
                <a href="{links['outlook']}">Outlook</a> &nbsp;|&nbsp;
                <a href="{links['yahoo']}">Yahoo</a>
              </p>
            """.strip()
        
            body_html = f"""
                <p>Hi {cust_first},</p>
                <p>Your appointment is confirmed for <strong>{appt_human}</strong> at {rooftop_name}.</p>
                {add_to_cal_html}
                <p>Please bring your title, ID, and keys. If you need to change your time, use this link: <{{LegacySalesApptSchLink}}></p>
            """.strip()
        
            # Normalize + footer + subject guard (no extra CTA here)
            body_html = normalize_patti_body(body_html)
            body_html = _patch_address_placeholders(body_html, rooftop_name)
            body_html = _PREFS_RE.sub("", body_html).strip()
            body_html = body_html + build_patti_footer(rooftop_name)
            if not subject.lower().startswith("re:"):
                subject = "Re: " + subject
        
            # Resolve recipient
            cust = (opportunity.get("customer") or {})
            # prefer the explicit customer.emails[].isPreferred if present
            emails = cust.get("emails") or []
            preferred = next((e for e in emails if e.get("isPreferred")), None)
            email = cust.get("emailAddress") or (preferred or (emails[0] if emails else {})).get("address")
            if not email:
                email = (opportunity.get("_lead", {}) or {}).get("email_address")
            recipients = [email] if (email and not SAFE_MODE) else [TEST_TO]
            if not recipients:
                log.warning("No recipient; skip send for opp=%s", opp_id)
                opportunity["_kbb_state"] = state
                return state, action_taken
            
            # --- PERSIST FIRST so re-runs short-circuit even if send fails ---
            now_iso = _dt.now(_tz.utc).isoformat()
            try:
                chosen_appt_id = new_id if (("new_id" in locals()) and new_id) else appt_id
            except NameError:
                chosen_appt_id = appt_id
            
            state["mode"]                  = "scheduled"
            state["last_appt_activity_id"] = chosen_appt_id
            state["appt_due_utc"]          = due_dt_iso_utc
            state["appt_due_local"]        = appt_human
            state["nudge_count"]           = 0
            state["last_agent_msg_at"]     = now_iso
            # (intentionally NOT setting last_confirm* yet — only after a successful send)
            opportunity["_kbb_state"]      = state
            
            # Persist state to ES (pre-send)
            try:
                from esQuerys import esClient
                from es_resilient import es_update_with_retry
                es_update_with_retry(
                    esClient,
                    index="opportunities",
                    id=opp_id,
                    doc={"_kbb_state": state}
                )
                log.info("Persisted _kbb_state to ES for opp=%s (pre-send confirm)", opp_id)
            except Exception as e:
                log.warning("ES persist of _kbb_state failed (pre-send confirm): %s", e)
            
            # ✅ Flip CRM subStatus → Appointment Set (record appt even if email is blocked)
            try:
                from fortellis import set_opportunity_substatus
                resp = set_opportunity_substatus(token, subscription_id, opp_id, sub_status="Appointment Set")
                log.info("SubStatus update response: %s", getattr(resp, "status_code", "n/a"))
                action_taken = True
            except Exception as e:
                log.warning("set_opportunity_substatus failed: %s", e)
            
            # --- SEND with DoNotEmail handling ---
            did_send = False
            
            # ✅ Choke point BEFORE any send
            if not _can_email(state):
                log.info("Email suppressed by state for opp=%s (booked-confirmation)", opp_id)
                opportunity["_kbb_state"] = state
                return state, action_taken  # we already recorded appt + substatus
            
            try:
                send_opportunity_email_activity(
                    token, subscription_id, opp_id,
                    sender=rooftop_sender,
                    recipients=recipients, carbon_copies=[],
                    subject=subject, body_html=body_html, rooftop_name=rooftop_name
                )
                did_send = True
            
                # Mark that we actually sent the confirmation
                state["last_confirmed_due_utc"] = due_dt_iso_utc
                state["last_confirm_sent_at"]   = _dt.now(_tz.utc).isoformat()
                opportunity["_kbb_state"]       = state
                try:
                    es_update_with_retry(
                        esClient,
                        index="opportunities",
                        id=opp_id,
                        doc={"_kbb_state": state}
                    )
                    log.info("Persisted _kbb_state to ES for opp=%s (post-send confirm)", opp_id)
                except Exception as e:
                    log.warning("ES persist of _kbb_state failed (post-send confirm): %s", e)
            
            except Exception as e:
                # Detect Fortellis DoNotEmail 400 and swallow (idempotent)
                resp = getattr(e, "response", None)
                body = ""
                if resp is not None:
                    try:
                        body = resp.json()
                    except Exception:
                        body = resp.text
                body_str = str(body)
            
                if "SendEmailInvalidRecipient" in body_str and "DoNotEmail" in body_str:
                    log.warning("KBB ICO: DoNotEmail — skipping confirmation email for opp %s.", opp_id)
                    # ✅ Set local suppression flag so future runs short-circuit
                    state["email_blocked_do_not_email"] = True
                    opportunity["_kbb_state"] = state
                    try:
                        es_update_with_retry(
                            esClient,
                            index="opportunities",
                            id=opp_id,
                            doc={"_kbb_state": state}
                        )
                    except Exception:
                        pass
                    # Optional: add a CRM comment for human follow-up by phone/text
                    try:
                        from fortellis import add_opportunity_comment
                        add_opportunity_comment(
                            token, subscription_id, opp_id,
                            comment="Auto-confirmation not sent: customer marked DoNotEmail."
                        )
                    except Exception as ee:
                        log.warning("Failed to add DoNotEmail comment: %s", ee)
                    # no re-raise
                else:
                    log.error("Confirm send failed for opp %s: %s", opp_id, e)
                    state["last_confirm_error"] = (body_str[:500] if isinstance(body_str, str) else str(body)[:500])
                    state["last_confirm_attempt_at"] = _dt.now(_tz.utc).isoformat()
                    try:
                        es_update_with_retry(
                            esClient,
                            index="opportunities",
                            id=opp_id,
                            doc={"_kbb_state": state}
                        )
                    except Exception:
                        pass
                    # no re-raise
            
            return state, action_taken





        # ---------- NORMAL GPT CONVO ----------
        else:
            from gpt import run_gpt
            # Include full conversation thread for context
            msgs = opportunity.get("messages", [])
            if isinstance(msgs, list) and inquiry_text:
                msgs = msgs + [{
                    "msgFrom": "customer",
                    "subject": reply_subject.replace("Re: ",""),
                    "body": inquiry_text,
                    "date": _dt.now(_tz.utc).isoformat()
                }]

            cust_first = (opportunity.get('customer', {}) or {}).get('firstName') or "there"
            # also persist for future cycles
            opportunity["messages"] = msgs

            
            # ===== send the normal GPT convo reply =====
            cust_first = (opportunity.get('customer', {}) or {}).get('firstName') or "there"
            prompt = compose_kbb_convo_body(rooftop_name, cust_first, inquiry_text or "")


            from helpers import build_kbb_ctx
            kbb_ctx = build_kbb_ctx(opportunity)

            from helpers import get_kbb_offer_context_simple
            
            facts = get_kbb_offer_context_simple(opportunity) or {}
            
            # Make sure GPT can see the amount + vehicle + url
            if facts.get("amount_usd"):
                kbb_ctx["offer_amount_usd"] = facts["amount_usd"]      # keep as "$27,000" string
            if facts.get("vehicle"):
                kbb_ctx["vehicle"] = facts["vehicle"]
            if facts.get("offer_url"):
                kbb_ctx["offer_url"] = facts["offer_url"]

            log.info("KBB_CTX debug: offer_amount_usd=%r offer_url=%r vehicle=%r",
             kbb_ctx.get("offer_amount_usd"), kbb_ctx.get("offer_url"), kbb_ctx.get("vehicle"))

            
            reply = run_gpt(
                prompt,
                customer_name=cust_first,
                rooftop_name=rooftop_name,
                prevMessages=False,
                persona="kbb_ico",
                kbb_ctx=kbb_ctx
            )
            
            subject   = (reply.get("subject") or reply_subject or "Re:").strip()
            body_html = (reply.get("body") or "").strip()
            
            # Normalize + scheduling CTA behavior
            body_html = normalize_patti_body(body_html)
            body_html = _patch_address_placeholders(body_html, rooftop_name)
            
            is_scheduled = (
                _crm_appt_set(opportunity)
                or scheduled_active_now
                or state.get("mode") == "scheduled"
                or _has_upcoming_appt(acts_live, state)
            )
            log.info(
                "KBB is_scheduled=%s (crm=%s scheduled_active_now=%s state_mode=%s upcoming_appt=%s)",
                is_scheduled,
                _crm_appt_set(opportunity),
                scheduled_active_now,
                state.get("mode"),
                _has_upcoming_appt(acts_live, state),
            )


            if is_scheduled:
                from helpers import rewrite_sched_cta_for_booked
                body_html = rewrite_sched_cta_for_booked(body_html)
            
                # STRIP any generic scheduling CTA variants no matter what wording GPT used
                body_html = _ANY_SCHED_LINE_RE.sub("", body_html).strip()
            
                # Remove raw token if it survived
                body_html = body_html.replace("<{LegacySalesApptSchLink}>", "").replace("<{LegacySalesApptSchLink }>", "")
            else:
                body_html = append_soft_schedule_sentence(body_html, rooftop_name)
            
            body_html = _PREFS_RE.sub("", body_html).strip()
            body_html = body_html + build_patti_footer(rooftop_name)
            if not subject.lower().startswith("re:"):
                subject = "Re: " + subject
            
            # Resolve recipient
            cust = (opportunity.get("customer") or {})
            email = cust.get("emailAddress") or ((cust.get("emails") or [{}])[0].get("address")) \
                    or (opportunity.get("_lead", {}) or {}).get("email_address")
            recipients = [email] if (email and not SAFE_MODE) else [TEST_TO]
            if not recipients:
                log.warning("No recipient; skip send for opp=%s", opp_id)
                opportunity["_kbb_state"] = state
                return state, action_taken
            
            # Reply in-thread to the selected inbound
            send_opportunity_email_activity(
                token, subscription_id, opp_id,
                sender=rooftop_sender,
                recipients=recipients,
                carbon_copies=[],
                subject=reply_subject,              # keep inbound thread subject
                body_html=body_html,
                rooftop_name=rooftop_name,
                reply_to_activity_id=selected_inbound_id  # <-- from above
            )
            
            # Persist compact thread memo
            now_iso = _dt.now(_tz.utc).isoformat()
            _thread_body = re.sub(r"<[^>]+>", " ", body_html)
            _thread_body = re.sub(r"\s+", " ", _thread_body).strip()
            
            msgs = opportunity.get("messages", []) or []
            msgs.append({
                "msgFrom": "patti",
                "subject": reply_subject.replace("Re: ", ""),
                "body": _thread_body,
                "date": now_iso
            })
            opportunity["messages"] = msgs
            
            state["last_agent_msg_at"] = now_iso
            state["mode"] = "convo"
            opportunity["_kbb_state"] = state
            return state, True

                
    # If we were scheduled at the start of this run, reply immediately (no nudge/template).
    if scheduled_active_now and selected_inbound_id:
        log.info("KBB ICO: scheduled_active_now=True + inbound → immediate reply")
    
        from gpt import run_gpt
    
        cust_first = ((opportunity.get('customer') or {}).get('firstName')) or "there"
    
        # 1) Persist the customer's latest message into thread history first
        msgs_hist = list(opportunity.get("messages") or [])
        if inquiry_text:
            msgs_hist.append({
                "msgFrom": "customer",
                "subject": (reply_subject or "Re:").replace("Re: ",""),
                "body": inquiry_text,
                "date": _dt.now(_tz.utc).isoformat()
            })
        opportunity["messages"] = msgs_hist
    
        # 2) Prompt with convo + appointment context (so no “we haven’t set a time yet”)
        prompt = compose_kbb_convo_body(rooftop_name, cust_first, inquiry_text or "")
        prompt += f"""
        
    
    Context for Patti (not shown to customer):
    - appointment_scheduled: yes
    - appointment_time_local: {state.get('appt_due_local') or 'unknown'}
    - reschedule_link_token: <{{LegacySalesApptSchLink}}>
    Instructions:
    - If appointment_time_local is known, explicitly confirm it.
    - Do NOT ask the customer to choose a day/time or propose times.
    - Do NOT include any scheduling CTA language such as:
      “let me know a time/day and time that works”, “schedule directly”, “reserve your time”, or the <{LegacySalesApptSchLink}> token.
    - Only mention rescheduling if the customer asks to change/cancel, or if they indicate a conflict.
      If rescheduling is needed, use this exact one-liner (and nothing else about scheduling):
      "If you need to reschedule, just reply here and we’ll help."
    - Do NOT include cadence/nudge language while appointment_scheduled is yes.

    """
    
        reply = run_gpt(
            prompt,
            customer_name=cust_first,
            rooftop_name=rooftop_name,
            prevMessages=False,
            persona="kbb_ico",
            kbb_ctx=kbb_ctx,
        )
    
        # 3) Build final body
        subject   = (reply.get("subject") or reply_subject or "Re:").strip()
        body_html = (reply.get("body") or "").strip()
        body_html = normalize_patti_body(body_html)
        body_html = _patch_address_placeholders(body_html, rooftop_name)
        if scheduled_active_now:
            from helpers import rewrite_sched_cta_for_booked

        body_html = _PREFS_RE.sub("", body_html).strip()
        body_html = body_html + build_patti_footer(rooftop_name)
        if not subject.lower().startswith("re:"):
            subject = "Re: " + subject
    
        # 4) Send in the SAME THREAD
        cust = (opportunity.get("customer") or {})
        email = cust.get("emailAddress") or ((cust.get("emails") or [{}])[0].get("address")) \
                or (opportunity.get("_lead", {}) or {}).get("email_address")
        recipients = [email] if (email and not SAFE_MODE) else [TEST_TO]
        if not recipients:
            log.warning("No recipient; skip send for opp=%s", opp_id)
            opportunity["_kbb_state"] = state
            return state, action_taken
            
        thread_subject = reply_subject if (reply_subject or "").strip() else subject
        
        send_opportunity_email_activity(
            token, subscription_id, opp_id,
            sender=rooftop_sender,
            recipients=recipients,
            carbon_copies=[],
            subject=thread_subject,                # keep inbound thread subject
            body_html=body_html,
            rooftop_name=rooftop_name,
            reply_to_activity_id=selected_inbound_id  # <— keeps CRM thread intact
        )
    
        # 5) Thread memo + state
        now_iso = _dt.now(_tz.utc).isoformat()
        _thread_body = re.sub(r"<[^>]+>", " ", body_html)
        _thread_body = re.sub(r"\s+", " ", _thread_body).strip()
        msgs_hist.append({
            "msgFrom": "patti",
            "subject": reply_subject.replace("Re: ", ""),
            "body": _thread_body,
            "date": now_iso
        })
        opportunity["messages"] = msgs_hist
    
        state["last_agent_msg_at"] = now_iso
        state["mode"] = "convo"  # stay in convo; DO NOT touch nudge_count
        opportunity["_kbb_state"] = state
        return state, True




    # ===== NUDGE LOGIC (customer went dark AFTER a reply) =====
    
    # If we were scheduled at the start of this run, reply immediately (no nudge/template).
    if scheduled_active_now and state.get("last_inbound_activity_id"):
        log.info("KBB ICO: scheduled_active_now=%s (mode=%s, last_appt_id=%r, appt_due=%r)",
             scheduled_active_now, state.get("mode"),
             state.get("last_appt_activity_id"), state.get("appt_due_utc"))

        
    # If we are already in convo mode, no new inbound detected now, and enough time has passed → send a nudge
    # 🔒 But NEVER send nudges once an appointment is scheduled/upcoming.
    if state.get("mode") == "convo" and not scheduled_active_now:

        last_agent_ts = state.get("last_agent_msg_at")
        nudge_count   = int(state.get("nudge_count") or 0)

        if last_agent_ts:
            try:
                last_agent_dt = _dt.fromisoformat(str(last_agent_ts).replace("Z", "+00:00"))
            except Exception:
                last_agent_dt = None

            if last_agent_dt:
                silence_days = (_dt.now(_tz.utc) - last_agent_dt).days
                # TUNABLES: interval & max nudges
                if silence_days >= 2 and nudge_count < 3:
                    log.info("KBB ICO: sending nudge #%s after %s days of silence", nudge_count + 1, silence_days)

                    from gpt import run_gpt
                    cust_first = (opportunity.get('customer', {}) or {}).get('firstName') or "there"
                    # Reuse prevMessages=True path (same JSON/format rules as processNewData follow-ups)
                    prompt = f"""
                    generate next Patti follow-up message for a Kelley Blue Book® Instant Cash Offer lead.
                    The customer previously replied once but has since gone silent.
                    Keep it short, warm, and helpful—remind about the ICO and next steps.

                    """

                    from helpers import build_kbb_ctx

                    # Build a fully armed context (adds offer_url, amount_usd, vehicle)
                    kbb_ctx = build_kbb_ctx(opportunity)
                    
                    reply = run_gpt(
                        prompt,
                        customer_name=cust_first,
                        rooftop_name=rooftop_name,
                        prevMessages=True,
                        persona="kbb_ico",
                        kbb_ctx=kbb_ctx,
                    )

                    subject   = reply.get("subject") or "Still interested in your Instant Cash Offer?"
                    body_html = reply.get("body") or ""
                    body_html = normalize_patti_body(body_html)
                    body_html = _patch_address_placeholders(body_html, rooftop_name)
                    body_html = _PREFS_RE.sub("", body_html).strip()
                    body_html = body_html + build_patti_footer(rooftop_name)

                    # Recipient
                    cust = (opportunity.get("customer") or {})
                    email = cust.get("emailAddress")
                    if not email:
                        emails = cust.get("emails") or []
                        email = (emails[0] or {}).get("address") if emails else None
                    if not email:
                        email = (opportunity.get("_lead", {}) or {}).get("email_address")
                    recipients = [email] if (email and not SAFE_MODE) else [TEST_TO]
                    if recipients:
                        #add_opportunity_comment(
                        #    token, subscription_id, opp_id,
                        #    f"[Patti] Sending Nudge #{nudge_count + 1} after silence → to {(email or 'TEST_TO')}"
                        #)
                        send_opportunity_email_activity(
                            token, subscription_id, opp_id,
                            sender=rooftop_sender,
                            recipients=recipients, carbon_copies=[],
                            subject=subject, body_html=body_html, rooftop_name=rooftop_name
                        )

                        # store a compact version of Patti’s reply in the thread (no footer/CTA)
                        _thread_body = re.sub(r"<[^>]+>", " ", body_html)          # strip tags
                        _thread_body = re.sub(r"\s+", " ", _thread_body).strip()   # collapse whitespace
                        _thread_body = _thread_body.split("Please let us know a convenient time", 1)[0].strip()
                        
                        now_iso = _dt.now(_tz.utc).isoformat()

                        msgs = opportunity.get("messages", [])
                        if not isinstance(msgs, list):
                            msgs = []
                        msgs.append({
                            "msgFrom": "patti",
                            "subject": subject.replace("Re: ", ""),
                            "body": _thread_body,
                            "date": now_iso
                        })
                        opportunity["messages"] = msgs

                        state["last_agent_msg_at"] = _dt.now(_tz.utc).isoformat()
                        state["nudge_count"] = nudge_count + 1
                        #_save_state_comment(token, subscription_id, opp_id, state)
                        action_taken = True 
                        opportunity["_kbb_state"] = state
                        return state, action_taken
                    else:
                        log.warning("No recipient for nudge; opp=%s", opp_id)
                    
                    opportunity["_kbb_state"] = state
                    return state, action_taken

        # In convo mode but not time for a nudge yet → do nothing this cycle
        log.info("KBB ICO: convo mode, no nudge due. Skipping send.")
        opportunity["_kbb_state"] = state
        return state, action_taken

    if state.get("mode") == "convo":
        log.info("KBB ICO: persisted convo mode — skip cadence.")
        opportunity["_kbb_state"] = state
        return state, action_taken
    # ===== Still in cadence (never replied) =====
    state["mode"] = "cadence"
    #_save_state_comment(token, subscription_id, opp_id, state)

    # --- DEBUG: inputs to day-pick ---
    try:
        log.info(
            "KBB day debug B → opp=%s created_iso=%r lead_age_days(param)=%s",
            opp_id,
            created_iso,
            lead_age_days,
        )
    except Exception as _e:
        log.warning("KBB day debug B failed: %s", _e)
    # --- /DEBUG ---

    # Offer-window override (if expired, jump to Day 08/09 track)
    expired = _ico_offer_expired(created_iso, exclude_sunday=True)
    effective_day = max(1, (lead_age_days or 0) + 1)
    if expired and lead_age_days < 8:
        effective_day = 8

    plan = events_for_day(effective_day)
    if not plan:
        opportunity["_kbb_state"] = state
        return state, action_taken

    if state.get("last_template_day_sent") == effective_day:
        log.info("KBB ICO: skipping Day %s (already sent)", effective_day)
        opportunity["_kbb_state"] = state
        return state, action_taken

    
    # --- DEBUG: result of day-pick & duplicate check ---
    try:
        log.info(
            "KBB day debug C → opp=%s effective_day=%s skipping_dup=%s",
            opp_id,
            effective_day,
            state.get("last_template_day_sent") == effective_day
        )
    except Exception as _e:
        log.warning("KBB day debug C failed: %s", _e)
    # --- /DEBUG ---

    # Subject/template selection zone (keep this early)
    tpl_key = plan.get("email_template_day")
    html = TEMPLATES.get(tpl_key)
    if not html:
        log.warning("KBB ICO: missing template for day key=%r", tpl_key)
        opportunity["_kbb_state"] = state
        return state, action_taken
    
    # Use the plan's explicit day if present; otherwise KEEP prior effective_day
    effective_day = plan.get("day") or effective_day


    # Rooftop info
    rooftop_addr = ((ROOFTOP_INFO.get(rooftop_name, {}) or {}).get("address") or "")

    # Salesperson (primary)
    sales_team = (opportunity.get("salesTeam") or [])
    sp = next((m for m in sales_team if m.get("isPrimary")), (sales_team[0] if sales_team else {}))
    salesperson_name  = " ".join(filter(None, [sp.get("firstName", ""), sp.get("lastName", "")])).strip()
    salesperson_phone = (sp.get("phone") or sp.get("mobile") or "")
    salesperson_email = (sp.get("email") or "")

    # Customer basics
    cust = (opportunity.get("customer") or {})
    cust_first = (cust.get("firstName") or opportunity.get("customer_first") or "there")

    # Trade info
    ti = (opportunity.get("tradeIns") or [{}])[0] if (opportunity.get("tradeIns") or []) else {}
    trade_year  = str(ti.get("year") or "")
    trade_make  = str(ti.get("make") or "")
    trade_model = str(ti.get("model") or "")

    # Merge fields
    ctx = {
        "DealershipName": rooftop_name,
        "SalesPersonName": salesperson_name,
        "SalespersonPhone": salesperson_phone,
        "SalespersonEmailAddress": salesperson_email,
        "CustFirstName": cust_first,
        "TradeYear": trade_year,
        "TradeMake": trade_make,
        "TradeModel": trade_model,
        "DealershipAddress": rooftop_addr,
    }
    body_html = fill_merge_fields(html, ctx)

    # Ensure exactly one booking CTA
    body_html = replace_or_append_booking_cta(body_html, rooftop_name)
    body_html = normalize_patti_body(body_html)
    body_html = _patch_address_placeholders(body_html, rooftop_name)
    body_html = _PREFS_RE.sub("", body_html).strip()

    # Subject from cadence plan
    subject = plan.get("subject") or f"{rooftop_name} — Your Instant Cash Offer"

    # Recipient resolution (SAFE_MODE honored)
    email_addr = ""
    emails = cust.get("emails") or []
    if emails:
        prim = next((e for e in emails if e.get("isPrimary") or e.get("isPreferred")), None)
        email_addr = (prim or emails[0]).get("address") or ""
    if not email_addr:
        email_addr = cust.get("emailAddress") or ""
    recipients = [email_addr] if (email_addr and not SAFE_MODE) else [TEST_TO]

    # Guard: no recipient → skip send cleanly
    if not recipients or not recipients[0]:
        log.warning("No recipient; skip cadence email for opp=%s", opp_id)
        opportunity["_kbb_state"] = state
        return state, action_taken

    # respect local suppression BEFORE marking anything sent
    if not _can_email(state):
        log.info("Email suppressed by state for opp=%s (cadence)", opp_id)
        opportunity["_kbb_state"] = state
        return state, action_taken

    # 1) Persist cadence step BEFORE send so we don’t re-send on next run
    now_iso = _dt.now(_tz.utc).isoformat()
    state["last_template_day_sent"] = effective_day
    state["last_template_sent_at"]  = now_iso
    opportunity["_kbb_state"] = state
    try:
        from esQuerys import esClient
        from es_resilient import es_update_with_retry
        es_update_with_retry(esClient, index="opportunities", id=opp_id, doc={"_kbb_state": state})
        log.info("Persisted _kbb_state pre-template send for opp=%s (day=%s)", opp_id, effective_day)
    except Exception as e:
        log.warning("ES persist failed (pre-template send): %s", e)

    # 2) Try to send, but swallow DoNotEmail so we don’t crash or loop
    try:
        send_opportunity_email_activity(
            token, subscription_id, opp_id,
            sender=rooftop_sender,
            recipients=recipients, carbon_copies=[],
            subject=subject, body_html=body_html, rooftop_name=rooftop_name
        )
        # success → update last_agent_msg_at
        state["last_agent_msg_at"] = _dt.now(_tz.utc).isoformat()
        opportunity["_kbb_state"] = state
        try:
            es_update_with_retry(esClient, index="opportunities", id=opp_id, doc={"_kbb_state": state})
        except Exception:
            pass

    except Exception as e:
        resp = getattr(e, "response", None)
        body = ""
        if resp is not None:
            try:
                body = resp.json()
            except Exception:
                body = resp.text
        s = str(body)

        if "SendEmailInvalidRecipient" in s and "DoNotEmail" in s:
            log.warning("DoNotEmail → skipping cadence email for opp %s", opp_id)
            state["email_blocked_do_not_email"] = True
            opportunity["_kbb_state"] = state
            try:
                es_update_with_retry(esClient, index="opportunities", id=opp_id, doc={"_kbb_state": state})
            except Exception:
                pass
            # Optional: note in CRM for human follow-up by phone/text
            try:
                from fortellis import add_opportunity_comment
                add_opportunity_comment(
                    token, subscription_id, opp_id,
                    comment="Auto-email not sent: customer is marked DoNotEmail. Recommend phone/text follow-up."
                )
            except Exception as ee:
                log.warning("Failed to add DoNotEmail comment: %s", ee)
            # do not re-raise; proceed so we can create phone/text task below if enabled
        else:
            log.error("Cadence send failed for opp %s: %s", opp_id, e)
            # state was persisted pre-send; do not re-raise to avoid loops

    # Phone/Text tasks 
    if ALLOW_TEXTING and plan.get("create_text_task", False) and _customer_has_text_consent(opportunity):
        schedule_activity(
            token, subscription_id, opp_id,
            due_dt_iso_utc=_dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            activity_name="KBB ICO: Text Task", activity_type=15,
            comments=f"Auto-scheduled per ICO Day {effective_day}."
        )
        
    if plan.get("create_phone_task", True):
        schedule_activity(
            token, subscription_id, opp_id,
            due_dt_iso_utc=_dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            activity_name="KBB ICO: Phone Task", activity_type=14,
            comments=f"Auto-scheduled per ICO Day {effective_day}."
        )
 
    action_taken = True  
    opportunity["_kbb_state"] = state
    return state, action_taken

def _customer_has_text_consent(opportunity) -> bool:
    # TODO: look at your CRM/TCPA field once available
    return bool((opportunity.get("customer",{}) or {}).get("tcpConsent", False))

