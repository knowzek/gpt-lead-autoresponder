# kbb_ico.py
from datetime import datetime as _dt, timezone as _tz, timedelta as _td
from kbb_templates import TEMPLATES, fill_merge_fields
from kbb_cadence import events_for_day
from fortellis import (
    add_opportunity_comment,
    send_opportunity_email_activity,
    schedule_activity,
)

from fortellis import search_activities_by_opportunity

import json, re
STATE_TAG = "[PATTI_KBB_STATE]"  # marker to find the state comment quickly

import os
TEST_TO = os.getenv("TEST_TO", "pattiautoresponder@gmail.com")
import logging
log = logging.getLogger(__name__)

import re as _re
from textwrap import dedent as _dd
from rooftops import ROOFTOP_INFO

import textwrap as _tw
import zoneinfo as _zi

from html import unescape as _unesc

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


_GMAIL_QUOTE_RE = _re.compile(r'(?is)<div[^>]*class="gmail_quote[^"]*"[^>]*>.*$', _re.M)
_BLOCKQUOTE_RE  = _re.compile(r'(?is)<blockquote[^>]*>.*$', _re.M)
_TAGS_RE        = _re.compile(r'(?is)<[^>]+>')

import re as _re2

# Detect any existing booking token/link so we don't double insert
_SCHED_ANY_RE = _re2.compile(r'(?is)(LegacySalesApptSchLink|Schedule\s+Your\s+Visit</a>)')

def _is_agent_send(act: dict) -> bool:
    nm = (act.get("activityName") or act.get("name") or "").strip().lower()
    at = str(act.get("activityType") or "").strip().lower()
    # Fortellis - Send Email is activityType 14
    return ("send email" in nm) or (at == "14" or act.get("activityType") == 14)

def _last_agent_send_dt(acts: list[dict]):
    latest = None
    for a in acts or []:
        if not _is_agent_send(a):
            continue
        dt = _activity_dt(a)  # your existing completedDate→dt converter
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
    from rooftops import ROOFTOP_INFO
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


def _short_circuit_if_booked(opportunity, acts_live, state,
                             *, token, subscription_id, rooftop_name, SAFE_MODE, rooftop_sender):
    """
    If we see a new 'Customer Scheduled Appointment', send a short confirmation,
    flip subStatus → Appointment Set, persist scheduled state, and return True.
    Otherwise return False.
    """
    opp_id      = opportunity.get("opportunityId") or opportunity.get("id")
    customer_id = (opportunity.get("customer") or {}).get("id")

    appt_id, appt_due_iso = _find_new_customer_scheduled_appt(
        acts_live, state,
        token=token, subscription_id=subscription_id,
        opp_id=opp_id, customer_id=customer_id
    )
    log.info("KBB ICO: booked-appt scan → id=%s due=%s", appt_id, appt_due_iso)
    if not (appt_id and appt_due_iso):
        return False

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
        due_dt_iso_utc = _dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Deterministic thanks email
    cust_first = (opportunity.get('customer', {}) or {}).get('firstName') or "there"
    subject = f"Re: Appointment confirmed for {appt_human}"
    body_html = f"""
        <p>Hi {cust_first},</p>
        <p>Thanks for booking — we’ll see you on <strong>{appt_human}</strong> at {rooftop_name}.</p>
        <p>Please bring your title, ID, and keys. If you need to change your time, use this link: <{{LegacySalesApptSchLink}}></p>
    """.strip()
    body_html = normalize_patti_body(body_html)
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
        return True  # we still consider it handled; don’t fall through

    send_opportunity_email_activity(
        token, subscription_id, opp_id,
        sender=rooftop_sender,
        recipients=recipients, carbon_copies=[],
        subject=subject, body_html=body_html, rooftop_name=rooftop_name
    )

    # Persist scheduled state so Patti stops nudges/templates
    state["mode"]                 = "scheduled"
    state["last_appt_activity_id"] = appt_id
    state["appt_due_utc"]         = due_dt_iso_utc
    state["appt_due_local"]       = appt_human
    state["nudge_count"]          = 0
    state["last_agent_msg_at"]    = _dt.now(_tz.utc).isoformat()
    _save_state_comment(token, subscription_id, opp_id, state)

    # Flip CRM subStatus → Appointment Set
    try:
        from fortellis import set_opportunity_substatus
        resp = set_opportunity_substatus(token, subscription_id, opp_id, sub_status="Appointment Set")
        log.info("SubStatus update response: %s", getattr(resp, "status_code", "n/a"))
    except Exception as e:
        log.warning("set_opportunity_substatus failed: %s", e)

    return True


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

def _has_upcoming_appt(acts_live: list[dict], state: dict) -> bool:
    """
    Returns True if there is an Appointment activity due in the future (not completed),
    or if state['mode']=='scheduled' and appt_due_utc is still in the future.
    """
    now_utc = _dt.now(_tz.utc)

    # A) Use live activities (most reliable)
    for a in acts_live or []:
        nm = (a.get("activityName") or a.get("name") or "").strip().lower()
        t  = str(a.get("activityType") or "").strip()
        cat = (a.get("category") or "").strip().lower()
        due = a.get("dueDate") or a.get("completedDate") or a.get("activityDate")
        try:
            due_dt = _dt.fromisoformat(str(due).replace("Z", "+00:00"))
        except Exception:
            due_dt = None

        is_appt = (nm == "appointment") or (t == "2") or (t == 2)
        not_completed = cat != "completed"

        if is_appt and due_dt and due_dt > now_utc and not_completed:
            return True

    # B) Fall back to state
    appt_due_utc = state.get("appt_due_utc")
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
    Return (activity_id, due_iso) for a new 'Customer Scheduled Appointment'.
    Falls back to fetching activity-history directly if the provided object
    doesn't expose the scheduled bucket.
    """
    last_seen = (state or {}).get("last_appt_activity_id")

    def _scan(items):
        for a in items or []:
            name = (a.get("activityName") or a.get("name") or "").strip().lower()
            if "customer scheduled appointment" in name:
                aid = str(a.get("activityId") or a.get("id") or "")
                if aid and aid != last_seen:
                    due = a.get("dueDate") or a.get("completedDate") or a.get("activityDate")
                    return aid, due
        return None, None

    # 1) If dict with scheduledActivities
    if isinstance(acts_live, dict):
        appt_id, due = _scan(acts_live.get("scheduledActivities") or [])
        if appt_id:
            return appt_id, due

    # 2) If list (flat), just scan it
    if isinstance(acts_live, list):
        appt_id, due = _scan(acts_live)
        if appt_id:
            return appt_id, due

    # 3) Fallback: pull a fresh activity-history so we can see scheduledActivities for sure
    try:
        from fortellis import get_activity_history_v1  # you'll add this small wrapper if not present
        fresh = get_activity_history_v1(token, subscription_id, opp_id, customer_id)
        appt_id, due = _scan((fresh or {}).get("scheduledActivities") or [])
        if appt_id:
            return appt_id, due
    except Exception as e:
        log.warning("KBB ICO: fallback activity-history fetch failed: %s", e)

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
    dealer_phone = rt.get("phone") or ""
    dealer_addr  = rt.get("address") or ""
    dealer_site  = rt.get("website") or "https://pattersonautos.com"

    sig = _tw.dedent(f"""
    <p>Patti<br/>
    {rooftop_name}<br/>
    {dealer_addr}<br/>
    {dealer_phone}</p>
    """).strip()

    return sig

def normalize_patti_body(body_html: str) -> str:
    """Tidy GPT output: strip stray Patti signatures and collapse whitespace."""
    body_html = _re.sub(r'(?is)(?:\n\s*)?patti\s*(?:<br/?>|\r?\n)+.*?$', '', body_html.strip())
    # collapse double spaces around <p> boundaries
    body_html = _re.sub(r'\n{2,}', '\n', body_html)
    return body_html


def compose_kbb_convo_body(rooftop_name: str, cust_first: str, customer_message: str, booking_link_text="Schedule Your Visit"):
    return _tw.dedent(f"""
    You are Patti, the virtual assistant for {rooftop_name}. This thread is about a Kelley Blue Book® Instant Cash Offer (ICO).
    Keep replies short, warm, and human—no corporate tone.
    Write HTML with simple <p> paragraphs (no lists). Always:
    - Begin with: "Hi {cust_first}," (exactly).
    - Acknowledge the customer's note in 1-2 concise sentences.
    - If relevant, mention their Kelley Blue Book® Instant Cash Offer naturally in your reply.
    - If the customer has not already specified or booked the meeting, close with a friendly nudge to pick a day/time (no links) — a short booking line with the link will be appended automatically.
    - You may remind them to bring title, ID, and keys if appropriate, and if you haven't already done it in an earlier message
    - No extra signatures; we will append yours.
    - Keep to 2–4 short paragraphs max.

    Customer said:
    \"\"\"{customer_message}\"\"\"

    Produce only the HTML body (no subject).
    """).strip()
_CTA_ANCHOR_RE = _re.compile(r'(?is)<a[^>]*>\s*Schedule\s+Your\s+Visit\s*</a>')
_RAW_TOKEN_RE  = _re.compile(r'(?i)<\{LegacySalesApptSchLink\}>')
_ANY_SCHED_LINE_RE = _re.compile(r'(?i)(reserve your time|schedule (an )?appointment|schedule your visit)[:\s]*', _re.I)

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
    comments = opportunity.get("messages") or opportunity.get("completedActivitiesTesting") or []
    # Look for our tagged comment body
    for c in comments:
        txt = (c.get("comments") or c.get("notes") or "")
        if STATE_TAG in txt:
            try:
                return json.loads(re.sub(r".*?\[PATTI_KBB_STATE\]\s*", "", txt, flags=re.S))
            except Exception:
                pass
    # default
    return {"mode": "cadence", "last_customer_msg_at": None, "last_agent_msg_at": None}

def _save_state_comment(token, subscription_id, opportunity_id, state: dict):
    if not opportunity_id:
        log.warning("skip state comment: missing opportunity_id")
        return
    payload = f"{STATE_TAG} {json.dumps(state, ensure_ascii=False)}"
    add_opportunity_comment(token, subscription_id, opportunity_id, payload)


def customer_has_replied(opportunity: dict, token: str, subscription_id: str, state: dict | None = None):
    """
    Returns (has_replied, last_customer_ts_iso, last_inbound_activity_id)
    Only returns True for a *new* inbound since the state's last seen inbound id/timestamp.
    """
    state = state or {}
    last_seen_ts = state.get("last_customer_msg_at") or ""
    last_seen_id = state.get("last_inbound_activity_id") or ""

    opportunity_id = opportunity.get("opportunityId") or opportunity.get("id")
    customer = (opportunity.get("customer") or {})
    customer_id = customer.get("id")
    if not opportunity_id:
        log.error("customer_has_replied: missing opportunity_id")
        return False, None, None

    acts = search_activities_by_opportunity(
        opportunity_id=opportunity_id,
        token=token,
        dealer_key=subscription_id,
        page=1, page_size=50,
        customer_id=customer_id,
    ) or []

    def _is_inbound(a: dict) -> bool:
        nm = (a.get("activityName") or "").strip().lower()
        # Your CRM logs *customer email replies* as "Read Email"
        if nm == "read email":
            return True
        # Never treat sends as inbound
        if "send email" in nm or "send email/letter" in nm or nm.startswith("send "):
            return False
        return False  # keep strict

    def _ts(a: dict) -> str:
        # Prefer completedDate first
        return (a.get("completedDate")
                or a.get("createdDate")
                or a.get("createdOn")
                or a.get("modifiedDate")
                or "").strip()

    # Walk newest→oldest. Return only if it's *newer* than we’ve seen.
    for a in acts:
        if not _is_inbound(a):
            continue
        aid = str(a.get("activityId") or a.get("id") or "")
        ats = _ts(a)
        if last_seen_id and aid == last_seen_id:
            # already processed
            continue
        if last_seen_ts:
            try:
                ats_dt = _dt.fromisoformat(ats.replace("Z","+00:00"))
                lst_dt = _dt.fromisoformat(str(last_seen_ts).replace("Z","+00:00"))
                if ats_dt <= lst_dt:
                    continue
            except Exception:
                # if we can't parse, be conservative: require id mismatch as signal
                if aid == last_seen_id:
                    continue
        return True, ats or None, aid or None

    return False, None, None



def process_kbb_ico_lead(opportunity, lead_age_days, rooftop_name, inquiry_text,
                         token, subscription_id, SAFE_MODE=False, rooftop_sender=None):
    opp_id = opportunity.get("opportunityId") or opportunity.get("id")
    created_iso = opportunity.get("createdDate") or opportunity.get("created_on")

    # Load state (from tagged comments) and normalize defaults
    state = _load_state_from_comments(opportunity)
    state.setdefault("mode", "cadence")
    state.setdefault("last_template_day_sent", None)
    state.setdefault("last_template_sent_at", None)
    state.setdefault("last_customer_msg_at", None)
    state.setdefault("last_agent_msg_at", None)
    state.setdefault("nudge_count", 0)
    state.setdefault("last_inbound_activity_id", None)

    # Before you compute mode/cadence, pull LIVE activities
    opp_id = opportunity.get("opportunityId") or opportunity.get("id")
    customer_id = (opportunity.get("customer") or {}).get("id")
    acts_live = _fetch_activities_live(opp_id, customer_id, token, subscription_id)

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

    # ✅ Always short-circuit if a new customer-booked appointment exists
    if _short_circuit_if_booked(
        opportunity, acts_live, state,
        token=token, subscription_id=subscription_id,
        rooftop_name=rooftop_name, SAFE_MODE=SAFE_MODE, rooftop_sender=rooftop_sender
    ):
        return

    # === If customer already declined earlier, stop everything ==============
    if state.get("mode") == "closed_declined":
        log.info("KBB ICO: declined → skip all outreach (opp=%s)", opp_id)
        return
    
    # === If an appointment is booked/upcoming, stop all outreach =====
    if state.get("mode") == "scheduled" or _has_upcoming_appt(acts_live, state):
        log.info("KBB ICO: upcoming appointment detected → skip nudges & cadence (opp=%s)", opp_id)
        # (Optional) keep state normalized to 'scheduled'
        state["mode"] = "scheduled"
        _save_state_comment(token, subscription_id, opp_id, state)
        return

    # === Detect customer-booked appointment via booking link (pre-convo) ===
    appt_id, appt_due_iso = _find_new_customer_scheduled_appt(
        acts_live, state,
        token=token, subscription_id=subscription_id,
        opp_id=opp_id, customer_id=customer_id
    )
    log.info("KBB ICO: booked-appt scan → id=%s due=%s", appt_id, appt_due_iso)
    if appt_id and appt_due_iso:
        try:
            # Human time in local tz
            try:
                dt_local = _dt.fromisoformat(str(appt_due_iso).replace("Z", "+00:00"))
            except Exception:
                dt_local = None
            if dt_local and dt_local.tzinfo:
                appt_human = _fmt_local_human(dt_local, tz_name="America/Los_Angeles")
                due_dt_iso_utc = dt_local.astimezone(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                appt_human = str(appt_due_iso)
                due_dt_iso_utc = _dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            # Compose a deterministic “thanks for booking” email (no GPT)
            cust_first = (opportunity.get('customer', {}) or {}).get('firstName') or "there"
            subject = f"Re: Appointment confirmed for {appt_human}"
            body_html = f"""
                <p>Hi {cust_first},</p>
                <p>Thanks for booking — we’ll see you on <strong>{appt_human}</strong> at {rooftop_name}.</p>
                <p>Please bring your title, ID, and keys. If you need to change your time, use this link: <{{LegacySalesApptSchLink}}></p>
            """.strip()
            body_html = normalize_patti_body(body_html)
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
                return

            send_opportunity_email_activity(
                token, subscription_id, opp_id,
                sender=rooftop_sender,
                recipients=recipients, carbon_copies=[],
                subject=subject, body_html=body_html, rooftop_name=rooftop_name
            )

            # Persist “scheduled” state so Patti stops nudges/templates
            state["mode"] = "scheduled"
            state["last_appt_activity_id"] = appt_id
            state["appt_due_utc"] = due_dt_iso_utc
            state["appt_due_local"] = appt_human
            state["nudge_count"] = 0
            state["last_agent_msg_at"] = _dt.now(_tz.utc).isoformat()
            _save_state_comment(token, subscription_id, opp_id, state)

            # Flip CRM subStatus → Appointment Set
            try:
                from fortellis import set_opportunity_substatus
                resp = set_opportunity_substatus(token, subscription_id, opp_id, sub_status="Appointment Set")
                log.info("SubStatus update response: %s", getattr(resp, "status_code", "n/a"))
            except Exception as e:
                log.warning("set_opportunity_substatus failed: %s", e)

            return  # stop here once we’ve handled the scheduled appointment

        except Exception as e:
            log.warning("Customer-booked appt handler failed: %s", e)
            # fall through to normal flow if something goes wrong

    # === If customer already declined earlier, stop everything ==============
    if state.get("mode") == "closed_declined":
        log.info("KBB ICO: declined → skip all outreach (opp=%s)", opp_id)
        return

    # === If an appointment is booked/upcoming, stop all outreach =====
    if state.get("mode") == "scheduled" or _has_upcoming_appt(acts_live, state):
        log.info("KBB ICO: upcoming appointment detected → skip nudges & cadence (opp=%s)", opp_id)
        state["mode"] = "scheduled"
        _save_state_comment(token, subscription_id, opp_id, state)
        return


    ## NOTE: pass state into the detector so it can ignore already-seen inbounds
    has_reply, last_cust_ts, last_inbound_id = customer_has_replied(
        opportunity, token, subscription_id, state
    )

    # Only flip to convo when we truly have a new inbound with a timestamp
    # Compute the last agent send time from state (if present)
    # Prefer real Fortellis send time; fall back to state if missing
    last_agent_dt_live = _last_agent_send_dt(acts_live)
    last_agent_dt = last_agent_dt_live
    if (last_agent_dt is None) and state.get("last_agent_msg_at"):
        try:
            last_agent_dt = _dt.fromisoformat(str(state["last_agent_msg_at"]).replace("Z","+00:00"))
        except Exception:
            last_agent_dt = None

    # ✅ Convo mode if and only if there is a READ EMAIL newer than Patti's send
    if has_reply or _has_new_read_email_since(acts_live, last_agent_dt):
        
        state["mode"] = "convo"
        state["nudge_count"] = 0
        if has_reply and last_cust_ts:
            state["last_customer_msg_at"] = last_cust_ts
        if last_inbound_id:
            state["last_inbound_activity_id"] = last_inbound_id
        _save_state_comment(token, subscription_id, opp_id, state)

        # ✅ NEW: Fetch the latest inbound activity and use its top reply as inquiry_text
        if last_inbound_id:
            try:
                from fortellis import get_activity_by_id_v1
                full = get_activity_by_id_v1(last_inbound_id, token, subscription_id)
                latest_body_raw = ((full.get("message") or {}).get("body") or "").strip()
                latest_body = _top_reply_only(latest_body_raw)
                if latest_body:
                    inquiry_text = latest_body
                    log.info("KBB ICO: using inbound top-reply (len=%d): %r",
                             len(latest_body), latest_body[:120])
                    
                    # Keep the thread subject to avoid subject churn
                    thread_subject = ((full.get("message") or {}).get("subject") or "").strip()
                    
                    def _clean_subject(s: str) -> str:
                        # strip RE:/FWD: prefixes and banners like [CAUTION]
                        s = _re.sub(r'^\s*\[.*?\]\s*', '', s or '', flags=_re.I)       # e.g., [CAUTION]
                        s = _re.sub(r'^\s*(re|fwd)\s*:\s*', '', s, flags=_re.I)        # leading RE:/FWD:
                        return s.strip()
                    
                    reply_subject = f"Re: {_clean_subject(thread_subject)}" if thread_subject else "Re:"


            except Exception as e:
                log.warning("Could not load inbound activity %s: %s", last_inbound_id, e)

        # Detect decline from customer's top reply
        declined = _is_decline(inquiry_text)
        if declined:
            log.info("KBB ICO: decline detected in inbound: %r", inquiry_text[:120])

        created_appt_ok = False
        appt_human = None
        dt_local = None

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
                    add_opportunity_comment(
                        token, subscription_id, opp_id,
                        f"[Patti] Auto-scheduled appointment for {appt_human} (local)."
                    )
                except Exception as e:
                    log.warning("KBB ICO: failed to auto-schedule proposed time: %s", e)


        # === COMPOSE + SEND ================================================
        cust_first = (opportunity.get('customer', {}) or {}).get('firstName') or "there"

        # ---------- DECLINED ----------
        if declined:
            subject = reply_subject  # keep thread subject
            body_html = f"""
                <p>Hi {cust_first},</p>
                <p>Thanks for letting me know — I’ve marked your Kelley Blue Book® Instant Cash Offer as not interested. We won’t send further emails.</p>
                <p>If you change your mind later, just reply here and I can pick it back up.</p>
            """.strip()

            # Normalize (NO CTA) + footer + subject guard
            body_html = normalize_patti_body(body_html)
            body_html = _PREFS_RE.sub("", body_html).strip()
            body_html = body_html + build_patti_footer(rooftop_name)
            if not subject.lower().startswith("re:"):
                subject = "Re: " + subject

            # 1) Note BEFORE inactivating
            try:
                add_opportunity_comment(
                    token, subscription_id, opp_id,
                    "[Patti] Customer declined the KBB ICO — marking inactive."
                )
            except Exception as e:
                log.warning("Decline note failed (pre-inactive): %s", e)

            # 2) Resolve recipient + send
            cust = (opportunity.get("customer") or {})
            email = cust.get("emailAddress") or ((cust.get("emails") or [{}])[0].get("address"))
            if not email:
                email = (opportunity.get("_lead", {}) or {}).get("email_address")
            recipients = [email] if (email and not SAFE_MODE) else [TEST_TO]
            if not recipients:
                log.warning("No recipient; skip send for opp=%s", opp_id)
                return

            send_opportunity_email_activity(
                token, subscription_id, opp_id,
                sender=rooftop_sender,
                recipients=recipients, carbon_copies=[],
                subject=subject, body_html=body_html, rooftop_name=rooftop_name
            )

            # 3) Save state BEFORE inactive
            state["mode"] = "closed_declined"
            state["nudge_count"] = 0
            state["last_agent_msg_at"] = _dt.now(_tz.utc).isoformat()
            try:
                _save_state_comment(token, subscription_id, opp_id, state)
            except Exception as e:
                log.warning("save_state_comment failed (pre-inactive): %s", e)

            # 4) Inactivate LAST
            try:
                from fortellis import set_opportunity_inactive
                resp = set_opportunity_inactive(
                    token, subscription_id, opp_id,
                    sub_status="Not In Market",
                    comments="Customer declined — set inactive by Patti"
                )
                log.info("Set inactive response: %s", getattr(resp, "status_code", "n/a"))
            except Exception as e:
                log.warning("set_opportunity_inactive failed: %s", e)

            return  # important: stop here

        # ---------- APPOINTMENT CONFIRMATION ----------
        elif created_appt_ok and appt_human:
            subject = f"Re: Your visit on {appt_human}"
            body_html = f"""
                <p>Hi {cust_first},</p>
                <p>Your appointment is confirmed for <strong>{appt_human}</strong> at {rooftop_name}.</p>
                <p>Please bring your title, ID, and keys. If you need to change your time, use this link: <{{LegacySalesApptSchLink}}></p>
            """.strip()

            # Normalize + footer + subject guard (no extra CTA here)
            body_html = normalize_patti_body(body_html)
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
                return

            send_opportunity_email_activity(
                token, subscription_id, opp_id,
                sender=rooftop_sender,
                recipients=recipients, carbon_copies=[],
                subject=subject, body_html=body_html, rooftop_name=rooftop_name
            )

            # Persist scheduled state so future runs short-circuit
            state["mode"] = "scheduled"
            state["appt_due_utc"]   = due_dt_iso_utc
            state["appt_due_local"] = appt_human
            state["nudge_count"]    = 0
            state["last_agent_msg_at"] = _dt.now(_tz.utc).isoformat()
            _save_state_comment(token, subscription_id, opp_id, state)

            # Flip CRM subStatus → Appointment Set
            try:
                from fortellis import set_opportunity_substatus
                resp = set_opportunity_substatus(token, subscription_id, opp_id, sub_status="Appointment Set")
                log.info("SubStatus update response: %s", getattr(resp, "status_code", "n/a"))
            except Exception as e:
                log.warning("set_opportunity_substatus failed: %s", e)

            return

        # ---------- NORMAL GPT CONVO ----------
        else:
            from gpt import run_gpt
            # Include full conversation thread for context
            msgs = opportunity.get("messages", [])
            prompt = f"""
            generate next Patti reply for a Kelley Blue Book® Instant Cash Offer conversation.
            Here is the full conversation so far (as a python list of dicts):
            {msgs}
            
            Most recent customer message:
            \"\"\"{inquiry_text}\"\"\"
            """
            reply = run_gpt(
                prompt,
                customer_name=cust_first,
                rooftop_name=rooftop_name,
                prevMessages=True,
                persona="kbb_ico",
                kbb_ctx={"offer_valid_days": 7, "exclude_sunday": True},
            )

            subject   = reply_subject  # keep thread subject; ignore GPT subject
            body_html = (reply.get("body") or "")

            # Normalize + add CTA only for this path
            body_html = normalize_patti_body(body_html)
            #body_html = enforce_standard_schedule_sentence(body_html)
            body_html = append_soft_schedule_sentence(body_html, rooftop_name)
            body_html = _PREFS_RE.sub("", body_html).strip()
            body_html = body_html + build_patti_footer(rooftop_name)
            if not subject.lower().startswith("re:"):
                subject = "Re: " + subject

            # Resolve recipient and send
            cust = (opportunity.get("customer") or {})
            email = cust.get("emailAddress") or ((cust.get("emails") or [{}])[0].get("address"))
            if not email:
                email = (opportunity.get("_lead", {}) or {}).get("email_address")
            recipients = [email] if (email and not SAFE_MODE) else [TEST_TO]
            if not recipients:
                log.warning("No recipient; skip send for opp=%s", opp_id)
                return

            add_opportunity_comment(
                token, subscription_id, opp_id,
                f"[Patti] Replying to customer (convo mode) → to {(email or 'TEST_TO')}"
            )

            import re as _re2
            m = _re2.search(r".{0,80}<\{LegacySalesApptSchLink.*?\}.{0,80}", body_html, flags=_re2.S)
            log.info("Scheduler token snippet: %r", m.group(0) if m else "none")

            send_opportunity_email_activity(
                token, subscription_id, opp_id,
                sender=rooftop_sender,
                recipients=recipients, carbon_copies=[],
                subject=subject, body_html=body_html, rooftop_name=rooftop_name
            )

            # Save convo state (NOT scheduled)
            state["last_agent_msg_at"] = _dt.now(_tz.utc).isoformat()
            _save_state_comment(token, subscription_id, opp_id, state)
            return

    # ===== NUDGE LOGIC (customer went dark AFTER a reply) =====
    # If we are already in convo mode, no new inbound detected now, and enough time has passed → send a nudge
    if state.get("mode") == "convo":
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
                    messages history (python list of dicts):
                    {opportunity.get('messages', [])}
                    """

                    reply = run_gpt(
                        prompt,
                        customer_name=cust_first,
                        rooftop_name=rooftop_name,
                        prevMessages=True,
                        persona="kbb_ico",
                        kbb_ctx={"offer_valid_days": 7, "exclude_sunday": True},
                    )

                    subject   = reply.get("subject") or "Still interested in your Instant Cash Offer?"
                    body_html = reply.get("body") or ""
                    body_html = normalize_patti_body(body_html)
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
                        add_opportunity_comment(
                            token, subscription_id, opp_id,
                            f"[Patti] Sending Nudge #{nudge_count + 1} after silence → to {(email or 'TEST_TO')}"
                        )
                        send_opportunity_email_activity(
                            token, subscription_id, opp_id,
                            sender=rooftop_sender,
                            recipients=recipients, carbon_copies=[],
                            subject=subject, body_html=body_html, rooftop_name=rooftop_name
                        )
                        state["last_agent_msg_at"] = _dt.now(_tz.utc).isoformat()
                        state["nudge_count"] = nudge_count + 1
                        _save_state_comment(token, subscription_id, opp_id, state)
                    else:
                        log.warning("No recipient for nudge; opp=%s", opp_id)
                    return

        # In convo mode but not time for a nudge yet → do nothing this cycle
        log.info("KBB ICO: convo mode, no nudge due. Skipping send.")
        return

    if state.get("mode") == "convo":
        log.info("KBB ICO: persisted convo mode — skip cadence.")
        return
    # ===== Still in cadence (never replied) =====
    state["mode"] = "cadence"
    _save_state_comment(token, subscription_id, opp_id, state)

    # Offer-window override (if expired, jump to Day 08/09 track)
    expired = _ico_offer_expired(created_iso, exclude_sunday=True)
    effective_day = max(1, (lead_age_days or 0) + 1)
    if expired and lead_age_days < 8:
        effective_day = 8

    plan = events_for_day(effective_day)
    if not plan:
        return

    if state.get("last_template_day_sent") == effective_day:
        log.info("KBB ICO: skipping Day %s (already sent)", effective_day)
        return

    # Load email template
    tpl_key = plan.get("email_template_day")
    html = TEMPLATES.get(tpl_key)
    if not html:
        log.warning("KBB ICO: missing template for day key=%r", tpl_key)
        return

    # Rooftop info
    from rooftops import ROOFTOP_INFO
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

    # Log + send
    add_opportunity_comment(
        token, subscription_id, opp_id,
        f"KBB ICO Day {effective_day}: sending template {tpl_key} to "
        f"{('TEST_TO' if SAFE_MODE else email_addr)}."
    )
    send_opportunity_email_activity(
        token, subscription_id, opp_id,
        sender=rooftop_sender,
        recipients=recipients, carbon_copies=[],
        subject=subject, body_html=body_html, rooftop_name=rooftop_name
    )

    # Persist idempotency for cadence sends
    state["last_template_day_sent"] = effective_day
    state["last_template_sent_at"]  = _dt.now(_tz.utc).isoformat()
    _save_state_comment(token, subscription_id, opp_id, state)

    # Phone/Text tasks (unchanged)
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



def _customer_has_text_consent(opportunity) -> bool:
    # TODO: look at your CRM/TCPA field once available
    return bool((opportunity.get("customer",{}) or {}).get("tcpConsent", False))

