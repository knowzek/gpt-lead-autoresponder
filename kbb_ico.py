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

_PREFS_RE = _re.compile(r'(?is)\s*to stop receiving these messages.*?(?:</p>|$)')

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
    - Acknowledge the customer's note in one concise sentence.
    - State clearly that you're helping with their Kelley Blue Book® Instant Cash Offer.
    - Remind them to bring title, ID, and keys.
    - Do NOT propose appointment times; the system will add the standard scheduling sentence.
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




def replace_or_append_booking_cta(body_html: str, rooftop_name: str) -> str:
    rt = (ROOFTOP_INFO.get(rooftop_name) or {})
    booking_link = rt.get("booking_link") or rt.get("scheduler_url") or ""

    # 1) Token already present? Replace it with real link or clickable token.
    if _LEGACY_TOKEN_RE.search(body_html):
        if booking_link:
            return _LEGACY_TOKEN_RE.sub(booking_link, body_html)
        return (_LEGACY_TOKEN_RE.sub('<{LegacySalesApptSchLink}>', body_html)
                .replace('<{LegacySalesApptSchLink}>',
                         '<a href="<{LegacySalesApptSchLink}>">Schedule Your Visit</a>'))

    # 2) No token. If plaintext "Schedule Your Visit" exists but isn't a link, wrap the FIRST one.
    if ("Schedule Your Visit" in body_html 
        and not re.search(r'(?i)<a[^>]*>\s*Schedule Your Visit\s*</a>', body_html)):
        href = booking_link or '<{LegacySalesApptSchLink}>'
        return re.sub(r"Schedule Your Visit",
                      f'<a href="{href}">Schedule Your Visit</a>',
                      body_html, count=1)

    # 3) Otherwise, append a proper linked CTA block.
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
        return (a.get("createdDate") or a.get("createdOn") or a.get("modifiedDate") or "").strip()

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

    ## NOTE: pass state into the detector so it can ignore already-seen inbounds
    has_reply, last_cust_ts, last_inbound_id = customer_has_replied(
        opportunity, token, subscription_id, state
    )

    # Only flip to convo when we truly have a new inbound with a timestamp
    if has_reply and last_cust_ts:
        state["mode"] = "convo"
        state["last_customer_msg_at"] = last_cust_ts
        if last_inbound_id:
            state["last_inbound_activity_id"] = last_inbound_id
    
        # 3) Reset nudge counter on real inbound (so a fresh silence starts clean)
        state["nudge_count"] = 0
    
        _save_state_comment(token, subscription_id, opp_id, state)

        # Compose natural reply with GPT (ICO persona)
        from gpt import run_gpt
        cust_first = (opportunity.get('customer', {}) or {}).get('firstName') or "there"
        prompt = compose_kbb_convo_body(rooftop_name, cust_first, inquiry_text)

        reply = run_gpt(
            prompt,
            customer_name=cust_first,
            rooftop_name=rooftop_name,
            prevMessages=True,
            persona="kbb_ico",
            kbb_ctx={"offer_valid_days": 7, "exclude_sunday": True},
        )

        subject   = (reply.get("subject") or f"Re: Your {rooftop_name} Instant Cash Offer")
        body_html = (reply.get("body") or "")
        body_html = normalize_patti_body(body_html)
        body_html = enforce_standard_schedule_sentence(body_html)
        body_html = _PREFS_RE.sub("", body_html).strip()
        body_html = body_html + build_patti_footer(rooftop_name)

        if not subject.lower().startswith("re:"):
            subject = "Re: " + subject

        # Resolve recipient
        cust = (opportunity.get("customer") or {})
        email = cust.get("emailAddress")
        if not email:
            emails = cust.get("emails") or []
            email = (emails[0] or {}).get("address") if emails else None
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
        send_opportunity_email_activity(
            token, subscription_id, opp_id,
            sender=rooftop_sender,
            recipients=recipients, carbon_copies=[],
            subject=subject, body_html=body_html, rooftop_name=rooftop_name
        )

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
                    body_html = enforce_standard_schedule_sentence(body_html)
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

    # ===== Still in cadence (never replied) =====
    state["mode"] = "cadence"
    _save_state_comment(token, subscription_id, opp_id, state)

    # Offer-window override (if expired, jump to Day 08/09 track)
    expired = _ico_offer_expired(created_iso, exclude_sunday=True)
    effective_day = lead_age_days
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
    body_html = body_html + build_patti_footer(rooftop_name)

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

