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
    <p>— Patti<br/>
    {rooftop_name}<br/>
    {dealer_addr}<br/>
    {dealer_phone}</p>
    """).strip()

    prefs = _tw.dedent(f"""
    <p style="margin-top:12px;">
    To stop receiving these messages, visit
    <a href="{dealer_site}/preferences">Communication Preferences</a>.
    </p>
    """).strip()

    return sig + prefs

def normalize_patti_body(body_html: str) -> str:
    """Tidy GPT output: strip stray Patti signatures and collapse whitespace."""
    body_html = _re.sub(r'(?is)(?:\n\s*)?patti\s*(?:<br/?>|\r?\n)+.*?$', '', body_html.strip())
    # collapse double spaces around <p> boundaries
    body_html = _re.sub(r'\n{2,}', '\n', body_html)
    return body_html

def compose_kbb_convo_body(rooftop_name: str, cust_first: str, customer_message: str, booking_link_text="Schedule Your Visit"):
    """
    Build the *prompt string* Patti should use for KBB replies (clean, friendly, short).
    Mirrors your “general Patti” tone: greeting, value, CTA, clean spacing.
    """
    return _tw.dedent(f"""
    You are Patti, the virtual assistant for {rooftop_name}. Keep replies short, warm, and human—no corporate tone.
    Write HTML with simple <p> paragraphs (no lists). Always:
    - Begin with: "Hi {cust_first}," (exactly).
    - Acknowledge the customer's note in one concise sentence.
    - Offer help and next step without hard-scheduling times. Do NOT invent specific time slots.
    - Include ONE booking line using the provided booking CTA (we will inject it).
    - No extra signatures; we will append yours.
    - Keep to 2–4 short paragraphs max.

    Customer said:
    \"\"\"{customer_message}\"\"\"

    Produce only the HTML body (no subject).
    """).strip()


_LEGACY_TOKEN_RE = _re.compile(r"(?i)<\{LegacySalesApptSchLink\}>")

def render_booking_cta(rooftop_name: str, link_text: str = "Schedule Your Visit") -> str:
    """Return a CTA <p> with either a hard booking_link or the CRM's dynamic token."""
    rt = (ROOFTOP_INFO.get(rooftop_name) or {})
    booking_link = rt.get("booking_link") or rt.get("scheduler_url") or ""
    if booking_link:
        return f'<p><a href="{booking_link}">{link_text}</a></p>'
    # leave the CRM token so eLeads can expand it (note the escaped braces)
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


def customer_has_replied(opportunity: dict, token: str, subscription_id: str) -> tuple[bool, str | None]:
    """Returns (has_replied, last_customer_ts_iso)."""
    opportunity_id = opportunity.get("opportunityId") or opportunity.get("id")
    customer = (opportunity.get("customer") or {})
    customer_id = customer.get("id")

    if not opportunity_id:
        log.error("customer_has_replied: missing opportunity_id")
        return False, None

    log.info("KBB ICO: activity search opp=%s cust=%s sub=%s", opportunity_id, customer_id, subscription_id)

    acts = search_activities_by_opportunity(
        opportunity_id=opportunity_id,
        token=token,
        dealer_key=subscription_id,
        page=1, page_size=50,
        customer_id=customer_id,
    ) or []

    # DEBUG: surface a few activities to tune predicates
    for a in acts[:10]:
        log.info("ACT id=%s type=%r name=%r dir=%r by=%r subj=%r",
                 a.get("activityId") or a.get("id"),
                 a.get("activityType"),
                 a.get("activityName"),
                 a.get("direction"),
                 a.get("createdBy"),
                 ((a.get("message") or {}).get("subject") or a.get("subject")))

    def _is_inbound(a: dict) -> bool:
        t  = a.get("activityType")
        nm = (a.get("activityName") or "").strip().lower()
        dr = (a.get("direction") or "").strip().lower()
        cb = (a.get("createdBy") or "").strip().lower()

        # Treat "Read Email" (v1) and message/email-ish types as inbound
        read_email_name = (nm == "read email")
        message_like = (
            t in (3, 20, 21) or
            str(t) in {"3", "20", "21"} or
            "message" in nm or "email" in nm or "reply" in nm
        )
        inbound_flag = dr in {"inbound", "incoming", "from customer", "received"} or dr == ""

        # consider it inbound if it's read-email OR message-like and looks inbound
        return read_email_name or (message_like and inbound_flag and cb not in {"patti","system"})

    last_ts = None
    for a in acts:
        if _is_inbound(a):
            last_ts = a.get("createdDate") or a.get("createdOn") or a.get("modifiedDate")
            return True, last_ts

    return False, last_ts


def process_kbb_ico_lead(opportunity, lead_age_days, rooftop_name, inquiry_text,
                         token, subscription_id, SAFE_MODE=False, rooftop_sender=None):
    opp_id = opportunity.get("opportunityId") or opportunity.get("id")
    created_iso = opportunity.get("createdDate") or opportunity.get("created_on")

    # Load state and see if customer replied
    state = _load_state_from_comments(opportunity)
    state.setdefault("mode", "cadence")
    state.setdefault("last_template_day_sent", None)
    state.setdefault("last_template_sent_at", None)
    has_reply, last_cust_ts = customer_has_replied(opportunity, token, subscription_id)


    if has_reply:
        # flip to convo mode & persist
        state["mode"] = "convo"
        state["last_customer_msg_at"] = last_cust_ts
        _save_state_comment(token, subscription_id, opp_id, state)
    
        # === Compose natural reply with GPT (ICO persona) using cleaner tone ===
        from gpt import run_gpt
        cust_first = (opportunity.get('customer',{}) or {}).get('firstName') or "there"
        prompt = compose_kbb_convo_body(rooftop_name, cust_first, inquiry_text)
        
        reply = run_gpt(
            prompt,
            customer_name=cust_first,
            rooftop_name=rooftop_name,
            prevMessages=True,
            persona="kbb_ico",
            kbb_ctx={"offer_valid_days": 7, "exclude_sunday": True},
        )
        
        # Normalize run_gpt output (dict vs string)
        if isinstance(reply, dict):
            subject   = reply.get("subject") or f"Re: Your {rooftop_name} Instant Cash Offer"
            body_html = reply.get("body") or ""
        else:
            subject   = f"Re: Your {rooftop_name} Instant Cash Offer"
            body_html = str(reply)
        
        # Clean body + ensure single CTA
        body_html = normalize_patti_body(body_html)
        body_html = replace_or_append_booking_cta(body_html, rooftop_name)
        
        # De-dupe any existing preferences line that templates/CRM might have added
        body_html = _PREFS_RE.sub("", body_html).strip()
        
        # Append Patti’s footer
        body_html = body_html + build_patti_footer(rooftop_name)
        
        # Ensure subject has one "Re:" prefix max
        if not subject.lower().startswith("re:"):
            subject = "Re: " + subject

        # ------------------------------------------------------------------------

    
        # Determine recipient safely
        cust = (opportunity.get("customer", {}) or {})
        email = cust.get("emailAddress")
        if not email:
            emails = cust.get("emails") or []
            email = (emails[0] or {}).get("address") if emails else None
        if not email:
            email = (opportunity.get("_lead", {}) or {}).get("email_address")  # if you stash it
        recipients = [email] if (email and not SAFE_MODE) else [TEST_TO]
        if not recipients or (not SAFE_MODE and recipients == [TEST_TO]):
            log.warning("No recipient email found; skipping send for opp=%s", opp_id)
            return
    
        # Optional breadcrumb comment
        add_opportunity_comment(
            token, subscription_id, opp_id,
            f"[Patti] Replying to customer (convo mode) → to {(email or 'TEST_TO')}"
        )
    
        # Send reply
        send_opportunity_email_activity(
            token, subscription_id, opp_id,
            sender=rooftop_sender,
            recipients=recipients, carbon_copies=[],
            subject=subject, body_html=body_html, rooftop_name=rooftop_name
        )
    
        # update agent timestamp and persist state
        state["last_agent_msg_at"] = _dt.now(_tz.utc).isoformat()
        _save_state_comment(token, subscription_id, opp_id, state)
        return


    # Still in cadence mode (no reply)
    state["mode"] = "cadence"
    _save_state_comment(token, subscription_id, opp_id, state)
    
    # Offer-window override (if expired jump to Day 08/09 track)
    expired = _ico_offer_expired(created_iso, exclude_sunday=True)
    effective_day = lead_age_days
    if expired and lead_age_days < 8:
        effective_day = 8  # or 9 based on your PDF plan
    
    # Compute plan for this day
    plan = events_for_day(effective_day)
    if not plan:
        return
    
    # 🚫 Skip if we already sent this day
    if state.get("last_template_day_sent") == effective_day:
        log.info("KBB ICO: skipping Day %s (already sent)", effective_day)
        return
    
    # --- Load template safely -------------------------------------------------
    tpl_key = plan.get("email_template_day")  # e.g., 0 / 1 / 2 (match your TEMPLATES keys)
    html = TEMPLATES.get(tpl_key)
    if not html:
        log.warning("KBB ICO: missing template for day key=%r", tpl_key)
        return
    
    # --- Rooftop address ------------------------------------------------------
    from rooftops import ROOFTOP_INFO
    rooftop_addr = ((ROOFTOP_INFO.get(rooftop_name, {}) or {}).get("address") or "")
    
    # --- Salesperson (primary) ------------------------------------------------
    sales_team = (opportunity.get("salesTeam") or [])
    sp = next((m for m in sales_team if m.get("isPrimary")), (sales_team[0] if sales_team else {}))
    salesperson_name  = " ".join(filter(None, [sp.get("firstName", ""), sp.get("lastName", "")])).strip()
    salesperson_phone = (sp.get("phone") or sp.get("mobile") or "")
    salesperson_email = (sp.get("email") or "")
    
    # --- Customer basics ------------------------------------------------------
    cust = (opportunity.get("customer") or {})
    cust_first = (cust.get("firstName") or opportunity.get("customer_first") or "there")
    
    # --- Trade info (first trade only) ---------------------------------------
    ti = (opportunity.get("tradeIns") or [{}])[0] if (opportunity.get("tradeIns") or []) else {}
    trade_year  = str(ti.get("year") or "")
    trade_make  = str(ti.get("make") or "")
    trade_model = str(ti.get("model") or "")
    
    # --- Merge fields for template -------------------------------------------
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

    # Ensure exactly one booking CTA (replace token or append CTA)
    body_html = replace_or_append_booking_cta(body_html, rooftop_name)

    # Harmonize style with general Patti; de-dupe any existing prefs line
    body_html = normalize_patti_body(body_html)
    body_html = _PREFS_RE.sub("", body_html).strip()
    body_html = body_html + build_patti_footer(rooftop_name)

    # Subject: from cadence plan (no "Re:")
    subject = plan.get("subject") or f"{rooftop_name} — Your Instant Cash Offer"

    # --- Recipient resolution (SAFE_MODE honored) -----------------------------
    email_addr = ""
    emails = cust.get("emails") or []
    if emails:
        prim = next((e for e in emails if e.get("isPrimary") or e.get("isPreferred")), None)
        email_addr = (prim or emails[0]).get("address") or ""
    if not email_addr:
        email_addr = cust.get("emailAddress") or ""
    recipients = [email_addr] if (email_addr and not SAFE_MODE) else [TEST_TO]

    # --- Log + send -----------------------------------------------------------
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

    # ✅ Persist idempotency state AFTER a successful send
    state["last_template_day_sent"] = effective_day
    state["last_template_sent_at"]  = _dt.now(_tz.utc).isoformat()
    _save_state_comment(token, subscription_id, opp_id, state)

    
    # --- Phone/Text tasks (TCPA guard) ---------------------------------------
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

