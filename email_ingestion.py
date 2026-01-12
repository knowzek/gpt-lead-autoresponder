#email_ingestion.py
import os
import re
import logging
from datetime import datetime as _dt, timezone as _tz
import json

from rooftops import get_rooftop_info
from fortellis import (
    get_token,
    add_opportunity_comment,
    get_opportunity,
    search_customers_by_email,
    get_opps_by_customer_id,
)

from kbb_ico import _top_reply_only
from airtable_store import (
    find_by_opp_id,
    find_by_customer_email,
    opp_from_record,
    save_opp,
    upsert_lead,
    _safe_json_dumps,
)


log = logging.getLogger("patti.email_ingestion")

# For now we only want this running on your single test opp
TEST_OPP_ID = "050a81e9-78d4-f011-814f-00505690ec8c"

DEFAULT_SUBSCRIPTION_ID = os.getenv("DEFAULT_SUBSCRIPTION_ID")  # set this to Tustin Kia's subscription id

def _resolve_subscription_id(inbound: dict, headers: dict | None) -> str | None:
    # 1) Prefer body fields (Power Automate is sending these)
    for k in ("subscription_id", "subscriptionId", "subscription", "sub_id"):
        v = inbound.get(k)
        if isinstance(v, str):
            v = v.strip()
        if v:
            return v

    # 2) Then check headers (case-insensitive)
    headers = headers or {}
    if isinstance(headers, dict):
        lower = {str(k).lower(): v for k, v in headers.items()}
        for k in ("x-subscription-id", "subscription_id", "subscriptionid"):
            v = lower.get(k)
            if isinstance(v, str):
                v = v.strip()
            if v:
                return v

    # 3) Optional fallback: infer from the "to" mailbox if you want
    # (super useful for vendor leads like carfax/cars.com)
    to_addr = (inbound.get("to") or "").lower().strip()
    if to_addr:
        # Example mapping (you can wire this to env vars)
        # if "patti@pattersonautos.com" in to_addr: return os.getenv("DEFAULT_SUBSCRIPTION_ID")
        pass

    return None


def _find_best_active_opp_for_email(*, shopper_email: str, token: str, subscription_id: str) -> str | None:
    target = (shopper_email or "").strip().lower()
    if not target:
        return None

    customers = search_customers_by_email(target, token, subscription_id, page_size=10) or []
    if not customers:
        return None

    candidates = []

    for c in customers:
        cid = c.get("id") or c.get("customerId")
        if not cid:
            continue

        opps = get_opps_by_customer_id(cid, token, subscription_id, page_size=100) or []
        for o in opps:
            status = (o.get("status") or "").strip().lower()
            if status != "active":
                continue

            opp_id = o.get("id") or o.get("opportunityId")
            if not opp_id:
                continue

            # pick most recently touched
            dt_str = (
                o.get("updatedAt")
                or o.get("updated_at")
                or o.get("createdAt")
                or o.get("created_at")
                or ""
            )
            candidates.append((str(dt_str), opp_id))

    if not candidates:
        return None

    candidates.sort(reverse=True)
    return candidates[0][1]

EMAIL_RE = re.compile(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", re.I)

def _extract_shopper_email_from_provider(body_text: str) -> str | None:
    body_text = body_text or ""
    m = re.search(r"(?im)^\s*email\s*:\s*([^\s<]+@[^\s<]+)\s*$", body_text)
    if m:
        return m.group(1).strip().lower()

    # fallback: pick first email that isn't the sender/provider
    candidates = [e.lower() for e in EMAIL_RE.findall(body_text)]
    block = {"noreplylead@carfax.com", "salesleads@cars.com", "reply@messages.kbb.com", "patti@pattersonautos.com"}
    for e in candidates:
        if e in block:
            continue
        if "carfax" in e or "cars.com" in e:
            continue
        return e
    return None


def process_lead_notification(inbound: dict) -> None:
    subject = inbound.get("subject") or ""
    body_html = inbound.get("body_html") or ""
    raw_text = inbound.get("body_text") or clean_html(body_html)
    body_text = (raw_text or "").strip()

    ts = inbound.get("timestamp") or _dt.now(_tz.utc).isoformat()
    headers = inbound.get("headers") or {}
    safe_mode = _safe_mode_from(inbound)
    test_recipient = (inbound.get("test_email") or os.getenv("TEST_TO")) if safe_mode else None


    log.info(
        "DEBUG resolve_subscription: inbound.subscription_id=%r inbound.subscriptionId=%r inbound.source=%r inbound.to=%r headers_keys=%s headers=%r",
        inbound.get("subscription_id"),
        inbound.get("subscriptionId"),
        inbound.get("source"),
        inbound.get("to"),
        list(headers.keys()) if isinstance(headers, dict) else None,
        headers,
    )

    subscription_id = _resolve_subscription_id(inbound, headers)
    if not subscription_id:
        log.warning("No subscription_id resolved; cannot process lead notification")
        return

    tok = get_token(subscription_id)

    shopper_email = _extract_shopper_email_from_provider(body_text)
    if not shopper_email:
        log.warning("No shopper email found in provider lead email. subj=%r", subject[:120])
        return

    opp_id = _find_best_active_opp_for_email(
        shopper_email=shopper_email,
        token=tok,
        subscription_id=subscription_id,
    )
    if not opp_id:
        log.warning("No active opp found for shopper=%s subj=%r", shopper_email, subject[:120])
        return

    # Airtable bootstrap
    rec = find_by_opp_id(opp_id)
    if rec:
        opportunity = opp_from_record(rec)
    else:
        opp = get_opportunity(opp_id, tok, subscription_id)
        opp["_subscription_id"] = subscription_id
        now_iso = ts
        opp.setdefault("followUP_date", now_iso)
        upsert_lead(opp_id, {
            "subscription_id": subscription_id,
            "source": opp.get("source") or "",
            "is_active": bool(opp.get("isActive", True)),
            "follow_up_at": opp.get("followUP_date"),
            "mode": "",
            "opp_json": _safe_json_dumps(opp),
        })
        rec2 = find_by_opp_id(opp_id)
        if not rec2:
            log.warning("Bootstrap upsert did not produce record opp=%s", opp_id)
            return
        opportunity = opp_from_record(rec2)

    # Seed message into thread for GPT context (optional, but useful)
    opportunity.setdefault("messages", []).append({
        "msgFrom": "customer",
        "subject": subject,
        "body": body_text[:1500],
        "date": ts,
        "source": "lead_notification",
    })
    opportunity.setdefault("checkedDict", {})["last_msg_by"] = "customer"

    save_opp(opportunity)

    # ✅ Call YOUR existing internet lead first-touch logic (the extracted helper)
    from processNewData import send_first_touch_email
    from rooftops import get_rooftop_info

    fresh_opp = get_opportunity(opp_id, tok, subscription_id) or {}

    # Rooftop / sender
    rt = get_rooftop_info(subscription_id) or {}
    rooftop_name   = rt.get("name") or rt.get("rooftop_name") or "Rooftop"
    rooftop_sender = rt.get("sender") or rt.get("patti_email") or os.getenv("TEST_FROM") or ""

    # Customer name/email (prefer CRM customer block)
    cust = fresh_opp.get("customer") or opportunity.get("customer") or {}
    customer_name = (
        (cust.get("firstName") or "").strip() + " " + (cust.get("lastName") or "").strip()
    ).strip() or "there"

    customer_email = None
    emails = cust.get("emails") or []
    if isinstance(emails, list):
        # preferred + not DNE
        for e in emails:
            if not isinstance(e, dict):
                continue
            if e.get("doNotEmail"):
                continue
            if e.get("isPreferred") and e.get("address"):
                customer_email = e["address"]
                break
        # fallback
        if not customer_email:
            for e in emails:
                if not isinstance(e, dict):
                    continue
                if e.get("doNotEmail"):
                    continue
                if e.get("address"):
                    customer_email = e["address"]
                    break

    # Salesperson (best-effort)
    salesperson = ""
    sp = fresh_opp.get("salesperson") or fresh_opp.get("owner") or {}
    if isinstance(sp, dict):
        salesperson = (sp.get("name") or sp.get("fullName") or "").strip()
    elif isinstance(sp, str):
        salesperson = sp.strip()
    if not salesperson:
        salesperson = "our team"

    # Vehicle string (best-effort)
    vehicle_str = "one of our vehicles"
    sought = fresh_opp.get("soughtVehicles") or opportunity.get("soughtVehicles") or []
    if isinstance(sought, list) and sought:
        primary = None
        for v in sought:
            if isinstance(v, dict) and v.get("isPrimary"):
                primary = v
                break
        if not primary and isinstance(sought[0], dict):
            primary = sought[0]

        if primary:
            make  = str(primary.get("make") or "").strip()
            model = str(primary.get("model") or "").strip()
            year  = str(primary.get("yearFrom") or primary.get("year") or "").strip()
            trim  = str(primary.get("trim") or "").strip()
            tmp = f"{year} {make} {model} {trim}".strip()
            if tmp:
                vehicle_str = tmp

    # Source label for the email copy
    source_label = (inbound.get("source") or "internet lead").strip()

    # Timing
    currDate = _dt.now(_tz.utc)
    currDate_iso = ts  # keep inbound timestamp as the sent timestamp

    # OFFLINE_MODE (however you store it)
    OFFLINE_MODE = bool(os.getenv("OFFLINE_MODE", "").strip().lower() in ["1", "true", "yes"])

    # We are NOT auto-scheduling from provider emails
    created_appt_ok = False
    appt_human = None

    customer_email = shopper_email

    sent_ok = send_first_touch_email(
        opportunity=opportunity,
        fresh_opp=fresh_opp,
        token=tok,
        subscription_id=subscription_id,
        rooftop_name=rooftop_name,
        rooftop_sender=rooftop_sender,
        customer_name=customer_name,
        customer_email=customer_email,
        source=source_label,
        vehicle_str=vehicle_str,
        salesperson=salesperson,
        inquiry_text="",  # provider emails usually don’t contain a real “question”
        created_appt_ok=created_appt_ok,
        appt_human=appt_human,
        currDate=currDate,
        currDate_iso=currDate_iso,
        opportunityId=opp_id,
        OFFLINE_MODE=OFFLINE_MODE,
        SAFE_MODE=safe_mode,                
        test_recipient=test_recipient,  
    )

    log.info("Lead notification first-touch sent_ok=%s opp=%s shopper=%s", sent_ok, opp_id, shopper_email)
    return



def _safe_mode_from(inbound: dict) -> bool:
    # Prefer the PA flag
    if inbound.get("test_mode") is True:
        return True

    # Fall back to Render env vars
    return (os.getenv("PATTI_SAFE_MODE", "0") == "1") or (os.getenv("SAFE_MODE", "0") == "1")


def clean_html(html: str) -> str:
    """Strip HTML tags and reduce to plain text."""
    text = re.sub(r"(?is)<[^>]+>", " ", html or "")
    return re.sub(r"\s+", " ", text).strip()

def _extract_email(addr: str) -> str:
    """
    Given "Kristin <foo@bar.com>" or just "foo@bar.com" return lowercase email.
    """
    if not addr:
        return ""
    m = re.search(r"<([^>]+)>", addr)
    email = m.group(1) if m else addr
    return email.strip().lower()


def _compute_lead_age_days(opportunity: dict) -> int:
    """
    Copy of the lead_age_days logic from processNewData.py
    so kbb_ico sees the same value.
    """
    lead_age_days = 0
    created_raw = (
        opportunity.get("dateIn")
        or opportunity.get("createdDate")
        or opportunity.get("created_at")
        or (opportunity.get("firstActivity") or {}).get("completedDate")
    )
    try:
        if created_raw:
            created_dt = _dt.fromisoformat(str(created_raw).replace("Z", "+00:00"))
            lead_age_days = (_dt.now(_tz.utc) - created_dt).days
    except Exception:
        pass
    return lead_age_days


def _find_opportunity_by_sender(sender_email: str):
    """
    Find opportunity in Airtable by matching the sender email against
    opp_json.customer.emails[].address (or a stored customer_email column if you have one).
    """
    if not sender_email:
        return None, None

    rec = find_by_customer_email(sender_email)  # you’ll add this in airtable_store.py
    if not rec:
        return None, None

    opp = opp_from_record(rec)
    return opp.get("opportunityId") or opp.get("id"), opp



def is_test_opp(opp: dict, opp_id: str | None) -> bool:
    if opp_id and opp_id == TEST_OPP_ID:
        return True
    if opp and opp.get("opportunityId") == TEST_OPP_ID:
        return True
    if opp and opp.get("id") == TEST_OPP_ID:
        return True
    return False

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

def _extract_customer_email_from_lead_body(body_text: str) -> str | None:
    """
    Best-effort: find the first plausible customer email in the lead body.
    Filters out known provider / dealership domains if you add them.
    """
    if not body_text:
        return None

    candidates = EMAIL_RE.findall(body_text) or []
    if not candidates:
        return None

    # filter obvious non-customer addresses
    bad_substrings = [
        "carfax.com",
        "cars.com",
        "autotrader",
        "kbb.com",
        "pattersonautos.com",
    ]
    for e in candidates:
        el = e.lower()
        if any(b in el for b in bad_substrings):
            continue
        return el

    # if all filtered, fall back to first
    return candidates[0].lower()


def process_inbound_email(inbound: dict) -> None:
    """
    Entry point called from web_app.py when Power Automate POSTs a
    "new email" JSON payload.

    Goal:
      - Resolve the opportunity
      - Append this message to opportunity["messages"]
      - Call process_kbb_ico_lead so the existing KBB brain decides
        what (if anything) to send next.
    """
    sender_raw = (inbound.get("from") or "").strip()
    subject = inbound.get("subject") or ""
    
    body_html = inbound.get("body_html") or ""
    raw_text = inbound.get("body_text") or clean_html(body_html)

    # Start with raw text as a fallback
    body_text = raw_text

    # 1️⃣ Try KBB's HTML reply-stripper first (when we actually have HTML)
    if body_html:
        try:
            top_html = _top_reply_only(body_html) or ""
            stripped = clean_html(top_html)
            # Only use it if we got something non-empty back
            if stripped:
                body_text = stripped
        except Exception:
            # If anything weird happens, just stick with raw_text
            pass

    # 2️⃣ Plain-text reply stripping for Outlook-style separators
    body_text = (body_text or "").strip()

    # Cut off everything after common reply delimiters so KBB only sees
    # the *new* line like "What was the kbb estimate?"
    for sep in [
        "\r\n________________________________",
        "\n________________________________",

        # HTML-cleaned versions (no underscores / newlines)
        " From:",
        " Sent:",
        " On ",
        " Subject:",
        " To:",

        # Raw newline forms, in case they survive
        "\r\nFrom:",
        "\nFrom:",
        "\r\nOn ",
        "\nOn ",
    ]:
        idx = body_text.find(sep)
        if idx != -1:
            body_text = body_text[:idx].strip()
            break


    # Optional but useful while testing:
    log.info(
        "Email ingestion text debug: raw=%r final=%r",
        (raw_text or "")[:160],
        (body_text or "")[:160],
    )

    ts = inbound.get("timestamp") or _dt.now(_tz.utc).isoformat()
    headers = inbound.get("headers") or {}

    log.info(
        "DEBUG resolve_subscription from process_inbound_email function - the kbb flow?: inbound.subscription_id=%r inbound.subscriptionId=%r inbound.source=%r inbound.to=%r headers_keys=%s headers=%r",
        inbound.get("subscription_id"),
        inbound.get("subscriptionId"),
        inbound.get("source"),
        inbound.get("to"),
        list(headers.keys()) if isinstance(headers, dict) else None,
        headers,
    )

    # 1) find opp
    subscription_id = _resolve_subscription_id(inbound, headers)
    if not subscription_id:
        log.warning("No subscription_id resolved; cannot lookup opp in Fortellis")
        return
    
    tok = get_token(subscription_id)
    
    # Prefer header opp_id if present (nice when available)
    opp_id = headers.get("X-Opportunity-ID") or headers.get("x-opportunity-id")
    
    # If missing, lookup opp_id in Fortellis by sender email
    if not opp_id:
        sender_email = _extract_email(sender_raw)

        # If sender is a provider/no-reply, pull customer email from the body
        if any(x in sender_email for x in ["carfax.com", "cars.com"]) or "noreply" in sender_email:
            maybe_customer = _extract_customer_email_from_lead_body(raw_text or body_text)
            if maybe_customer:
                sender_email = maybe_customer

        opp_id = _find_best_active_opp_for_email(
            shopper_email=sender_email,
            token=tok,
            subscription_id=subscription_id,
        )
        if not opp_id:
            log.warning("No active opp found in Fortellis for sender=%s (sub=%s)", sender_raw, subscription_id)
            return
    
    # Now try Airtable
    rec = find_by_opp_id(opp_id)
    if rec:
        opportunity = opp_from_record(rec)
    else:
        # Bootstrap from Fortellis by opp_id, then create Airtable lead
        opp = get_opportunity(opp_id, tok, subscription_id)

        opp["_subscription_id"] = subscription_id
    
        now_iso = inbound.get("timestamp") or _dt.now(_tz.utc).isoformat()
        opp.setdefault("followUP_date", now_iso)
    
        upsert_lead(opp_id, {
            "subscription_id": subscription_id,
            "source": opp.get("source") or "",
            "is_active": bool(opp.get("isActive", True)),
            "follow_up_at": opp.get("followUP_date"),
            "mode": "",
            "opp_json": _safe_json_dumps(opp),
        })
    
        rec2 = find_by_opp_id(opp_id)
        if not rec2:
            log.warning("Bootstrap upsert did not produce record opp=%s", opp_id)
            return
    
        opportunity = opp_from_record(rec2)
        
    source = (opportunity.get("source") or "").lower()
    # if opp_json is a dict in your normalized object, include it too:
    try:
        source2 = (opportunity.get("opp_json", {}) or {}).get("source", "")
    except Exception:
        source2 = ""
    source = (source + " " + str(source2)).lower()
    
    is_kbb = ("kbb" in source) or ("kelley blue book" in source) or ("instant cash offer" in source)

    
    # 2) Append inbound message into the thread (in-memory)
    ts = inbound.get("timestamp") or _dt.now(_tz.utc).isoformat()
    msg_dict = {
        "msgFrom": "customer",
        "subject": subject,
        "body": body_text,
        "date": ts,
    }
    opportunity.setdefault("messages", []).append(msg_dict)
    
    # 3) Mark inbound + set KBB convo signals
    now_iso = ts  # use the inbound timestamp we already computed
    opportunity.setdefault("checkedDict", {})["last_msg_by"] = "customer"
    opportunity["followUP_date"] = now_iso  # due now
    
    if is_kbb:
        st = opportunity.setdefault("_kbb_state", {})
        st["mode"] = "convo"
        st["last_customer_msg_at"] = now_iso
    else:
        st = opportunity.setdefault("_internet_state", {})
        st["last_customer_msg_at"] = now_iso
        st["mode"] = "convo"


    # 4) Persist to Airtable (save_opp already updates follow_up_at + opp_json)
    save_opp(opportunity)

    # 5) IMMEDIATE reply (do NOT wait for cron)
    try:
        from kbb_ico import process_kbb_ico_lead
        from rooftops import get_rooftop_info

        subscription_id = opportunity.get("_subscription_id")
        if not subscription_id:
            log.warning("Inbound email matched opp=%s but missing _subscription_id; cannot reply", opp_id)
            return

        tok = get_token(subscription_id)

        rt = get_rooftop_info(subscription_id) or {}
        rooftop_name   = rt.get("name") or rt.get("rooftop_name") or "Rooftop"
        rooftop_sender = rt.get("sender") or rt.get("patti_email") or None

        # Let the brain answer the customer's question immediately
        safe_mode = _safe_mode_from(inbound)

        if is_kbb:
            state, action_taken = process_kbb_ico_lead(
                opportunity=opportunity,
                lead_age_days=0,
                rooftop_name=rooftop_name,
                inquiry_text=body_text,
                token=tok,
                subscription_id=subscription_id,
                SAFE_MODE=safe_mode,
                rooftop_sender=rooftop_sender,
                trigger="email_webhook",
                inbound_ts=ts,
                inbound_subject=subject,
            )
            if isinstance(state, dict):
                opportunity["_kbb_state"] = state
        else:
            from processNewData import send_thread_reply_now
    
            # Always fetch a fresh opp so subStatus / scheduled appt state is current
            fresh_opp = get_opportunity(opp_id, tok, subscription_id)
    
            safe_mode = _safe_mode_from(inbound)
            test_recipient = inbound.get("test_email") or os.getenv("INTERNET_TEST_EMAIL")
    
            state, action_taken = send_thread_reply_now(
                opportunity=opportunity,
                fresh_opp=fresh_opp,
                token=tok,
                subscription_id=subscription_id,
                SAFE_MODE=safe_mode,
                test_recipient=test_recipient,
                inbound_ts=ts,
                inbound_subject=subject,
            )
    
            if isinstance(state, dict):
                opportunity["_internet_state"] = state

        log.info("Inbound email processed immediately opp=%s action_taken=%s", opp_id, action_taken)

    except Exception as e:
        log.exception("Immediate inbound reply failed opp=%s err=%s", opp_id, e)

    # 6) Optional: log inbound email to CRM as a COMPLETED ACTIVITY (not a Note)
    subscription_id = opportunity.get("_subscription_id")
    if subscription_id:
        try:
            token = get_token(subscription_id)
            preview = (body_text or "")[:500]

            # Prefer the same "numeric type" pattern as KBB to avoid mapping confusion
            complete_activity(
                token,
                subscription_id,
                opp_id,
                activity_name="Inbound Email",
                activity_type=20,  # Inbound Email (matches your ACTIVITY_TYPE_MAP)
                comments=f"From: {sender_raw}\nSubject: {subject}\n\n{preview}",
            )
        except Exception as e:
            log.warning("Failed to log inbound email activity opp=%s err=%s", opp_id, e)


    log.info("Inbound email queued + processed immediately for opp=%s", opp_id)
    return
