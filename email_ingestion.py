#email_ingestion.py
import os
import re
import logging
from datetime import datetime as _dt, timezone as _tz
import json
from airtable_store import mark_customer_reply, mark_unsubscribed
from kbb_ico import _is_optout_text as _kbb_is_optout_text, _is_decline as _kbb_is_decline

from rooftops import get_rooftop_info
from fortellis import (
    get_token,
    add_opportunity_comment,
    get_opportunity,
    search_customers_by_email,
    find_recent_opportunity_by_email,
    get_opps_by_customer_id,
)
from processNewData import send_first_touch_email
from fortellis import complete_activity
from fortellis import complete_read_email_activity
from patti_triage import classify_inbound_email, handoff_to_human, should_triage
from patti_common import EMAIL_RE, PHONE_RE

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

# --- Provider template comment extraction (for CARFAX / Cars.com style lead emails) ---

_PROVIDER_TEMPLATE_HINT_RE = re.compile(
    r"(?is)\bNEW\s+CUSTOMER\s+LEAD\s+FOR\b|\bLead\s*ID\s*:\b|\bYear/Make/Model\s*:\b|\bVIN\s*:\b|\bStock\s*:\b|\bPrice\s*:\b"
)

_CUSTOMER_COMMENT_RE = re.compile(
    r"(?is)\b(?:Additional\s+comments|Customer\s+comments?|Comments?|Message|Questions?)\s*:\s*(.+?)(?:\n{2,}|\Z)"
)

def _extract_customer_comment_from_provider(body_text: str) -> str:
    t = (body_text or "").strip()
    if not t:
        return ""

    m = _CUSTOMER_COMMENT_RE.search(t)
    if not m:
        return ""

    comment = (m.group(1) or "").strip()

    # Stop if the template starts repeating the contact block
    comment = re.split(
        r"(?im)^\s*(?:Here's how to contact this customer|First Name|Last Name|Email|Phone|Date Submitted)\s*:",
        comment
    )[0].strip()

    return comment


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

def extract_phone_from_text(body_text: str) -> str | None:
    t = body_text or ""
    # Apollo: Telephone<TAB>2069993915
    m = re.search(r"(?im)^\s*(telephone|phone)\s*(?:[:\t ]+)\s*([0-9\-\(\)\.\s\+]{7,})\s*$", t)
    if m:
        return re.sub(r"\s+", " ", m.group(2)).strip()
    return None



def _find_best_active_opp_for_email(*, shopper_email: str, token: str, subscription_id: str) -> str | None:
    target = (shopper_email or "").strip().lower()
    if not target:
        return None

    candidates: list[tuple[str, str]] = []

    # ----------------------------
    # Path A: customerId -> opps
    # ----------------------------
    try:
        customers = search_customers_by_email(target, token, subscription_id, page_size=10) or []
        for c in customers:
            cid = c.get("id") or c.get("customerId")
            if not cid:
                continue

            try:
                opps = get_opps_by_customer_id(cid, token, subscription_id, page_size=100) or []
            except Exception as e:
                # Key fix: don't crash reply handling because one endpoint isn't supported.
                log.warning("get_opps_by_customer_id failed (will fallback): sub=%s cid=%s err=%r", subscription_id, cid, e)
                opps = []

            for o in opps:
                status = (o.get("status") or "").strip().lower()
                if status != "active":
                    continue

                opp_id = o.get("id") or o.get("opportunityId")
                if not opp_id:
                    continue

                dt_str = (
                    o.get("updatedAt")
                    or o.get("updated_at")
                    or o.get("createdAt")
                    or o.get("created_at")
                    or ""
                )
                candidates.append((str(dt_str), opp_id))

        if candidates:
            candidates.sort(reverse=True)
            return candidates[0][1]

    except Exception as e:
        # If customer search itself blows up, still try Path B.
        log.warning("customerId-based opp match failed (will fallback): sub=%s email=%s err=%r", subscription_id, target, e)

    # ----------------------------
    # Path B: searchDelta fallback
    # ----------------------------
    # Uses your existing get_recent_opportunities() which already handles 404 "empty window".
    try:
        best: tuple[str, str] | None = None  # (dt_str, opp_id)

        page = 1
        max_pages = 20
        since_minutes = 60 * 24 * 14  # 14 days
        page_size = 100

        while page <= max_pages:
            data = get_recent_opportunities(token, subscription_id, since_minutes=since_minutes, page=page, page_size=page_size) or {}
            items = data.get("items") or []
            if not items:
                break

            for op in items:
                status = (op.get("status") or "").strip().lower()
                if status and status != "active":
                    continue

                # Match email either in customer.emails[] or customerEmail
                match = False
                cust = op.get("customer") or {}
                for e in (cust.get("emails") or []):
                    addr = (e.get("address") or "").strip().lower()
                    if addr == target:
                        match = True
                        break
                if not match:
                    addr2 = (op.get("customerEmail") or "").strip().lower()
                    if addr2 == target:
                        match = True

                if not match:
                    continue

                opp_id = op.get("opportunityId") or op.get("id")
                if not opp_id:
                    continue

                dt_str = (
                    op.get("updatedAt")
                    or op.get("updated_at")
                    or op.get("createdAt")
                    or op.get("created_at")
                    or op.get("dateIn")
                    or ""
                )

                cand = (str(dt_str), str(opp_id))
                if (best is None) or (cand[0] > best[0]):
                    best = cand

            page += 1

        return best[1] if best else None

    except Exception as e:
        log.warning("searchDelta fallback opp match failed: sub=%s email=%s err=%r", subscription_id, target, e)
        return None

def _extract_shopper_email_from_provider(body_text: str) -> str | None:
    body_text = body_text or ""

    # Apollo (tab), or colon, or spaces
    m = re.search(r"(?im)^\s*email\s*(?:[:\t ]+)\s*([^\s<]+@[^\s<]+)\s*$", body_text)
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

PHONE_RE = re.compile(r"(?i)\b(\+?1[\s\-\.]?)?\(?\d{3}\)?[\s\-\.]?\d{3}[\s\-\.]?\d{4}\b")

def _extract_carscom_name_email_phone(body_text: str) -> tuple[str, str, str]:
    """
    Cars.com format (as plain text) tends to look like:
      Travis Marshall
      jdmarshall.cras@gmail.com
      661-433-7553
    """
    t = (body_text or "").strip()
    if not t:
        return "", "", ""

    lines = [ln.strip() for ln in t.splitlines() if ln.strip()]

    # best-effort email
    email = ""
    for ln in lines[:25]:
        m = EMAIL_RE.search(ln)
        if m and "cars.com" not in m.group(1).lower():
            email = m.group(1).lower()
            break

    # best-effort phone
    phone = ""
    for ln in lines[:40]:
        m = PHONE_RE.search(ln)
        if m:
            phone = m.group(0).strip()
            break

    # best-effort name: look for a 2–3 word line near the top (before email)
    name_line = ""
    email_idx = None
    if email:
        for i, ln in enumerate(lines[:25]):
            if email in ln.lower():
                email_idx = i
                break

    scan = lines[: (email_idx if email_idx is not None else 12)]
    for ln in scan:
        if EMAIL_RE.search(ln) or PHONE_RE.search(ln):
            continue
        if re.search(r"(?i)\byou have a new lead\b|\bcars\.com\b|view shopper details", ln):
            continue
        # simple: "First Last" (allow 2-3 tokens)
        toks = ln.split()
        if 2 <= len(toks) <= 3 and all(tok[:1].isalpha() for tok in toks):
            name_line = ln
            break

    first, last = "", ""
    if name_line:
        parts = name_line.split()
        first = parts[0]
        last = " ".join(parts[1:])

    return _clean_first_name(first), (last or "").strip().title(), phone


def _clean_first_name(name: str) -> str:
    n = (name or "").strip()
    if not n:
        return ""
    n = n.split()[0]  # first token only
    if n.isupper():
        n = n.title()
    return n

def _extract_first_last_from_provider(body_text: str) -> tuple[str, str]:
    """
    Handles:
      First Name: Michael
      First Name\tMichael
      First Name Michael
      ...and inline formats like:
      First Name Michael Last Name Smith Email ...
    """
    t = body_text or ""
    first = ""
    last = ""

    # 1) Line-based patterns (your current behavior)
    m1 = re.search(r"(?im)^\s*First\s*Name\s*(?:[:\t ]+)\s*(.+?)\s*$", t)
    m2 = re.search(r"(?im)^\s*Last\s*Name\s*(?:[:\t ]+)\s*(.+?)\s*$", t)
    if m1:
        first = m1.group(1).strip()
    if m2:
        last = m2.group(1).strip()

    # 2) Inline fallback (Apollo / compact provider formats)
    # Only run if missing either field.
    if not first or not last:
        # Capture First Name up to the next field label or end-of-line
        mi_first = re.search(
            r"(?is)\bFirst\s*Name\b\s*[:\t ]+\s*([A-Za-z][A-Za-z'’-]*(?:\s+[A-Za-z][A-Za-z'’-]*)?)"
            r"(?=\s+\bLast\s*Name\b|\s+\bEmail\b|\s+\bTelephone\b|\s+\bPhone\b|\s+\bStreet\b|\s+\bCity\b|\s+\bZip\b|$)",
            t,
        )
        mi_last = re.search(
            r"(?is)\bLast\s*Name\b\s*[:\t ]+\s*([A-Za-z][A-Za-z'’-]*(?:\s+[A-Za-z][A-Za-z'’-]*)?)"
            r"(?=\s+\bEmail\b|\s+\bTelephone\b|\s+\bPhone\b|\s+\bStreet\b|\s+\bCity\b|\s+\bZip\b|$)",
            t,
        )

        if mi_first and not first:
            first = mi_first.group(1).strip()
        if mi_last and not last:
            last = mi_last.group(1).strip()

    # 3) CARFAX pattern fallback (keep this)
    if not first:
        m = re.search(
            r"(?i)\bNEW CUSTOMER LEAD FOR .*?\b([A-Z][a-zA-Z'’-]+)\s+([A-Z][a-zA-Z'’-]+)\b\s+is interested\b",
            t,
        )
        if m:
            first = first or m.group(1).strip()
            last = last or m.group(2).strip()

    first = _clean_first_name(first)
    last = (last or "").strip()
    if last.isupper():
        last = last.title()
    return first, last




_KBB_SOURCES = {"kbb instant cash offer", "kbb servicedrive", "kbb service drive"}

def _is_kbb_opp(opp: dict) -> bool:
    src = (opp or {}).get("source")
    return (src or "").strip().lower() in _KBB_SOURCES


def process_lead_notification(inbound: dict) -> None:
    subject = inbound.get("subject") or ""
    body_html = inbound.get("body_html") or ""
    raw_text = inbound.get("body_text") or clean_html(body_html)
    body_text = (raw_text or "").strip()
    first_name, last_name = _extract_first_last_from_provider(body_text)

    log.info(
        "lead_notification parsed body_text len=%d sender=%r subj=%r head=%r",
        len(body_text or ""),
        inbound.get("from"),
        subject[:120],
        (body_text or "")[:260],
    )


    phone = ""  # single source of truth
    
    sender = (inbound.get("from") or "").lower()
    is_cars = ("cars.com" in sender) or ("salesleads@cars.com" in sender) or ("you have a new lead from cars.com" in body_text.lower())
    log.info("lead_notification is_cars=%s sender=%r", is_cars, sender)
    
    if is_cars:
        log.info(
            "cars.com before extract len=%d head=%r",
            len(body_text or ""),
            (body_text or "")[:300],
        )
    
        cf, cl, ph = _extract_carscom_name_email_phone(body_text)
    
        log.info(
            "cars.com extracted first=%r last=%r phone=%r (pre-merge first=%r last=%r)",
            cf, cl, ph, first_name, last_name
        )

        if cf: first_name = cf
        if cl: last_name = cl
        if ph: phone = ph
    
    # If Cars.com didn't yield a phone, try Apollo-style Telephone line
    if not phone:
        phone = extract_phone_from_text(body_text) or ""


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

    # ✅ Guard KBB opps from General Leads
    opp = get_opportunity(opp_id, tok, subscription_id)
    
    if _is_kbb_opp(opp):
        log.info(
            "Skipping General Leads lead_notification bootstrap for KBB opp=%s source=%r",
            opp_id,
            opp.get("source"),
        )
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

        log.info(
            "bootstrap upsert opp=%s email=%r first=%r last=%r phone=%r source=%r",
            opp_id, shopper_email, first_name, last_name, phone, (opp.get("source") or "")
        )

        upsert_lead(opp_id, {
            "subscription_id": subscription_id,
            "source": opp.get("source") or "",
            "is_active": bool(opp.get("isActive", True)),
            "follow_up_at": opp.get("followUP_date"),
            "mode": "",
            "opp_json": _safe_json_dumps(opp),
            "customer_email": shopper_email,
            "Customer First Name": first_name,
            "Customer Last Name": last_name,
            "Phone": phone,
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

    # Persist guest email once, forever
    opportunity["customer_email"] = shopper_email
    
    extra = {
        "customer_email": shopper_email,
        "Customer First Name": first_name,
        "Customer Last Name": last_name,
        "Phone": phone,
    }
    
    save_opp(opportunity, extra_fields=extra)

    # ✅ Call existing internet lead first-touch logic (the extracted helper)

    fresh_opp = get_opportunity(opp_id, tok, subscription_id) or {}

    # Rooftop / sender
    rt = get_rooftop_info(subscription_id) or {}
    rooftop_name   = rt.get("name") or rt.get("rooftop_name") or "Rooftop"
    rooftop_sender = rt.get("sender") or rt.get("patti_email") or os.getenv("TEST_FROM") or ""

    # Customer name (prefer Airtable-hydrated first/last, then Fortellis, else "there")
    cust = fresh_opp.get("customer") or opportunity.get("customer") or {}
    afn = (opportunity.get("customer_first_name") or (cust.get("firstName") or "")).strip()
    aln = (opportunity.get("customer_last_name") or (cust.get("lastName") or "")).strip()
    customer_name = afn or "there"
    
    # ✅ For provider lead notifications, always send to the provider-extracted shopper email
    customer_email = shopper_email

    # -----------------------------
    # TRIAGE: provider lead message
    # -----------------------------
    triage_intended_handoff = False
    try:
        if should_triage(is_kbb=False):
            log.info("lead_notification triage running opp=%s shopper=%s", opp_id, shopper_email)
    
            triage_text = body_text or ""
            triage = None  # ✅ ensure defined
    
            # Provider template? Only triage guest-written comment
            if _PROVIDER_TEMPLATE_HINT_RE.search(triage_text):
                comment = _extract_customer_comment_from_provider(triage_text)
                if comment:
                    triage_text = comment
                else:
                    triage = {
                        "classification": "AUTO_REPLY_SAFE",
                        "reason": "Provider lead template with no customer-written comments"
                    }
                    triage_text = ""  # ✅ no GPT call
    
            if triage is None:
                if triage_text.strip():
                    triage = classify_inbound_email(triage_text)
                else:
                    triage = {
                        "classification": "AUTO_REPLY_SAFE",
                        "reason": "Empty triage text"
                    }
    
            classification = (triage.get("classification") or "").strip().upper()
            reason = (triage.get("reason") or "").strip()
    
            log.info(
                "lead_notification triage classification=%s reason=%r opp=%s shopper=%s",
                classification, reason[:220], opp_id, shopper_email
            )
    
            if classification == "HUMAN_REVIEW_REQUIRED":
                triage_intended_handoff = True
                opportunity["needs_human_review"] = True
                opportunity.setdefault("patti", {})["human_review_reason"] = (reason or "Human review required").strip()
                save_opp(opportunity)
    
                handoff_to_human(
                    opportunity=opportunity,
                    fresh_opp=fresh_opp,
                    token=tok,
                    subscription_id=subscription_id,
                    rooftop_name=rooftop_name,
                    inbound_subject=subject,
                    inbound_text=body_text or "",
                    inbound_ts=ts,
                    triage=triage,
                )
                return
    
            if classification == "NON_LEAD":
                log.info("lead_notification triage NON_LEAD opp=%s - ignoring", opp_id)
                return
    
            if classification == "EXPLICIT_OPTOUT":
                log.info("lead_notification triage EXPLICIT_OPTOUT opp=%s - stopping", opp_id)
                return
    
    except Exception as e:
        log.exception("lead_notification triage failed opp=%s err=%s", opp_id, e)

        # ✅ FAIL CLOSED if this lead required human review
        if triage_intended_handoff:
            log.warning(
                "Blocking first-touch because triage intended handoff but failed opp=%s shopper=%s",
                opp_id, shopper_email
            )
            # make sure the lock is persisted (best-effort)
            try:
                opportunity["needs_human_review"] = True
                opportunity.setdefault("patti", {})["human_review_reason"] = (
                    opportunity.get("patti", {}).get("human_review_reason")
                    or "Human review required (triage handoff failed)"
                )
                save_opp(opportunity)
            except Exception:
                pass
            return
        # otherwise: continue to normal first-touch



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

    patti = opportunity.get("patti") or {}
    checked = opportunity.get("checkedDict") or {}
    
    if opportunity.get("needs_human_review") is True or patti.get("human_review_reason"):
        log.warning("Blocking first-touch: human review lock opp=%s", opp_id)
        return

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
    # Turn <br> and </p> into newlines first
    h = html or ""
    h = re.sub(r"(?i)<br\s*/?>", "\n", h)
    h = re.sub(r"(?i)</p\s*>", "\n", h)

    # Strip remaining tags
    text = re.sub(r"(?is)<[^>]+>", " ", h)

    # Normalize whitespace but KEEP newlines
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()


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

    # 🚫 Skip internal Patterson emails (Patti gets CC'd on vendor/internal threads)
    sender_email = _extract_email(sender_raw).strip().lower()
    if sender_email.endswith("@pattersonautos.com"):
        log.info(
            "Skipping inbound email opp-match (internal sender): sender=%r subject=%r to=%r",
            sender_email,
            (subject or "")[:120],
            inbound.get("to"),
        )
        return

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

        # 🚫 Guard: do not allow KBB opportunities into General Leads Airtable
        if _is_kbb_opp(opp):
            log.info("Skipping General Leads bootstrap for KBB opp=%s source=%r", opp_id, opp.get("source"))
            return
            
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
        
    block_auto_reply = bool(opportunity.get("needs_human_review") is True)
        
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
    
    # ✅ PATCH 2A: Mark engagement (customer replied)
    mark_customer_reply(opportunity, when_iso=ts)
    
    # ✅ PATCH 2B: If opt-out detected, mark unsubscribed and stop
    if _kbb_is_optout_text(body_text) or _kbb_is_decline(body_text):
        mark_unsubscribed(opportunity, when_iso=ts, reason=body_text[:300])
        opportunity.setdefault("checkedDict", {})["last_msg_by"] = "customer"
        opportunity["followUP_date"] = ts
        save_opp(opportunity)  # ✅ ensures latest thread + flags are persisted
        log.info("Inbound opt-out detected; unsubscribed opp=%s", opp_id)
        return

    if block_auto_reply:
        log.info("Blocking inbound auto-reply (but reply logged): Needs Human Review opp=%s", opp_id)
        return

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

    # 4.5) TRIAGE (classify BEFORE any immediate reply)
    try:
        if should_triage(is_kbb):
            triage = classify_inbound_email(body_text)
            cls = (triage.get("classification") or "").strip().upper()
            log.info(
                "triage classification=%s reason=%r opp=%s",
                cls,
                (triage.get("reason") or "")[:220],
                opp_id,
            )

            if cls == "HUMAN_REVIEW_REQUIRED":
                fresh_opp_for_triage = None
                try:
                    fresh_opp_for_triage = get_opportunity(opp_id, tok, subscription_id)
                except Exception:
                    fresh_opp_for_triage = None
    
                rt = get_rooftop_info(subscription_id) or {}
                rooftop_name_triage = rt.get("name") or rt.get("rooftop_name") or "Rooftop"
    
                handoff_to_human(
                    opportunity=opportunity,
                    fresh_opp=fresh_opp_for_triage,
                    token=tok,
                    subscription_id=subscription_id,
                    rooftop_name=rooftop_name_triage,
                    inbound_subject=subject,
                    inbound_text=body_text,
                    inbound_ts=ts,
                    triage=triage,
                )
    
                # extra safety persist
                try:
                    save_opp(opportunity)
                except Exception:
                    pass
    
                log.info(
                    "Triage routed to human opp=%s reason=%s",
                    opp_id,
                    triage.get("reason"),
                )
                return  # 🚫 STOP: do not auto-reply
    
            if cls == "NON_LEAD":
                log.info("Triage NON_LEAD opp=%s - ignoring", opp_id)
                return
    
            if cls == "EXPLICIT_OPTOUT":
                if not is_kbb:
                    log.info(
                        "Triage EXPLICIT_OPTOUT non-KBB opp=%s - stopping auto reply",
                        opp_id,
                    )
                    return
                log.info(
                    "Triage EXPLICIT_OPTOUT KBB opp=%s - letting KBB brain handle",
                    opp_id,
                )
    
    except Exception as e:
        log.exception(
            "Triage failure opp=%s err=%s - defaulting to normal flow",
            opp_id,
            e,
        )


    # 5) IMMEDIATE reply (do NOT wait for cron)
    try:
        from kbb_ico import process_kbb_ico_lead
    
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

    # 6) Optional: log inbound email to CRM as a COMPLETED "Read Email" activity (type 20)
    subscription_id = opportunity.get("_subscription_id")
    if subscription_id:
        try:
            from datetime import datetime, timezone
    
            token = get_token(subscription_id)
            preview = (body_text or "")[:500]
    
            # Convert inbound ts (your ts is usually like 2026-01-15T18:59:19+00:00)
            # into Zulu "YYYY-MM-DDTHH:MM:SSZ" for Fortellis.
            def _to_z(iso_str: str | None) -> str:
                if iso_str:
                    try:
                        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
                        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    except Exception:
                        pass
                return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
            completed_z = _to_z(ts)
    
            complete_read_email_activity(
                token=token,
                subscription_id=subscription_id,
                opportunity_id=opp_id,
                completed_dt_iso_utc=completed_z,
                comments=f"From: {sender_raw}\nSubject: {subject}\n\n{preview}",
            )
    
        except Exception as e:
            log.warning("Failed to log inbound email activity opp=%s err=%s", opp_id, e)


    log.info("Inbound email queued + processed immediately for opp=%s", opp_id)
    return
