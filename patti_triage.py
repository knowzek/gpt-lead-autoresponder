# patti_triage.py
import os
import re
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from openai import OpenAI

from airtable_store import patch_by_id, save_opp
from rooftops import get_rooftop_info
from outlook_email import send_email_via_outlook

from fortellis import schedule_activity, add_opportunity_comment

from patti_common import EMAIL_RE, PHONE_RE
from patti_common import extract_customer_comment_from_provider

log = logging.getLogger("patti.triage")

# -----------------------
# ENV CONFIG
# -----------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
TRIAGE_MIN_CONF = float(os.getenv("HUMAN_REVIEW_MIN_CONF", "0.75"))
TRIAGE_DUE_HOURS = int(os.getenv("HUMAN_REVIEW_DUE_HOURS", "2"))

# CC leadership
HUMAN_REVIEW_CC = os.getenv(
    "HUMAN_REVIEW_CC",
    "kristin@blackoctopusai.com"
).strip()

# Optional: also triage KBB inbound replies (default off)
TRIAGE_KBB = os.getenv("TRIAGE_KBB", "0").strip().lower() in ("1", "true", "yes")

# Airtable field names (must match your base)
AT_NEEDS = os.getenv("AT_NEEDS_HUMAN_REVIEW", "Needs Human Review")
AT_REASON = os.getenv("AT_HUMAN_REVIEW_REASON", "Human Review Reason")
AT_AT = os.getenv("AT_HUMAN_REVIEW_AT", "Human Review At")
AT_NOTIFIED = os.getenv("AT_HUMAN_REVIEW_NOTIFIED", "Human Review Notified")
AT_NOTIFIED_AT = os.getenv("AT_HUMAN_REVIEW_NOTIFIED_AT", "Human Review Notified At")

_oai = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# Fortellis salesTeam[].id -> email
SALESTEAM_ID_TO_EMAIL = {
    # --- Tustin Kia / Internet leads ---
    "486120e6-c7b5-f011-814f-00505690ec8c": "roozbehb@pattersonautos.com",   # Roozbeh Behrangi
    "250ddd78-cab5-f011-814f-00505690ec8c": "ashleym@pattersonautos.com",  # Ashley Madrigal
    "5cf595e6-c8b5-f011-814f-00505690ec8c": "Aydenc@pattersonautos.com",  # Ayden Chanthavong
    "54316e4e-cab5-f011-814f-00505690ec8c": "Gustavol@pattersonautos.com",  # Gustavo Lopez
    "85fa8c85-cdb5-f011-814f-00505690ec8c": "TommyV@pattersonautos.com",  # Tommy Vilayphonh (S + DM)
    "448c97cd-cab5-f011-814f-00505690ec8c": "Gabrielm@pattersonautos.com",  # Gabriel Martinez
    "a7201e67-cdb5-f011-814f-00505690ec8c": "Joannet@pattersonautos.com",  # Joanne Tran
    "033b8d23-cbb5-f011-814f-00505690ec8c": "Damianp@pattersonautos.com",  # Damian Perez
    "2b0e5005-c7b5-f011-814f-00505690ec8c": "dannyam@pattersonautos.com",  # Danny Amezcua
    "7cfd1fa3-cab5-f011-814f-00505690ec8c": "johnnym@pattersonautos.com", # Johnny Madrigal
    "268a74af-ccb5-f011-814f-00505690ec8c": "donalds@pattersonautos.com",  # Donald Smalley (Desk Manager)
    "0084c9ba-94a8-f011-814f-00505690ec8c": "ala@pattersonautos.com",  # Al Alcontin (BDC)
    "ea44fdc0-c1c7-f011-814f-00505690ec8c": "Juliem@pattersonautos.com",  # Julie Manallo (BDC)
    "598a45a3-efca-f011-814f-00505690ec8c": "Jhoannec@pattersonautos.com",  # Jhoanne Canceran (BDC)

    "8f693b1a-7966-ea11-a977-005056b72b57": "joshuaw@pattersonautos.com",  # Joshua Wheelan
}

def notify_staff_patti_scheduled_appt(
    *,
    opportunity: dict,
    fresh_opp: dict | None,
    subscription_id: str,
    rooftop_name: str,
    appt_human: str,
    customer_reply: str,
    subject: str,
) -> None:
    """
    Email salesperson + CC managers when Patti schedules an appointment in the CRM.
    Reuses the same recipient-resolution rules as human handoff.
    No Airtable updates.
    """
    opp_id = opportunity.get("opportunityId") or opportunity.get("id") or ""

    resolved_sales_email = resolve_primary_sales_email(fresh_opp or {}) or ""
    to_addr = resolved_sales_email or os.getenv("HUMAN_REVIEW_FALLBACK_TO", "") or "knowzek@gmail.com"

    # CC list: follow the same process as human handoff
    raw_cc = (os.getenv("HUMAN_REVIEW_CC") or "").strip()
    cc_addrs = []
    if raw_cc:
        parts = raw_cc.replace(",", ";").split(";")
        cc_addrs = [p.strip() for p in parts if p.strip()]

    # dedupe + don't duplicate To
    to_lower = (to_addr or "").lower()
    seen = set()
    cc_clean = []
    for e in cc_addrs:
        el = e.lower()
        if not el or el == to_lower or el in seen:
            continue
        seen.add(el)
        cc_clean.append(e)
    cc_addrs = cc_clean

    # -----------------------
    # SAFE MODE recipient gate (hard override)
    # -----------------------
    safe_mode = (os.getenv("SAFE_MODE", "0").strip() == "1")

    if safe_mode:
        test_to = (os.getenv("TEST_TO") or "").strip()
        if not test_to:
            raise RuntimeError("SAFE_MODE is enabled but TEST_TO is not set")

        original_to = to_addr
        original_cc = list(cc_addrs or [])

        # hard override
        to_addr = test_to
        cc_addrs = []

        # make it obvious
        subj = f"[SAFE MODE] {subject}"

        # optional: show original recipients in body for debugging
        html = (
            f"<div style='padding:10px;border:2px solid #cc0000;margin-bottom:12px;'>"
            f"<b>SAFE MODE:</b> This appointment notify was rerouted to <b>{test_to}</b>.<br/>"
            f"<b>Original To:</b> {original_to}<br/>"
            f"<b>Original CC:</b> {', '.join(original_cc) if original_cc else '(none)'}"
            f"</div>"
            + html
        )

        log.warning(
            "SAFE_MODE enabled: rerouting APPT notify opp=%s original_to=%r original_cc=%r -> test_to=%r",
            opp_id, original_to, original_cc, test_to
        )


    # Customer info (prefer Airtable-saved fields)
    first = (opportunity.get("customer_first_name") or "").strip()
    last  = (opportunity.get("customer_last_name") or "").strip()
    customer_name = (f"{first} {last}").strip() or "Customer"
    customer_email = (opportunity.get("customer_email") or "").strip() or "unknown"
    customer_phone = (opportunity.get("customer_phone") or "").strip() or "unknown"
    vehicle = _vehicle_str(opportunity, fresh_opp or {}) or ""

    subj = f"[Patti] Appointment scheduled — {rooftop_name} — {customer_name} — {appt_human}"
    if vehicle:
        subj += f" — {vehicle}"

    html = f"""
    <p><strong>Patti scheduled a sales appointment</strong></p>
    <p><strong>Store:</strong> {rooftop_name}<br/>
       <strong>When:</strong> {appt_human}<br/>
       <strong>Customer:</strong> {customer_name}<br/>
       <strong>Email:</strong> {customer_email}<br/>
       <strong>Phone:</strong> {customer_phone}<br/>
       <strong>Opportunity ID:</strong> {opp_id}<br/>
       <strong>Vehicle:</strong> {vehicle or "—"}</p>

    <p><strong>Customer reply:</strong><br/>
    <em>{(customer_reply or "").strip()[:800]}</em></p>

    <p>You can take it from here to confirm details and prep the vehicle.</p>
    """

    send_email_via_outlook(
        to_addr=to_addr,
        subject=_clip(subj, 180),
        html_body=html,
        opp_id=opp_id,
        cc_addrs=cc_addrs,
        timeout=20,
        enforce_compliance=False,
    )

    log.info("APPT NOTIFY EMAIL: to=%s cc=%s opp=%s when=%s", to_addr, cc_addrs, opp_id, appt_human)


# -----------------------
# Helpers
# -----------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _clip(s: str, n: int) -> str:
    return (s or "").strip()[:n]

def _parse_cc_list(csv: str) -> list[str]:
    return [x.strip() for x in (csv or "").split(",") if x.strip()]

def _first_preferred_email(cust: dict) -> str:
    emails = cust.get("emails") or []
    if isinstance(emails, list):
        for e in emails:
            if not isinstance(e, dict):
                continue
            if e.get("doNotEmail"):
                continue
            if e.get("isPreferred") and e.get("address"):
                return (e["address"] or "").strip()
        for e in emails:
            if not isinstance(e, dict):
                continue
            if e.get("doNotEmail"):
                continue
            if e.get("address"):
                return (e["address"] or "").strip()
    return ""

def _first_phone(cust: dict) -> str:
    phones = cust.get("phones") or []
    if isinstance(phones, list):
        for p in phones:
            if not isinstance(p, dict):
                continue
            if p.get("isPreferred") and p.get("number"):
                return (p["number"] or "").strip()
        for p in phones:
            if isinstance(p, dict) and p.get("number"):
                return (p["number"] or "").strip()
    return ""

def _vehicle_str(opportunity: dict, fresh_opp: Optional[dict]) -> str:
    """
    Build vehicle display string from Airtable-hydrated fields (canonical source).
    Falls back to empty string if no vehicle data is available.
    """
    year  = (opportunity.get("year") or "").strip()
    make  = (opportunity.get("make") or "").strip()
    model = (opportunity.get("model") or "").strip()
    trim  = (opportunity.get("trim") or "").strip()
    return f"{year} {make} {model} {trim}".strip()

def _primary_salesperson(sales_team: Any) -> Tuple[str, str]:
    """
    Returns (name, email). Empty strings if missing.
    """
    if not isinstance(sales_team, list):
        return ("", "")
    primary = None
    for m in sales_team:
        if not isinstance(m, dict):
            continue
        if str(m.get("isPrimary")).lower() in ("true", "1", "yes"):
            primary = m
            break
    if not primary:
        primary = next((m for m in sales_team if isinstance(m, dict)), None)
    if not primary:
        return ("", "")
    fn = (primary.get("firstName") or "").strip()
    ln = (primary.get("lastName") or "").strip()
    nm = (f"{fn} {ln}").strip() or (primary.get("name") or "").strip()
    em = (primary.get("email") or "").strip()
    return (nm, em)

def resolve_salesperson_contact(opportunity: dict, fresh_opp: Optional[dict]) -> Dict[str, str]:
    st = (fresh_opp or {}).get("salesTeam")
    if st is None:
        st = opportunity.get("salesTeam")
    name, email = _primary_salesperson(st)
    return {"name": name or "Sales Team", "email": email or ""}

def resolve_primary_sales_email(
    fresh_opp: dict | None = None,
    opportunity: dict | None = None,
) -> str | None:

    # ✅ Prefer Fortellis shape
    st = (fresh_opp or {}).get("salesTeam")

    # ✅ Fallback to Airtable-hydrated opp
    if not st:
        st = (opportunity or {}).get("salesTeam")

    if not isinstance(st, list) or not st:
        return None

    # Prefer primary
    primary = None
    for p in st:
        if isinstance(p, dict) and str(p.get("isPrimary")).lower() in ("true", "1", "yes"):
            primary = p
            break

    if not primary:
        primary = st[0] if isinstance(st[0], dict) else None

    if not primary:
        return None

    sid = (primary.get("id") or "").strip()
    if not sid:
        return None

    return SALESTEAM_ID_TO_EMAIL.get(sid)


# -----------------------
# Fast triage rules
# -----------------------
_OPT_OUT_RE = re.compile(
    r"(?i)\b("
    r"stop|unsubscribe|remove\s+me|do\s+not\s+contact|do\s+not\s+email|don't\s+email|"
    r"no\s+further\s+contact|stop\s+contacting|stop\s+emailing|opt\s*out|opt-?out|"
    r"cease\s+and\s+desist"
    r")\b"
)

# anything “sensitive / negotiation / needs human”
_HUMAN_REVIEW_RE = re.compile(
    r"(?i)\b("
    r"otd|out[-\s]*the[-\s]*door|best\s+price|lowest\s+price|discount|msrp|invoice|quote|"
    r"deal\s*(?:sheet|breakdown|terms|numbers)|"

    r"payment|monthly|lease|apr|interest|finance|financing|credit|down\s+payment|"

    r"trade-?in\s+(?:value|worth|offer|apprais|estimate)|"
    r"value\s+my\s+trade|"
    r"kbb\s+value|"
    r"trade\s+(?:value|offer|worth)|"
    r"appraisal|carmax|carvana|"

    r"offer\s*(?:amount|price|value)\s*[:=]?\s*(?:\$\s*)?\d{2,}|"

    r"complaint|bbb|dmv|attorney|legal|lawsuit|"
    r"angry|upset|frustrated|scam|fraud|ripoff|"
    r"asap|urgent|today|immediately|call\s+me\s+now"
    r")\b"
)



_NON_LEAD_RE = re.compile(r"(?i)\b(auto-?reply|out\s+of\s+office|delivery\s+has\s+failed|undeliverable)\b")

# Inventory-specific config questions (model + qualifier) => human review
_INVENTORY_QUAL_RE = re.compile(
    r"(?i)\b("
    r"panoramic|sunroof|moonroof|interior|leather|heated|ventilated|"
    r"awd|fwd|4wd|4x4|"
    r"package|tech|technology|premium|"
    r"color|grey|gray|black|white|tan|beige|red|blue|"
    r"captain|bench|tow|navigation|nav"
    r")\b"
)

# "Model" hints (expand anytime)
_MODEL_HINT_RE = re.compile(
    r"(?i)\b("
    # Kia
    r"sportage|telluride|sorento|seltos|niro|carnival|ev6|ev9|k5|k4|forte|rio|"
    # Mazda
    r"mazda3|mazda\s*3|cx-?30|cx-?5|cx-?50|cx-?90|"
    # Hyundai (add/trim as needed)
    r"elantra|sonata|tucson|santa\s*fe|palisade|ioniq|kona|venue"
    r")\b"
)

def gpt_reply_gate(email_text: str) -> Dict[str, Any]:
    """
    Decide if Patti can safely auto-reply WITHOUT guessing or needing verification.
    Returns:
      {
        "can_auto_reply": bool,
        "confidence": float,
        "reason": str
      }
    """
    t = (email_text or "").strip()
    t_short = _clip(t, 2500)

    if not _oai:
        # If OpenAI not configured, default safe (human)
        return {"can_auto_reply": False, "confidence": 0.60, "reason": "OpenAI not configured; default to human review."}

    prompt = f"""
You are the escalation gate for a dealership assistant named Patti.

Your only job: decide if Patti can reply directly **truthfully and completely** using ONLY what she has,
or if this should be handed off to a human.

Patti capabilities / constraints:
- Inventory access: NO (cannot confirm availability, exact options, trims, or holds)
- Vehicle history access: NO (cannot confirm accident history, title status, service history, number of owners, etc.)
- Pricing authority: LIMITED (cannot negotiate, quote OTD, confirm discounts, or fees)
- Scheduling: YES (can propose times and set appointments)
- Location/hours: YES
- Can ask a clarifying question: YES

Rules:
- HANDOFF if the customer asks for anything that requires verification or dealership-only data
  (e.g. accident history, condition assessment, title, service records, exact availability, "is it still available", "clean title",
   "how many owners", "any damage", "what's the best price", OTD, discounts, finance approvals, payoff, etc.)
- HANDOFF if the message is multi-part, ambiguous, upset/angry, or requires a nuanced human response.
- AUTO_REPLY is allowed only if Patti can answer with high confidence from general info (hours/location/next steps)
  or by asking 1 clarifying question safely.

Return JSON ONLY:
{{
  "can_auto_reply": true/false,
  "confidence": 0.0,
  "reason": "short"
}}

Email:
\"\"\"{t_short}\"\"\"
""".strip()

    try:
        resp = _oai.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "Return JSON only. No extra text."},
                {"role": "user", "content": prompt},
            ],
        )
        raw = resp.choices[0].message.content or "{}"
        data = json.loads(raw)

        can_auto = bool(data.get("can_auto_reply"))
        conf = float(data.get("confidence") or 0.0)
        reason = (data.get("reason") or "").strip()

        # safety valve: if it says "auto" but low confidence, force handoff
        if can_auto and conf < TRIAGE_MIN_CONF:
            return {
                "can_auto_reply": False,
                "confidence": conf,
                "reason": f"Confidence below threshold ({TRIAGE_MIN_CONF}). {reason}".strip()
            }

        return {"can_auto_reply": can_auto, "confidence": conf, "reason": reason or "Gated by model."}

    except Exception as e:
        log.exception("Reply gate failed: %s", e)
        return {"can_auto_reply": False, "confidence": 0.55, "reason": "Reply gate error; default to human review."}


def classify_inbound_email(email_text: str, *, provider_template: bool = False) -> Dict[str, Any]:

    """
    Returns:
      {
        "classification": "AUTO_REPLY_SAFE|HUMAN_REVIEW_REQUIRED|EXPLICIT_OPTOUT|NON_LEAD",
        "confidence": float,
        "reason": str
      }
    """
    t = (email_text or "").strip()
    t_short = _clip(t, 2500)

    # cheap + reliable rules first
    if not t_short or _NON_LEAD_RE.search(t_short):
        return {"classification": "NON_LEAD", "confidence": 0.95, "reason": "Looks like system/auto message or empty."}

    if _OPT_OUT_RE.search(t_short):
        return {"classification": "EXPLICIT_OPTOUT", "confidence": 0.98, "reason": "Contains opt-out language."}

    t_lower = t_short.lower()

    # ✅ SAFE AUTO-REPLY OVERRIDE:
    # Common lead intents like scheduling/availability should NOT require human review.
    # We can auto-reply using hedged language ("I can confirm/check availability").
    SAFE_INTENT_RE = re.compile(
        r"\b("
        r"available|availability|still available|still there|in stock|"
        r"test drive|drive it|"
        r"appointment|schedule|set up a time|book|"
        r"what times|what time|times are available|"
        r"does that work|that works|works for me|yes that works|"
        r"tomorrow|today|tonight|this week|this weekend|"
        r"monday|tuesday|wednesday|thursday|friday|saturday|sunday|"
        r"\b\d{1,2}(:\d{2})?\s?(am|pm)\b"
        r")\b",
        re.IGNORECASE
    )

    # If it's basically a normal lead reply/intent, treat as safe to auto-reply.
    # We'll enforce safe wording in the response generator (hedged language).
    if SAFE_INTENT_RE.search(t_short):
        return {
            "classification": "AUTO_REPLY_SAFE",
            "confidence": 0.88,
            "reason": "Common lead intent (availability/scheduling). Auto-reply is safe using hedged language (offer to check/confirm availability and propose times)."
        }


    if provider_template:
        # Extract only the customer-written part from the provider template
        comment = extract_customer_comment_from_provider(t_short)  # you already have this helper
        comment = (comment or "").strip()
    
        # If we truly have no customer comment, it's just a normal first-touch lead
        if not comment:
            return {
                "classification": "AUTO_REPLY_SAFE",
                "confidence": 0.85,
                "reason": "Provider template with no customer comment; safe for normal first-touch."
            }
    
        # If customer comment contains opt-out, honor it
        if _OPT_OUT_RE.search(comment):
            return {"classification": "EXPLICIT_OPTOUT", "confidence": 0.98, "reason": "Opt-out in customer comment."}
    
        # Run the GPT reply gate ONLY on the extracted customer comment
        gate = gpt_reply_gate(comment)
    
        if not gate.get("can_auto_reply", False):
            return {
                "classification": "HUMAN_REVIEW_REQUIRED",
                "confidence": float(gate.get("confidence") or 0.0),
                "reason": gate.get("reason") or "Reply gate: requires human review (provider comment)."
            }
    
        return {
            "classification": "AUTO_REPLY_SAFE",
            "confidence": float(gate.get("confidence") or 0.0),
            "reason": gate.get("reason") or "Reply gate: safe to auto-reply (provider comment)."
        }

    # Generic trade interest (no numbers/value/offer/payment/finance terms) should NOT force human review
    if re.search(r"\btrade\b|\btrade[-\s]?in\b", t_lower):
        has_trade_value_terms = re.search(
            r"\$|\b\d{3,}\b|\boffer\b|\bvalue\b|\bworth\b|\bapprais|\bquote\b|\botd\b|\bout the door\b|"
            r"\bpayoff\b|\bowe\b|\bnegative equity\b|\bpayment\b|\bfinance\b|\bapr\b|\brate\b",
            t_lower
        )
        if not has_trade_value_terms:
            # Let GPT decide (or mark safe directly)
            # return {"classification": "AUTO_REPLY_SAFE", "confidence": 0.85, "reason": "Generic trade-in interest (no value/pricing terms)."}
            pass  # <-- best: allow GPT to classify instead of auto-escalating

    # 3) NEW: GPT Reply Gate (THIS REPLACES regex escalation + old GPT classifier)
    gate = gpt_reply_gate(t_short)

    log.info("TRIAGE_GATE can_auto=%s conf=%.2f reason=%s",
         gate.get("can_auto_reply"), float(gate.get("confidence") or 0), gate.get("reason"))

    if not gate.get("can_auto_reply", False):
        return {
            "classification": "HUMAN_REVIEW_REQUIRED",
            "confidence": float(gate.get("confidence") or 0.0),
            "reason": gate.get("reason") or "Reply gate: requires human review."
        }

    return {
        "classification": "AUTO_REPLY_SAFE",
        "confidence": float(gate.get("confidence") or 0.0),
        "reason": gate.get("reason") or "Reply gate: safe to auto-reply."
    }


def handoff_to_human(
    *,
    opportunity: dict,
    fresh_opp: Optional[dict],
    token: str,
    subscription_id: str,
    rooftop_name: str,
    inbound_subject: str,
    inbound_text: str,
    inbound_ts: Optional[str],
    triage: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Executes handoff:
      - Airtable flags + timestamps
      - Fortellis scheduled activity + comment
      - Outlook email to salesperson + CC managers
    """
    opp_id = opportunity.get("opportunityId") or opportunity.get("id")
    if not opp_id:
        raise ValueError("Missing opportunityId for human handoff.")

    log.warning(
        "HR_WRITE handoff_to_human opp=%s rec_id=%s reason=%r",
        opportunity.get("id") or opportunity.get("opportunityId"),
        opportunity.get("_airtable_rec_id"),
        (triage.get("reason") if isinstance(triage, dict) else None) or opportunity.get("human_review_reason"),
    )

    now_iso = inbound_ts or _now_iso()

    rec_id = opportunity.get("_airtable_rec_id")
    # idempotency: if already notified, skip
    patti = opportunity.get("patti") or {}
    if isinstance(patti, dict) and patti.get("human_review_notified_at"):
        # ✅ Even if already notified, ensure cron won't retry
        try:
            opportunity["needs_human_review"] = True
            opportunity["followUP_date"] = None
            patti["mode"] = "handoff"
            if not OFFLINE_MODE and rec_id:
                patch_by_id(rec_id, {"follow_up_at": None})
            save_opp(opportunity)
        except Exception:
            pass
        return {"ok": True, "skipped": True, "opp_id": opp_id, "reason": "Already notified."}


    # resolve people + context
    rt = get_rooftop_info(subscription_id) or {}
    rooftop_sender = rt.get("sender") or ""
    rooftop_addr = rt.get("address") or ""

    sp = resolve_salesperson_contact(opportunity, fresh_opp)
    salesperson_name = sp.get("name") or "Sales Team"
    
    # ✅ Use your Fortellis-ID→email mapping (the same one you use to route the email)
    resolved_sales_email = (
        resolve_primary_sales_email(fresh_opp=fresh_opp, opportunity=opportunity)
        or ""
    )
    salesperson_email = resolved_sales_email  # for the body


    # ✅ Source of truth is what ingestion saved into the opp blob (Airtable)
    first = (opportunity.get("customer_first_name") or "").strip()
    last  = (opportunity.get("customer_last_name") or "").strip()
    customer_name = (f"{first} {last}").strip() or "Customer"
    
    customer_email = (opportunity.get("customer_email") or "").strip() or "unknown"
    customer_phone = (opportunity.get("customer_phone") or "").strip() or "unknown"

    vehicle = _vehicle_str(opportunity, fresh_opp) or ""

    reason = (triage.get("reason") or "").strip()
    conf = triage.get("confidence")

    # -----------------------
    # Airtable patch
    # -----------------------
    
    if rec_id:
        try:
            patch_by_id(rec_id, {
                AT_NEEDS: True,
                AT_REASON: f"{reason} (conf={conf})".strip(),
                AT_AT: now_iso,
                AT_NOTIFIED: True,
                AT_NOTIFIED_AT: now_iso,
                "follow_up_at": None,
            })
        except Exception as e:
            log.exception("Airtable patch failed rec=%s: %s", rec_id, e)

    # persist in opp blob (idempotency + durable stop)
    try:
        p = opportunity.setdefault("patti", {})
        if not isinstance(p, dict):
            opportunity["patti"] = {}
            p = opportunity["patti"]

        # ✅ STOP customer automation immediately (durable fields)
        opportunity["needs_human_review"] = True
        opportunity["human_review_reason"] = reason
        opportunity["human_review_at"] = now_iso
        opportunity["followUP_date"] = None
        opportunity["followUP_count"] = 0

        # ✅ Durable state into snapshot (these SHOULD be in _build_patti_snapshot)
        p["mode"] = "handoff"
        p["handoff"] = {"reason": reason, "at": now_iso}

        # Idempotency markers
        p["human_review_requested_at"] = now_iso
        p["human_review_notified_at"] = now_iso
        p["human_review_reason"] = reason

        save_opp(opportunity)

    except Exception as e:
        log.warning("save_opp failed (ignored): %s", e)

    # -----------------------
    # Fortellis: schedule + comment
    # -----------------------
    due_utc = (datetime.now(timezone.utc) + timedelta(hours=TRIAGE_DUE_HOURS)).strftime("%Y-%m-%dT%H:%M:%SZ")

    activity_comments = (
        "Patti flagged this lead for HUMAN REVIEW.\n"
        f"Reason: {reason} (conf={conf})\n"
        f"Customer: {customer_name} | {customer_email} | {customer_phone}\n"
        f"Vehicle: {vehicle or 'N/A'}\n"
        f"Latest msg: {_clip(inbound_text, 900)}\n"
    )

    try:
        schedule_activity(
            token,
            subscription_id,
            opp_id,
            due_dt_iso_utc=due_utc,
            activity_name="Patti: Human Review Needed",
            activity_type="Send Email",  
            comments=activity_comments,
        )

    except Exception as e:
        log.warning("schedule_activity failed (ignored): %s", e)

    try:
        add_opportunity_comment(
            token,
            subscription_id,
            opp_id,
            _clip(
                "Patti HUMAN REVIEW handoff executed.\n"
                f"Salesperson: {salesperson_name} ({salesperson_email or 'unknown'})\n"
                f"Reason: {reason} (conf={conf})\n"
                f"Inbound subject: {inbound_subject}\n"
                f"Latest msg: {_clip(inbound_text, 600)}",
                1800,
            )
        )
    except Exception as e:
        log.warning("add_opportunity_comment failed (ignored): %s", e)

    # -----------------------
    # Outlook email notify
    # -----------------------
    to_addr = resolved_sales_email or os.getenv("HUMAN_REVIEW_FALLBACK_TO", "") or "knowzek@gmail.com"
    
    subj = f"[Patti] Human review needed - {rooftop_name} - {customer_name}"
    if vehicle:
        subj += f" — {vehicle}"
    
    html = f"""
    <p><b>Patti flagged a lead for human attention.</b></p>
    
    <p><b>Rooftop:</b> {rooftop_name}<br>
    <b>Opp ID:</b> {opp_id}<br>
    <b>Salesperson:</b> {salesperson_name} ({salesperson_email or "unknown"})</p>
    
    <p><b>Customer:</b> {customer_name}<br>
    <b>Email:</b> {customer_email or "unknown"}<br>
    <b>Phone:</b> {customer_phone or "unknown"}<br>
    <b>Vehicle:</b> {vehicle or "unknown"}</p>
    
    <p><b>Classification:</b> HUMAN_REVIEW_REQUIRED<br>
    <b>Reason:</b> {reason or "N/A"}<br>
    <b>Confidence:</b> {conf}</p>
    
    <p><b>Inbound subject:</b> {inbound_subject or ""}</p>
    
    <p><b>Latest customer message:</b><br>
    <pre style="white-space:pre-wrap;font-family:Arial,Helvetica,sans-serif;">{_clip(inbound_text, 2000)}</pre></p>
    
    <p><b>Next step:</b> Please take over this lead in the CRM and reply to the customer directly.</p>
    
    <p style="color:#666;font-size:12px;">
    Human Review Needed logged by Patti in eLead. Sent from: patti@pattersonautos.com<br>
    {rooftop_addr}
    </p>
    """.strip()
    
    # Build CC list from env
    raw_cc = (os.getenv("HUMAN_REVIEW_CC") or "").strip()
    cc_addrs = []
    if raw_cc:
        parts = raw_cc.replace(",", ";").split(";")
        cc_addrs = [p.strip() for p in parts if p.strip()]
    
    # Remove duplicates + avoid duplicating To
    to_lower = (to_addr or "").lower()
    seen = set()
    cc_clean = []
    for e in cc_addrs:
        el = e.lower()
        if not el or el == to_lower:
            continue
        if el in seen:
            continue
        seen.add(el)
        cc_clean.append(e)
    cc_addrs = cc_clean

    # -----------------------
    # SAFE MODE recipient gate (hard override)
    # -----------------------
    safe_mode = (
        (os.getenv("PATTI_SAFE_MODE", "0").strip() == "1")
        or (os.getenv("SAFE_MODE", "0").strip() == "1")
        # optional: if you ever persist a flag into opp blob
        or (opportunity.get("test_mode") is True)
        or (isinstance(opportunity.get("patti"), dict) and opportunity["patti"].get("test_mode") is True)
    )

    if safe_mode:
        test_to = (
            (os.getenv("TEST_TO") or "").strip()
            or (os.getenv("INTERNET_TEST_EMAIL") or "").strip()
            or (os.getenv("HUMAN_REVIEW_FALLBACK_TO") or "").strip()
        )
        if not test_to:
            raise RuntimeError("SAFE_MODE is enabled but TEST_TO (or INTERNET_TEST_EMAIL) is not set")

        original_to = to_addr
        original_cc = list(cc_addrs or [])

        # hard override: nothing goes to real humans in safe mode
        to_addr = test_to
        cc_addrs = []

        # make it obvious in inbox/logs
        subj = f"[SAFE MODE] {subj}"

        # optional: embed who it *would* have gone to (helps debugging)
        html = (
            f"<div style='padding:10px;border:2px solid #cc0000;margin-bottom:12px;'>"
            f"<b>SAFE MODE:</b> This escalation was rerouted to <b>{test_to}</b>.<br/>"
            f"<b>Original To:</b> {original_to}<br/>"
            f"<b>Original CC:</b> {', '.join(original_cc) if original_cc else '(none)'}"
            f"</div>"
            + html
        )

        log.warning(
            "SAFE_MODE enabled: rerouting HUMAN_REVIEW email opp=%s original_to=%r original_cc=%r -> test_to=%r",
            opp_id, original_to, original_cc, test_to
        )

    
    send_email_via_outlook(
        to_addr=to_addr,
        subject=_clip(subj, 180),
        html_body=html,
        opp_id=opp_id,
        cc_addrs=cc_addrs,
        timeout=20,
        enforce_compliance=False,
    )
    
    log.info("TRIAGE EMAIL: to=%s cc=%s", to_addr, cc_addrs)
    
    return {"ok": True, "skipped": False, "opp_id": opp_id, "to": to_addr, "cc": cc_addrs, "due_utc": due_utc}



def should_triage(is_kbb: bool) -> bool:
    """
    We always triage non-KBB. KBB triage is optional via TRIAGE_KBB env var.
    """
    if is_kbb:
        return TRIAGE_KBB
    return True
