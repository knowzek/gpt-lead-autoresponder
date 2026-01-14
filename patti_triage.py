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

    "268a74af-ccb5-f011-814f-00505690ec8c": "donalds@pattersonautos.com",  # Donald Smalley (Desk Manager)
    "0084c9ba-94a8-f011-814f-00505690ec8c": "ala@pattersonautos.com",  # Al Alcontin (BDC)
    "ea44fdc0-c1c7-f011-814f-00505690ec8c": "Juliem@pattersonautos.com",  # Julie Manallo (BDC)
    "598a45a3-efca-f011-814f-00505690ec8c": "Jhoannec@pattersonautos.com",  # Jhoanne Canceran (BDC)

    "8f693b1a-7966-ea11-a977-005056b72b57": "joshuaw@pattersonautos.com",  # Joshua Wheelan
}


# -----------------------
# Helpers
# -----------------------
EMAIL_RE = re.compile(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", re.I)

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
    sought = (fresh_opp or {}).get("soughtVehicles") or opportunity.get("soughtVehicles") or []
    if isinstance(sought, list) and sought:
        primary = None
        for v in sought:
            if isinstance(v, dict) and v.get("isPrimary"):
                primary = v
                break
        if not primary and isinstance(sought[0], dict):
            primary = sought[0]
        if primary:
            year = str(primary.get("yearFrom") or primary.get("year") or "").strip()
            make = str(primary.get("make") or "").strip()
            model = str(primary.get("model") or "").strip()
            trim = str(primary.get("trim") or "").strip()
            return f"{year} {make} {model} {trim}".strip()
    return ""

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

def resolve_primary_sales_email(fresh_opp: dict) -> str | None:
    st = (fresh_opp or {}).get("salesTeam") or []
    if not isinstance(st, list) or not st:
        return None

    # Prefer primary salesperson/BDC
    primary = None
    for p in st:
        if isinstance(p, dict) and p.get("isPrimary"):
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
    r"otd|out\s*the\s*door|best\s+price|lowest\s+price|discount|msrp|invoice|quote|offer|deal|"
    r"payment|monthly|lease|apr|interest|finance|financing|credit|down\s+payment|"
    r"trade|trade-?in|value\s+my\s+trade|appraisal|kbb\s+value|carmax|carvana|"
    r"complaint|bbb|dmv|attorney|legal|lawsuit|"
    r"angry|upset|frustrated|scam|fraud|ripoff|"
    r"asap|urgent|today|immediately|call\s+me\s+now"
    r")\b"
)

_NON_LEAD_RE = re.compile(r"(?i)\b(auto-?reply|out\s+of\s+office|delivery\s+has\s+failed|undeliverable)\b")


def classify_inbound_email(email_text: str) -> Dict[str, Any]:
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

    if _HUMAN_REVIEW_RE.search(t_short):
        return {"classification": "HUMAN_REVIEW_REQUIRED", "confidence": 0.90, "reason": "Pricing/financing/trade/legal/urgent/angry indicators."}

    # GPT classifier (conservative gate)
    if not _oai:
        return {"classification": "HUMAN_REVIEW_REQUIRED", "confidence": 0.60, "reason": "OpenAI not configured; default to human."}

    prompt = f"""
You are classifying an inbound dealership internet lead email.

Do NOT write a reply. Only classify.

Choose ONE:
AUTO_REPLY_SAFE
HUMAN_REVIEW_REQUIRED
EXPLICIT_OPTOUT
NON_LEAD

Rules:
- pricing/discounts/OTD/quotes → HUMAN_REVIEW_REQUIRED
- financing/credit/payments → HUMAN_REVIEW_REQUIRED
- trade-in/value disputes → HUMAN_REVIEW_REQUIRED
- angry/urgent/emotional → HUMAN_REVIEW_REQUIRED
- multiple questions or unclear intent → HUMAN_REVIEW_REQUIRED
- stop contact → EXPLICIT_OPTOUT
- auto-reply/system noise → NON_LEAD
- simple availability/basic info/confirmation → AUTO_REPLY_SAFE

Return JSON ONLY:
{{
  "classification": "AUTO_REPLY_SAFE|HUMAN_REVIEW_REQUIRED|EXPLICIT_OPTOUT|NON_LEAD",
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

        cls = (data.get("classification") or "").strip().upper()
        conf = float(data.get("confidence") or 0.0)
        reason = (data.get("reason") or "").strip()

        if cls not in {"AUTO_REPLY_SAFE", "HUMAN_REVIEW_REQUIRED", "EXPLICIT_OPTOUT", "NON_LEAD"}:
            cls = "HUMAN_REVIEW_REQUIRED"
            reason = reason or "Invalid classifier output; defaulted to human review."

        # confidence safety valve
        if cls == "AUTO_REPLY_SAFE" and conf < TRIAGE_MIN_CONF:
            return {
                "classification": "HUMAN_REVIEW_REQUIRED",
                "confidence": conf,
                "reason": f"Confidence below threshold ({TRIAGE_MIN_CONF}). {reason}".strip()
            }

        return {"classification": cls, "confidence": conf, "reason": reason or "Classified by model."}

    except Exception as e:
        log.exception("Classifier failed: %s", e)
        return {"classification": "HUMAN_REVIEW_REQUIRED", "confidence": 0.55, "reason": "Classifier error; default to human review."}


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

    now_iso = inbound_ts or _now_iso()

    # idempotency: if already notified, skip
    patti = opportunity.get("patti") or {}
    if isinstance(patti, dict) and patti.get("human_review_notified_at"):
        return {"ok": True, "skipped": True, "opp_id": opp_id, "reason": "Already notified."}

    # resolve people + context
    rt = get_rooftop_info(subscription_id) or {}
    rooftop_sender = rt.get("sender") or ""
    rooftop_addr = rt.get("address") or ""

    sp = resolve_salesperson_contact(opportunity, fresh_opp)
    salesperson_name = sp.get("name") or "Sales Team"
    salesperson_email = sp.get("email") or ""

    cust = (fresh_opp or {}).get("customer") or opportunity.get("customer") or {}
    customer_name = (f"{(cust.get('firstName') or '').strip()} {(cust.get('lastName') or '').strip()}").strip() or "Customer"
    customer_email = _first_preferred_email(cust) or ""
    customer_phone = _first_phone(cust) or ""
    vehicle = _vehicle_str(opportunity, fresh_opp) or ""

    reason = (triage.get("reason") or "").strip()
    conf = triage.get("confidence")

    # -----------------------
    # Airtable patch
    # -----------------------
    rec_id = opportunity.get("_airtable_rec_id")
    if rec_id:
        try:
            patch_by_id(rec_id, {
                AT_NEEDS: True,
                AT_REASON: f"{reason} (conf={conf})".strip(),
                AT_AT: now_iso,
                AT_NOTIFIED: True,
                AT_NOTIFIED_AT: now_iso,
            })
        except Exception as e:
            log.exception("Airtable patch failed rec=%s: %s", rec_id, e)

    # persist in opp blob (idempotency)
    try:
        p = opportunity.setdefault("patti", {})
        if not isinstance(p, dict):
            opportunity["patti"] = {}
            p = opportunity["patti"]
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
    # to_addr = resolve_primary_sales_email(fresh_opp) or os.getenv("HUMAN_REVIEW_FALLBACK_TO", "")

    to_addr = "knowzek@gmail.com"
    cc_list = _parse_cc_list(HUMAN_REVIEW_CC)
    headers = {"cc": ",".join(cc_list)} if cc_list else {}

    subj = f"[Patti] Human review needed — {rooftop_name} — {customer_name}"
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
    Logged by Patti (Fortellis task + comment). Sent from: {rooftop_sender or "Patti inbox"}<br>
    {rooftop_addr}
    </p>
    """.strip()

    send_email_via_outlook(
        to_addr=to_addr,
        subject=_clip(subj, 180),
        html_body=html,
        cc_addrs=cc_addrs,    
        headers=headers,
        timeout=20,
    )

    return {"ok": True, "skipped": False, "opp_id": opp_id, "to": to_addr, "cc": cc_list, "due_utc": due_utc}


def should_triage(is_kbb: bool) -> bool:
    """
    We always triage non-KBB. KBB triage is optional via TRIAGE_KBB env var.
    """
    if is_kbb:
        return TRIAGE_KBB
    return True
