# patti_triage.py
"""
Patti Triage (General Internet Leads)

Purpose:
- Classify inbound customer emails BEFORE auto-reply.
- If HUMAN_REVIEW_REQUIRED:
    1) Flag Airtable fields (Needs Human Review, reason, timestamps, notified)
    2) Create a scheduled activity on the opportunity for the salesperson
    3) Email the salesperson + CC leadership from the Patti Outlook inbox
    4) Log an internal CRM comment (no customer email from CRM)

Usage (typical):
    from patti_triage import classify_inbound_email, handle_human_review_handoff

    triage = classify_inbound_email(email_text=latest_plain)
    if triage["classification"] == "HUMAN_REVIEW_REQUIRED":
        handle_human_review_handoff(
            opportunity=opportunity,
            fresh_opp=fresh_opp,
            token=tok,
            subscription_id=subscription_id,
            rooftop_name=rooftop_name,
            inbound_subject=inbound_subject,
            inbound_text=latest_plain,
            inbound_ts=inbound_ts,
        )
        return  # do NOT auto-reply
"""

from __future__ import annotations

import os
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from openai import OpenAI

from airtable_store import patch_by_id, save_opp  # existing helpers
from outlook_email import send_email_via_outlook
from rooftops import get_rooftop_info

# Fortellis actions we rely on:
# - schedule_activity: creates a scheduled activity on the opp
# - add_opportunity_comment: logs our action without sending customer email
from fortellis import schedule_activity, add_opportunity_comment

log = logging.getLogger("patti.triage")

# -----------------------
# Config
# -----------------------

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
OPENAI_TEMPERATURE = float(os.getenv("OPENAI_TEMPERATURE", "0"))

# Who gets CC'd on human review alerts
HUMAN_REVIEW_CC = os.getenv(
    "HUMAN_REVIEW_CC",
    "alexc@pattersonautos.com,austiny@pattersonautos.com,donalds@pattersonautos.com"
).strip()

# Minimum confidence to allow AUTO_REPLY_SAFE. Below this, treat as HUMAN_REVIEW_REQUIRED.
HUMAN_REVIEW_MIN_CONF = float(os.getenv("HUMAN_REVIEW_MIN_CONF", "0.75"))

# Due time for the scheduled "Human Review Needed" task
HUMAN_REVIEW_DUE_HOURS = int(os.getenv("HUMAN_REVIEW_DUE_HOURS", "2"))

# Airtable field names (must match your table)
AT_NEEDS_HUMAN_REVIEW = os.getenv("AT_NEEDS_HUMAN_REVIEW", "Needs Human Review")
AT_HUMAN_REVIEW_REASON = os.getenv("AT_HUMAN_REVIEW_REASON", "Human Review Reason")
AT_HUMAN_REVIEW_AT = os.getenv("AT_HUMAN_REVIEW_AT", "Human Review At")
AT_HUMAN_REVIEW_NOTIFIED = os.getenv("AT_HUMAN_REVIEW_NOTIFIED", "Human Review Notified")
AT_HUMAN_REVIEW_NOTIFIED_AT = os.getenv("AT_HUMAN_REVIEW_NOTIFIED_AT", "Human Review Notified At")

_oai = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


# -----------------------
# Fast local detectors
# -----------------------

_OPT_OUT_RE = re.compile(
    r"(?i)\b("
    r"stop|stop\s+all|stop\s+now|unsubscribe|remove\s+me|do\s+not\s+contact|do\s+not\s+email|don't\s+email|"
    r"no\s+further\s+contact|stop\s+contacting|stop\s+emailing|opt\s*out|opt-?out|"
    r"cease\s+and\s+desist"
    r")\b"
)

# Common triggers for "human review" in sales leads
_HUMAN_TRIGGERS_RE = re.compile(
    r"(?i)\b("
    r"out\s*the\s*door|o\.t\.d\.|otd|best\s+price|lowest\s+price|price\s+match|match\s+this|beat\s+this|"
    r"discount|msrp|invoice|quote|offer|deal|"
    r"payment|monthly|lease|apr|interest|finance|financing|credit|down\s+payment|"
    r"trade|trade-?in|value\s+my\s+trade|appraisal|kbb\s+value|carmax|carvana|"
    r"lawsuit|attorney|legal|complaint|bbb|dmv|"
    r"angry|upset|frustrated|scam|fraud|ripoff|"
    r"call\s+me\s+now|asap|urgent|today|immediately"
    r")\b"
)

def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _safe_strip(s: str, limit: int = 1800) -> str:
    s = (s or "").strip()
    return s[:limit]

def _parse_cc_list(cc_csv: str) -> list[str]:
    out = []
    for part in (cc_csv or "").split(","):
        p = part.strip()
        if p:
            out.append(p)
    return out

def _looks_like_non_lead(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return True
    # system noise patterns
    if "delivery has failed" in t or "undeliverable" in t:
        return True
    if "auto-reply" in t or "out of office" in t:
        return True
    if "do not reply" in t and "noreply" in t:
        return True
    return False


# -----------------------
# Classification
# -----------------------

def classify_inbound_email(*, email_text: str) -> Dict[str, Any]:
    """
    Returns dict:
      {
        "classification": "AUTO_REPLY_SAFE|HUMAN_REVIEW_REQUIRED|EXPLICIT_OPTOUT|NON_LEAD",
        "confidence": float,
        "reason": str
      }
    """
    latest = _safe_strip(email_text, limit=2500)

    # 1) Quick local rules first (cheap + reliable)
    if _looks_like_non_lead(latest):
        return {"classification": "NON_LEAD", "confidence": 0.95, "reason": "Looks like system noise/auto message or empty."}

    if _OPT_OUT_RE.search(latest):
        return {"classification": "EXPLICIT_OPTOUT", "confidence": 0.98, "reason": "Contains opt-out language."}

    if _HUMAN_TRIGGERS_RE.search(latest):
        return {"classification": "HUMAN_REVIEW_REQUIRED", "confidence": 0.90, "reason": "Contains pricing/financing/trade/legal/urgent/angry trigger terms."}

    # 2) GPT classifier (conservative)
    if not _oai:
        # No OpenAI configured: default safe behavior
        return {"classification": "HUMAN_REVIEW_REQUIRED", "confidence": 0.60, "reason": "OpenAI not configured; defaulting to human review for safety."}

    prompt = f"""
You are classifying an inbound dealership internet lead email.

Your job is NOT to write a reply.
Your job is ONLY to classify whether this email can be safely auto-replied to,
or must be handed to a human sales associate.

Classify the email into ONE of these categories:

AUTO_REPLY_SAFE
HUMAN_REVIEW_REQUIRED
EXPLICIT_OPTOUT
NON_LEAD

Rules:
- If the message mentions pricing, discounts, financing, trade-ins, or negotiation → HUMAN_REVIEW_REQUIRED
- If the tone is angry, frustrated, urgent, or emotional → HUMAN_REVIEW_REQUIRED
- If the message asks multiple questions → HUMAN_REVIEW_REQUIRED
- If the intent is unclear or ambiguous → HUMAN_REVIEW_REQUIRED
- If the message is a simple availability question, basic info request, or confirmation → AUTO_REPLY_SAFE
- If the message asks to stop contact → EXPLICIT_OPTOUT
- If it is auto-reply/system noise → NON_LEAD

Return JSON ONLY in this exact format:
{{
  "classification": "AUTO_REPLY_SAFE|HUMAN_REVIEW_REQUIRED|EXPLICIT_OPTOUT|NON_LEAD",
  "confidence": 0.0,
  "reason": "short explanation"
}}

Email content:
\"\"\"{latest}\"\"\"
""".strip()

    try:
        resp = _oai.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=OPENAI_TEMPERATURE,  # should be 0
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "Return JSON only. No extra text."},
                {"role": "user", "content": prompt},
            ],
        )
        content = resp.choices[0].message.content or "{}"
        data = json.loads(content)

        cls = (data.get("classification") or "").strip().upper()
        conf = float(data.get("confidence") or 0.0)
        reason = (data.get("reason") or "").strip()

        if cls not in {"AUTO_REPLY_SAFE", "HUMAN_REVIEW_REQUIRED", "EXPLICIT_OPTOUT", "NON_LEAD"}:
            cls = "HUMAN_REVIEW_REQUIRED"
            reason = reason or "Invalid classification returned; defaulting to human review."

        # Confidence safety valve
        if cls == "AUTO_REPLY_SAFE" and conf < HUMAN_REVIEW_MIN_CONF:
            return {
                "classification": "HUMAN_REVIEW_REQUIRED",
                "confidence": conf,
                "reason": f"Classifier confidence below threshold ({HUMAN_REVIEW_MIN_CONF}). {reason}".strip()
            }

        return {"classification": cls, "confidence": conf, "reason": reason or "Classified by model."}

    except Exception as e:
        log.exception("GPT triage classification failed: %s", e)
        return {"classification": "HUMAN_REVIEW_REQUIRED", "confidence": 0.55, "reason": "Classifier failed; defaulting to human review."}


# -----------------------
# Salesperson resolution
# -----------------------

def _primary_salesperson_from_sales_team(sales_team: Any) -> Tuple[str, str]:
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
        # fallback first dict-ish entry
        primary = next((m for m in sales_team if isinstance(m, dict)), None)

    if not primary:
        return ("", "")

    fn = (primary.get("firstName") or "").strip()
    ln = (primary.get("lastName") or "").strip()
    nm = (f"{fn} {ln}").strip() or (primary.get("name") or "").strip() or ""
    em = (primary.get("email") or "").strip()
    return (nm, em)


def resolve_salesperson_contact(*, opportunity: dict, fresh_opp: Optional[dict] = None) -> Dict[str, str]:
    """
    Determine salesperson (name/email) from fresh_opp if possible, else opportunity snapshot.
    """
    st = (fresh_opp or {}).get("salesTeam")
    if st is None:
        st = opportunity.get("salesTeam")

    name, email = _primary_salesperson_from_sales_team(st)
    return {"name": name or "Sales Team", "email": email or ""}


# -----------------------
# Handoff implementation
# -----------------------

def handle_human_review_handoff(
    *,
    opportunity: dict,
    fresh_opp: Optional[dict],
    token: str,
    subscription_id: str,
    rooftop_name: str,
    inbound_subject: str,
    inbound_text: str,
    inbound_ts: Optional[str] = None,
    classification: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Executes the human handoff workflow:
      - Patch Airtable fields (Needs Human Review, reason, timestamps, notified flags)
      - Schedule Fortellis activity for salesperson (attached to opp)
      - Email salesperson + CC managers from Outlook
      - Log Fortellis comment to record what we did

    Returns summary dict.
    """

    opp_id = opportunity.get("opportunityId") or opportunity.get("id") or ""
    if not opp_id:
        raise ValueError("Missing opportunity id for human review handoff.")

    # Idempotency guard: if we've already notified, do nothing.
    patti_meta = opportunity.get("patti") or {}
    if isinstance(patti_meta, dict) and patti_meta.get("human_review_notified_at"):
        return {"ok": True, "skipped": True, "reason": "Already notified (patti.human_review_notified_at present).", "opp_id": opp_id}

    triage = classification or {"classification": "HUMAN_REVIEW_REQUIRED", "confidence": 0.0, "reason": ""}
    reason = (triage.get("reason") or "").strip()
    conf = triage.get("confidence")

    # Resolve rooftop info (sender/address), helpful for the internal email
    rt = get_rooftop_info(subscription_id) or {}
    rooftop_sender = rt.get("sender") or ""
    rooftop_addr = rt.get("address") or ""

    # Resolve salesperson
    sp = resolve_salesperson_contact(opportunity=opportunity, fresh_opp=fresh_opp)
    salesperson_name = sp.get("name") or "Sales Team"
    salesperson_email = sp.get("email") or ""

    # Customer info (best effort)
    cust = (fresh_opp or {}).get("customer") or opportunity.get("customer") or {}
    customer_name = (f"{(cust.get('firstName') or '').strip()} {(cust.get('lastName') or '').strip()}").strip() or "Customer"
    customer_email = ""
    customer_phone = ""

    emails = cust.get("emails") or []
    if isinstance(emails, list):
        for e in emails:
            if not isinstance(e, dict):
                continue
            if e.get("doNotEmail"):
                continue
            if e.get("isPreferred") and e.get("address"):
                customer_email = e["address"]
                break
        if not customer_email:
            for e in emails:
                if isinstance(e, dict) and e.get("address") and not e.get("doNotEmail"):
                    customer_email = e["address"]
                    break

    phones = cust.get("phones") or []
    if isinstance(phones, list):
        for p in phones:
            if not isinstance(p, dict):
                continue
            if p.get("isPreferred") and p.get("number"):
                customer_phone = p["number"]
                break
        if not customer_phone:
            for p in phones:
                if isinstance(p, dict) and p.get("number"):
                    customer_phone = p["number"]
                    break

    # Vehicle best-effort
    vehicle_str = ""
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
            vehicle_str = f"{year} {make} {model} {trim}".strip()

    # --- Airtable patch ---
    rec_id = opportunity.get("_airtable_rec_id")
    now_iso = inbound_ts or _now_iso_utc()

    if rec_id:
        at_fields = {
            AT_NEEDS_HUMAN_REVIEW: True,
            AT_HUMAN_REVIEW_REASON: f"{reason} (conf={conf})".strip(),
            AT_HUMAN_REVIEW_AT: now_iso,
            AT_HUMAN_REVIEW_NOTIFIED: True,
            AT_HUMAN_REVIEW_NOTIFIED_AT: now_iso,
        }
        try:
            patch_by_id(rec_id, at_fields)
        except Exception as e:
            log.exception("Airtable patch failed for rec_id=%s: %s", rec_id, e)

    # Also persist lightweight state into the opp blob for idempotency
    try:
        patti = opportunity.setdefault("patti", {})
        if not isinstance(patti, dict):
            opportunity["patti"] = {}
            patti = opportunity["patti"]
        patti["human_review_requested_at"] = now_iso
        patti["human_review_notified_at"] = now_iso
        patti["human_review_reason"] = reason
        # Persist full blob (best effort)
        save_opp(opportunity, extra_fields={})
    except Exception as e:
        log.warning("save_opp failed (ignored): %s", e)

    # --- Schedule Fortellis activity (for salesperson attention) ---
    due_utc = (datetime.now(timezone.utc) + timedelta(hours=HUMAN_REVIEW_DUE_HOURS)).strftime("%Y-%m-%dT%H:%M:%SZ")

    activity_comments = (
        "Patti flagged this lead for HUMAN REVIEW.\n"
        f"Reason: {reason} (conf={conf})\n"
        f"Customer: {customer_name} | {customer_email} | {customer_phone}\n"
        f"Vehicle: {vehicle_str or 'N/A'}\n"
        f"Latest message: { _safe_strip(inbound_text, 900) }\n"
    )

    try:
        schedule_activity(
            token,
            subscription_id,
            opp_id,
            due_dt_iso_utc=due_utc,
            activity_name="Patti: Human Review Needed",
            activity_type="Task",
            comments=activity_comments[:1800],
        )
    except Exception as e:
        log.warning("schedule_activity failed (ignored): %s", e)

    # --- Send internal Outlook email ---
    # If salesperson email is missing, still notify leadership so it doesn't disappear.
    to_email = salesperson_email or "alexc@pattersonautos.com"
    cc_list = _parse_cc_list(HUMAN_REVIEW_CC)

    subj = f"[Patti] Human review needed — {rooftop_name} — {customer_name}"
    if vehicle_str:
        subj += f" — {vehicle_str}"

    body = f"""
    <p><b>Patti flagged a lead for human attention.</b></p>

    <p><b>Rooftop:</b> {rooftop_name}<br>
    <b>Opp ID:</b> {opp_id}<br>
    <b>Salesperson:</b> {salesperson_name} ({salesperson_email or "unknown"})</p>

    <p><b>Customer:</b> {customer_name}<br>
    <b>Email:</b> {customer_email or "unknown"}<br>
    <b>Phone:</b> {customer_phone or "unknown"}<br>
    <b>Vehicle:</b> {vehicle_str or "unknown"}</p>

    <p><b>Classification:</b> HUMAN_REVIEW_REQUIRED<br>
    <b>Reason:</b> {reason or "N/A"}<br>
    <b>Confidence:</b> {conf}</p>

    <p><b>Inbound subject:</b> {inbound_subject or ""}</p>

    <p><b>Latest customer message:</b><br>
    <pre style="white-space:pre-wrap;font-family:Arial,Helvetica,sans-serif;">{_safe_strip(inbound_text, 2000)}</pre></p>

    <p><b>Next step:</b> Please take over this lead in the CRM and reply to the customer directly.</p>

    <p style="color:#666;font-size:12px;">
    Logged by Patti. Sent from: {rooftop_sender or "Patti inbox"}<br>
    {rooftop_addr}
    </p>
    """.strip()

    # Add CC in headers (Power Automate flow accepts headers dict)
    headers = {"cc": ",".join(cc_list)} if cc_list else {}

    try:
        send_email_via_outlook(
            to_addr=to_email,
            subject=subj[:180],
            html_body=body,
            headers=headers,
            timeout=15,
        )
    except Exception as e:
        log.exception("Failed to send Outlook human review email: %s", e)

    # --- Log to Fortellis as a comment (no customer send) ---
    try:
        add_opportunity_comment(
            token,
            subscription_id,
            opp_id,
            (
                "Patti HUMAN REVIEW handoff executed.\n"
                f"Notified: {to_email} (cc: {', '.join(cc_list)})\n"
                f"Reason: {reason} (conf={conf})\n"
                f"Latest msg: {_safe_strip(inbound_text, 600)}"
            )[:1800],
        )
    except Exception as e:
        log.warning("add_opportunity_comment failed (ignored): %s", e)

    return {
        "ok": True,
        "skipped": False,
        "opp_id": opp_id,
        "salesperson_email": salesperson_email,
        "cc": cc_list,
        "scheduled_due_utc": due_utc,
        "airtable_rec_id": rec_id,
    }
