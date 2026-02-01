from helpers import (
    rJson,
    wJson,
    getFirstActivity,
    adf_to_dict,
    getInqueryUsingAdf,
    get_names_in_dir,
    sortActivities
)
from kbb_ico import process_kbb_ico_lead 
from kbb_ico import _top_reply_only, _is_optout_text as _kbb_is_optout_text, _is_decline as _kbb_is_decline
from kbb_ico import _patch_address_placeholders, build_patti_footer, _PREFS_RE
from rooftops import get_rooftop_info
from constants import *
from gpt import run_gpt, getCustomerMsgDict, extract_appt_time
import re
import logging
import hashlib, json, time
from uuid import uuid4
from pathlib import Path
from zoneinfo import ZoneInfo
import uuid
from airtable_store import (
    find_by_opp_id, query_view, acquire_lock, release_lock,
    opp_from_record, save_opp, find_by_customer_email
)
from patti_mailer import _bump_ai_send_metrics_in_airtable

from fortellis import (
    get_activities,
    get_token,
    get_activity_by_id_v1,
    get_opportunity,
    add_opportunity_comment,
    schedule_activity,
    send_opportunity_email_activity,
    schedule_appointment_with_notify,
    set_opportunity_substatus,
)

from patti_common import _SCHED_ANY_RE, enforce_standard_schedule_sentence, EMAIL_RE
from patti_common import fmt_local_human, normalize_patti_body, append_soft_schedule_sentence, rewrite_sched_cta_for_booked 
from patti_triage import classify_inbound_email, handoff_to_human, should_triage
from airtable_store import find_by_customer_email

#from fortellis import get_vehicle_inventory_xml  
from inventory_matcher import recommend_from_xml

from datetime import datetime as _dt, timedelta as _td, timezone as _tz
import os
from dotenv import load_dotenv
load_dotenv()

def _norm_email(s: str | None) -> str | None:
    s = (s or "").strip().lower()
    return s if ("@" in s and "." in s) else None


RUN_KBB = os.getenv("RUN_KBB", "0").lower() in ("1", "true", "yes")

log = logging.getLogger(__name__)
OFFLINE_MODE = os.getenv("OFFLINE_MODE", "0").lower() in ("1", "true", "yes")

EXIT_KEYWORDS = [
    "not interested", "no longer interested", "bought elsewhere",
    "already purchased", "stop emailing", "unsubscribe",
    "please stop", "no thanks", "do not contact",
    "leave me alone", "sold my car", "found another dealer"
]
test_recipient = (os.getenv("TEST_RECIPIENT") or "").strip() or None

SALES_AI_EMAIL_DAYS = [1, 2, 3, 5, 8, 11, 14, 17, 21, 28, 31, 32, 34, 37, 40, 44, 51]

def _next_salesai_due_iso(*, created_iso: str, last_idx: int) -> str | None:
    """
    created_iso = lead create timestamp (UTC ISO)
    last_idx = index of last sent cadence email in SALES_AI_EMAIL_DAYS (start at -1)
    Returns next due date ISO in UTC, or None if cadence complete.
    """
    if last_idx + 1 >= len(SALES_AI_EMAIL_DAYS):
        return None

    created_dt = _dt.fromisoformat(str(created_iso).replace("Z", "+00:00"))
    day_num = SALES_AI_EMAIL_DAYS[last_idx + 1]
    due_dt = (created_dt + _td(days=int(day_num))).astimezone(_tz.utc)
    return due_dt.isoformat()


def is_exit_message(msg: str) -> bool:
    if not msg:
        return False
    msg_low = msg.lower()
    return any(k in msg_low for k in EXIT_KEYWORDS)


already_processed = get_names_in_dir("jsons/process")
DEBUGMODE = os.getenv("DEBUGMODE", "1") == "1"

import random

VARIANT_LONG = "A_long"
VARIANT_SHORT = "B_short"

def get_or_assign_ab_variant(opportunity: dict) -> str:
    """
    Assign once per opportunity and persist in opportunity['patti']['ab_variant'].
    Never re-randomize.
    """
    patti = opportunity.setdefault("patti", {})
    v = (patti.get("ab_variant") or "").strip()
    if v in (VARIANT_LONG, VARIANT_SHORT):
        return v

    # 50/50 split
    v = VARIANT_SHORT if random.random() < 0.5 else VARIANT_LONG
    patti["ab_variant"] = v
    return v

_VAGUE_TIME_WORDS_RE = re.compile(r"\b(later|tonight|this evening|this afternoon|after work)\b", re.I)
_HAS_DIGIT_RE = re.compile(r"\d")

# explicit time = contains an actual clock time or specific hour w/ am/pm
_HAS_EXPLICIT_TIME_RE = re.compile(
    r"""
    (
        \b\d{1,2}:\d{2}\s*(am|pm)?\b |      # 5:30, 17:00, 5:30pm
        \b\d{1,2}\s*(am|pm)\b |             # 5pm, 11 am
        \b(noon|midnight)\b                # noon, midnight
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

def explicit_time_ok(text: str) -> bool:
    t = text or ""

    # must contain an explicit time (not "later today", "this evening", etc.)
    if not _HAS_EXPLICIT_TIME_RE.search(t):
        return False

    # extra guard: vague words without digits should not pass
    if _VAGUE_TIME_WORDS_RE.search(t) and not _HAS_DIGIT_RE.search(t):
        return False

    return True


def _already_sent_first_touch_recently(*, customer_email: str, subscription_id: str, current_opp_id: str, lookback_hours: int = 72) -> bool:
    email = (customer_email or "").strip().lower()
    sub = (subscription_id or "").strip()
    if not email or not sub:
        return False

    recs = find_by_customer_email(email)  # your existing airtable_store helper
    if not recs:
        return False
    if isinstance(recs, dict):
        recs = [recs]

    cutoff = _dt.now(_tz.utc) - _td(hours=lookback_hours)

    for r in recs:
        opp_id = r.get("opportunityId") or r.get("opp_id") or ""
        if str(opp_id) == str(current_opp_id):
            continue

        r_sub = r.get("_subscription_id") or r.get("subscription_id") or ""
        if str(r_sub) != str(sub):
            continue

        sent_at = _parse_iso_utc(r.get("first_email_sent_at"))  # <-- Airtable field
        if sent_at and sent_at >= cutoff:
            return True

    return False


def airtable_save(opportunity: dict, extra_fields: dict | None = None):
    """
    Persist the opportunity back to Airtable using:
      - patti_json + patti_hash (snapshot)
      - key columns (follow_up_at, is_active, mode, Suppressed, etc.)
    Does NOT write opp_json.
    """
    if OFFLINE_MODE:
        return
    return save_opp(opportunity, extra_fields=extra_fields or {})


_KBB_SOURCES = {
    "kbb instant cash offer",
    "kbb servicedrive",
    "kbb service drive",
}

# --- Tustin Kia GM Day-2 email -------------------------------------------------

def is_tustin_kia_rooftop(rooftop_name: str) -> bool:
    return (rooftop_name or "").strip().lower() == "patterson tustin kia" or (rooftop_name or "").strip().lower() == "tustin kia"

TK_GM_DAY2_SUBJECT = "From the GM - How can I help?"

def build_tk_gm_day2_html(customer_name: str) -> str:
    cn = (customer_name or "there").strip()

    # project root = folder containing this file (processNewData.py)
    base_dir = Path(__file__).resolve().parent

    # template location in your repo
    tpl_path = base_dir / "templates" / "cadence" / "tustin_kia" / "day2_gm_email.html"

    html = tpl_path.read_text(encoding="utf-8")

    # Support either placeholder style (so you don’t have to remember which one you used)
    html = html.replace("{{customer_name}}", cn)
    html = html.replace("{customer_name}", cn)

    return html.strip()

def resolve_customer_email(
    opportunity: dict,
    *,
    SAFE_MODE: bool = False,
    test_recipient: str | None = None
) -> str | None:
    if SAFE_MODE:
        tr = (test_recipient or "").strip()
        return tr or None

    # Fortellis customer.emails (used for doNotEmail + fallback)
    cust = opportunity.get("customer") or {}
    emails = cust.get("emails") or []
    
    # DEBUG: Log what we're working with
    opp_id = opportunity.get("opportunityId") or opportunity.get("id") or "unknown"
    log.info("EMAIL_DEBUG opp=%s customer_email=%r cust.email=%r emails_count=%s",
             opp_id,
             opportunity.get("customer_email"),
             cust.get("email"),
             len(emails) if isinstance(emails, list) else "N/A")

    def _is_donot(addr: str) -> bool:
        target = (addr or "").strip().lower()
        if not target or not isinstance(emails, list):
            return False
        for e in emails:
            if not isinstance(e, dict):
                continue
            eaddr = (e.get("address") or "").strip().lower()
            if eaddr and eaddr == target:
                return bool(e.get("doNotEmail"))
        return False

    # ✅ Canonical: Airtable hydrated field (but honor doNotEmail if Fortellis knows it)
    air_email = (opportunity.get("customer_email") or "").strip()
    if air_email and not _is_donot(air_email):
        log.info("EMAIL_DEBUG opp=%s resolved from customer_email: %s", opp_id, air_email)
        return air_email
    
    # ✅ Fallback: customer.email from patti_json snapshot
    cust_email_direct = (cust.get("email") or "").strip()
    if cust_email_direct and not _is_donot(cust_email_direct):
        log.info("EMAIL_DEBUG opp=%s resolved from cust.email: %s", opp_id, cust_email_direct)
        return cust_email_direct

    # Fallback: Fortellis customer.emails (preferred first, else first deliverable)
    preferred = None
    first_ok = None
    if isinstance(emails, list):
        for e in emails:
            if not isinstance(e, dict):
                continue
            if e.get("doNotEmail"):
                continue
            addr = (e.get("address") or "").strip()
            if not addr:
                continue
            if not first_ok:
                first_ok = addr
            if e.get("isPreferred"):
                preferred = addr
                break

    result = preferred or first_ok
    log.info("EMAIL_DEBUG opp=%s resolved from emails list: %s", opp_id, result)
    return result



def maybe_send_tk_gm_day2_email(
    *,
    opportunity: dict,
    opportunityId: str,
    token: str,
    subscription_id: str,
    rooftop_name: str,
    rooftop_sender: str,
    customer_name: str,
    currDate,
    currDate_iso: str,
) -> bool:
    """
    Returns True if it sent (or OFFLINE_MODE), else False.
    Uses Airtable-stored state to avoid re-sends.
    Sends even if appointment is scheduled or opp is inactive.
    Still respects DoNotEmail on the email address itself.
    """

    # Rooftop gate
    if not is_tustin_kia_rooftop(rooftop_name):
        return False

    # ✅ ROOT GATE: Airtable checkbox (primary)
    if bool(opportunity.get("TK GM Day 2 Sent")):
        return False
    
    # ✅ SECONDARY GATE: Check last_template_day_sent >= 2 (backup if checkbox failed to persist)
    patti_meta = opportunity.get("patti") or {}
    last_sent = patti_meta.get("last_template_day_sent")
    if last_sent is not None and int(last_sent) >= 2:
        log.info("TK GM Day2: skipping opp=%s — last_template_day_sent=%s >= 2", opportunityId, last_sent)
        return False
        
    # Resolve customer email (preferred + not doNotEmail)
    # ✅ Resolve customer email from Airtable-hydrated field first
    to_addr = resolve_customer_email(opportunity)
    if not to_addr:
        log.warning("TK GM Day2: no deliverable email for opp=%s", opportunityId)
        return False

    # Optional: if Fortellis emails exist and mark this address doNotEmail, respect it
    cust = opportunity.get("customer") or {}
    emails = cust.get("emails") or []
    for e in emails:
        if isinstance(e, dict) and (e.get("address") or "").strip().lower() == to_addr.lower():
            if e.get("doNotEmail"):
                log.info("TK GM Day2: doNotEmail flagged for %s opp=%s", to_addr, opportunityId)
                return False

    body_html = build_tk_gm_day2_html(customer_name)

    if OFFLINE_MODE:
        sent_ok = True
    else:
        from patti_mailer import send_patti_email
        try:
            send_patti_email(
                token=token,
                subscription_id=subscription_id,
                opp_id=opportunityId,
                rooftop_name=rooftop_name,
                rooftop_sender=rooftop_sender,
                to_addr=to_addr,
                subject=TK_GM_DAY2_SUBJECT,
                body_html=body_html,
                cc_addrs=[],
            )
            sent_ok = True
        except Exception as e:
            log.warning("TK GM Day2 send failed opp=%s: %s", opportunityId, e)
            sent_ok = False

    if sent_ok:
    
        # Optional: record in thread history (helps auditing)
        opportunity.setdefault("messages", []).append({
            "msgFrom": "patti",
            "subject": TK_GM_DAY2_SUBJECT,
            "body": body_html,
            "date": currDate_iso,
            "trigger": "tk_gm_day2",
        })
        opportunity.setdefault("checkedDict", {})["last_msg_by"] = "patti"
    
        airtable_save(opportunity, extra_fields={
            "TK GM Day 2 Sent": True,
            "TK GM Day 2 Sent At": currDate_iso,
            "last_template_day_sent": 2,
        })

        try:
            _bump_ai_send_metrics_in_airtable(opportunityId)
        except Exception as e:
            log.warning("AI metrics update failed (non-blocking) opp=%s: %s", opportunityId, e)

    return sent_ok


# --- Tustin Kia Day-3 Walk-around Video Email --------------------------------

# Vehicle model (lowercase) -> YouTube walk-around video URL
# Based on Kia vehicle lineup commonly sold at Tustin Kia
# Vehicle model (lowercase) -> YouTube walk-around video URL
# TODO: Replace these placeholder video IDs with actual YouTube video IDs from Tustin Kia channel
# YouTube watch URLs should use format: https://www.youtube.com/watch?v={11-char-video-id}
KIA_WALKAROUND_VIDEOS = {
    "sportage": "https://www.youtube.com/watch?v=PLACEHOLDER_SPORTAGE",
    "telluride": "https://www.youtube.com/watch?v=PLACEHOLDER_TELLURIDE",
    "sorento": "https://www.youtube.com/watch?v=PLACEHOLDER_SORENTO",
    "soul": "https://www.youtube.com/watch?v=PLACEHOLDER_SOUL",
    "forte": "https://www.youtube.com/watch?v=PLACEHOLDER_FORTE",
    "k5": "https://www.youtube.com/watch?v=PLACEHOLDER_K5",
    "niro": "https://www.youtube.com/watch?v=PLACEHOLDER_NIRO",
    "niro ev": "https://www.youtube.com/watch?v=PLACEHOLDER_NIRO_EV",
    "stinger": "https://www.youtube.com/watch?v=PLACEHOLDER_STINGER",
    "carnival": "https://www.youtube.com/watch?v=PLACEHOLDER_CARNIVAL",
    "ev6": "https://www.youtube.com/watch?v=PLACEHOLDER_EV6",
    "ev9": "https://www.youtube.com/watch?v=PLACEHOLDER_EV9",
    "seltos": "https://www.youtube.com/watch?v=PLACEHOLDER_SELTOS",
    "rio": "https://www.youtube.com/watch?v=PLACEHOLDER_RIO",
}

TK_DAY3_WALKAROUND_SUBJECT = "Check out this walk-around video of your {vehicle_make} {vehicle_model}"


def get_walkaround_video_url(vehicle_model: str) -> str | None:
    """
    Returns the YouTube walk-around video URL for a vehicle model, or None if not found.
    Uses prefix matching to handle trim levels (e.g., "Rio LX" -> "rio").
    Matches the longest key first to avoid false positives.
    """
    model_lower = (vehicle_model or "").strip().lower()
    if not model_lower:
        return None
    
    # Direct match
    if model_lower in KIA_WALKAROUND_VIDEOS:
        return KIA_WALKAROUND_VIDEOS[model_lower]
    
    # Prefix/substring match: check if model_lower starts with or contains any known key
    # Sort by key length descending to match longest key first
    # This handles cases like "sportage lx" -> "sportage", and partial matches like "2024 sportage sx"
    for key in sorted(KIA_WALKAROUND_VIDEOS.keys(), key=len, reverse=True):
        if model_lower.startswith(key) or key in model_lower:
            return KIA_WALKAROUND_VIDEOS[key]
    
    return None


def _extract_vehicle_info(opportunity: dict) -> dict:
    """
    Extract vehicle year, make, model from opportunity's soughtVehicles.
    Returns dict with keys: year, make, model.
    Returns None if model cannot be determined (caller should skip Day 3).
    """
    opp_id = opportunity.get("opportunityId") or opportunity.get("id") or "unknown"
    
    soughtVehicles = opportunity.get("soughtVehicles") or []
    log.info("DAY3 VEHICLE DEBUG: opp=%s soughtVehicles=%r", opp_id, soughtVehicles)
    
    if not isinstance(soughtVehicles, list):
        soughtVehicles = []

    vehicleObj = None
    for v in soughtVehicles:
        if isinstance(v, dict) and v.get("isPrimary"):
            vehicleObj = v
            break
    if not vehicleObj:
        vehicleObj = (soughtVehicles[0] if soughtVehicles and isinstance(soughtVehicles[0], dict) else {})

    log.info("DAY3 VEHICLE DEBUG: opp=%s vehicleObj=%r", opp_id, vehicleObj)

    year = str(vehicleObj.get("yearFrom") or vehicleObj.get("year") or "").strip()
    make = str(vehicleObj.get("make") or "").strip()
    model = str(
        vehicleObj.get("model")
        or vehicleObj.get("modelFrom")
        or vehicleObj.get("modelTo")
        or vehicleObj.get("variant")
        or vehicleObj.get("description")
        or ""
    ).strip()

    # Fallback 1: Try opportunity["vehicle"] if available
    if not model:
        vehicle_str = opportunity.get("vehicle") or ""
        if isinstance(vehicle_str, str):
            model = vehicle_str.strip()
        elif isinstance(vehicle_str, dict):
            model = str(vehicle_str.get("model") or "").strip()
    
    # Fallback 2: Try extracting from notes using regex (common Kia models)
    if not model:
        notes = str(opportunity.get("notes") or "").lower()
        kia_models = ["sportage", "telluride", "sorento", "soul", "forte", "k5", "niro", "stinger", "carnival", "ev6", "ev9", "seltos", "rio"]
        for kia_model in kia_models:
            if kia_model in notes:
                model = kia_model.title()
                log.info("DAY3 VEHICLE DEBUG: opp=%s extracted model=%r from notes", opp_id, model)
                break
    
    # If still no model, return None to signal skip
    if not model:
        log.info("DAY3 VEHICLE DEBUG: opp=%s NO MODEL FOUND - skipping Day 3", opp_id)
        return None

    log.info("DAY3 VEHICLE DEBUG: opp=%s extracted year=%r make=%r model=%r", opp_id, year, make, model)

    return {
        "year": year,
        "make": make,
        "model": model,
    }


def build_tk_day3_walkaround_html(
    *,
    customer_name: str,
    vehicle_year: str,
    vehicle_make: str,
    vehicle_model: str,
    youtube_walkaround_url: str,
) -> str:
    """Build Day 3 walk-around email HTML from template."""
    cn = (customer_name or "there").strip()

    base_dir = Path(__file__).resolve().parent
    tpl_path = base_dir / "templates" / "cadence" / "tustin_kia" / "day3_walkaround_email.html"

    html = tpl_path.read_text(encoding="utf-8")

    # Replace all placeholders
    html = html.replace("{{customer_name}}", cn)
    html = html.replace("{{vehicle_year}}", vehicle_year or "")
    html = html.replace("{{vehicle_make}}", vehicle_make or "Kia")
    html = html.replace("{{vehicle_model}}", vehicle_model or "vehicle")
    html = html.replace("{{youtube_walkaround_url}}", youtube_walkaround_url or "")

    return html.strip()


def _norm_phone_e164_us_local(raw: str) -> str:
    """Normalize phone to E.164 format for US numbers."""
    raw = (raw or "").strip()
    if not raw:
        return ""
    digits = re.sub(r"\D+", "", raw)
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    if raw.startswith("+") and len(digits) >= 10:
        return "+" + digits
    return ""


def maybe_send_tk_day3_walkaround(
    *,
    opportunity: dict,
    opportunityId: str,
    token: str,
    subscription_id: str,
    rooftop_name: str,
    rooftop_sender: str,
    customer_name: str,
    currDate,
    currDate_iso: str,
    SAFE_MODE: bool = False,
    test_recipient: str | None = None,
) -> bool:
    """
    Send Day 3 walk-around video email for Tustin Kia leads.
    
    Returns True if sent, else False.
    
    Conditions:
    - Tustin Kia rooftop only
    - mode == "cadence"
    - TK Day 3 Walkaround Sent != True
    - last_template_day_sent == 2
    - Vehicle has a matching walk-around video
    """
    
    log.info("DAY3 DEBUG: entering Day 3 check for opp=%s", opportunityId)

    # Rooftop gate
    if not is_tustin_kia_rooftop(rooftop_name):
        log.info("DAY3 DEBUG: skipping opp=%s — not Tustin Kia", opportunityId)
        return False

    # Already sent gate
    if bool(opportunity.get("TK Day 3 Walkaround Sent")):
        log.info("DAY3 DEBUG: skipping opp=%s — already sent", opportunityId)
        return False
    
    # Mode and cadence state gates
    patti_meta = opportunity.get("patti") or {}
    mode = (patti_meta.get("mode") or "").strip().lower()
    last_template_day_sent = patti_meta.get("last_template_day_sent")
    
    log.info("DAY3 DEBUG: opp=%s mode=%r last_template_day_sent=%r", opportunityId, mode, last_template_day_sent)
    
    # Must be in cadence mode
    if mode != "cadence":
        log.info("DAY3 DEBUG: skipping opp=%s — mode is not 'cadence'", opportunityId)
        return False
    
    # Must have sent Day 2 (last_template_day_sent == 2)
    if last_template_day_sent != 2:
        log.info("DAY3 DEBUG: skipping opp=%s — last_template_day_sent=%r (need 2)", opportunityId, last_template_day_sent)
        return False

    # Extract vehicle info
    vehicle_info = _extract_vehicle_info(opportunity)
    if not vehicle_info:
        log.info("DAY3 DEBUG: skipping opp=%s — no vehicle model found", opportunityId)
        return False
    
    vehicle_year = vehicle_info["year"]
    vehicle_make = vehicle_info["make"]
    vehicle_model = vehicle_info["model"]

    # Check if we have a walk-around video for this vehicle
    video_url = get_walkaround_video_url(vehicle_model)
    log.info("DAY3 DEBUG: video_url=%s for model=%r opp=%s", video_url, vehicle_model, opportunityId)
    
    if not video_url:
        log.info("DAY3 DEBUG: no video for model=%r opp=%s", vehicle_model, opportunityId)
        return False

    # Resolve customer email
    to_addr = resolve_customer_email(opportunity)
    if not to_addr:
        log.warning("DAY3 DEBUG: no deliverable email for opp=%s", opportunityId)
        return False
    
    log.info("DAY3 DEBUG: sending Day 3 email to %s for opp=%s", to_addr, opportunityId)

    # Check doNotEmail flag
    cust = opportunity.get("customer") or {}
    emails = cust.get("emails") or []
    for e in emails:
        if isinstance(e, dict) and (e.get("address") or "").strip().lower() == to_addr.lower():
            if e.get("doNotEmail"):
                log.info("DAY3 DEBUG: doNotEmail flagged for %s opp=%s", to_addr, opportunityId)
                return False

    # Build email from Day 3 template
    subject = TK_DAY3_WALKAROUND_SUBJECT.format(
        vehicle_make=vehicle_make or "Kia",
        vehicle_model=vehicle_model or "vehicle"
    )
    body_html = build_tk_day3_walkaround_html(
        customer_name=customer_name,
        vehicle_year=vehicle_year,
        vehicle_make=vehicle_make,
        vehicle_model=vehicle_model,
        youtube_walkaround_url=video_url,
    )

    # Send the email using Day 3 template ONLY
    log.info("DAY3 DEBUG: sending email for opp=%s subject=%r", opportunityId, subject)
    
    sent_ok = False
    if OFFLINE_MODE:
        sent_ok = True
    else:
        from patti_mailer import send_patti_email
        try:
            send_patti_email(
                token=token,
                subscription_id=subscription_id,
                opp_id=opportunityId,
                rooftop_name=rooftop_name,
                rooftop_sender=rooftop_sender,
                to_addr=to_addr,
                subject=subject,
                body_html=body_html,
                cc_addrs=[],
            )
            sent_ok = True
        except Exception as exc:
            log.error("DAY3 DEBUG: email send failed for opp=%s: %s", opportunityId, exc)
            return False

    log.info("DAY3 DEBUG: updating Airtable fields for opp=%s", opportunityId)
    
    # Update Airtable: mark Day 3 sent and advance cadence
    now_utc = _dt.now(_tz.utc)
    next_due = (now_utc + _td(days=2)).replace(microsecond=0).isoformat()
    
    opportunity["follow_up_at"] = next_due
    
    if not OFFLINE_MODE:
        try:
            airtable_save(opportunity, extra_fields={
                "TK Day 3 Walkaround Sent": True,
                "TK Day 3 Walkaround Sent At": currDate_iso,
                "last_template_day_sent": 3,
                "follow_up_at": next_due,
            })
        except Exception as e:
            log.warning("DAY3 DEBUG: Airtable save failed opp=%s (continuing): %s", opportunityId, e)

    log.info("DAY3 DEBUG: Day 3 complete for opp=%s to=%s next_due=%s", opportunityId, to_addr, next_due)
    log.info("✅ Sent TK Day 3 Walkaround for opp=%s to=%s", opportunityId, to_addr)
    return True


def _next_kbb_followup_iso(*, lead_age_days: int) -> str:
    """
    Returns the UTC iso timestamp for the next cadence day after lead_age_days.
    Uses your kbb_cadence.CADENCE keys (day numbers).
    """
    from kbb_cadence import CADENCE

    days_sorted = sorted(int(d) for d in CADENCE.keys())
    # pick next cadence day strictly greater than current day
    next_day = next((d for d in days_sorted if d > lead_age_days), None)

    # if nothing left, park far in future (no more nudges)
    if next_day is None:
        return (_dt.now(_tz.utc) + _td(days=365)).isoformat()

    delta_days = max(1, next_day - lead_age_days)  # safety: at least 1 day
    return (_dt.now(_tz.utc) + _td(days=delta_days)).isoformat()

def _is_exact_kbb_source(val) -> bool:
    return (val or "").strip().lower() in _KBB_SOURCES

def _is_exact_kbb_ico_flags(flags: dict | None, es_doc: dict | None = None) -> bool:
    src_fortellis = ((flags or {}).get("source") or "").strip()
    src_es        = ((es_doc or {}).get("source") or "").strip()
    return _is_exact_kbb_source(src_fortellis) or _is_exact_kbb_source(src_es)


STATE_KEYS = ("mode", "last_template_day_sent", "nudge_count",
              "last_customer_msg_at", "last_agent_msg_at")

def _state_signature(state: dict) -> str:
    base = {k: state.get(k) for k in STATE_KEYS}
    blob = json.dumps(base, sort_keys=True, separators=(',', ':'))
    return hashlib.md5(blob.encode("utf-8")).hexdigest()


def is_active_opp(opportunity: dict) -> bool:
    # Fortellis opp payloads typically include a status or flags you can check.
    status = (opportunity.get("status") or "").strip().lower()
    # Some payloads have booleans like "isActive" or "isClosed"
    is_active_flag = opportunity.get("isActive")
    is_closed_flag = opportunity.get("isClosed")

    if isinstance(is_active_flag, bool):
        return is_active_flag
    if isinstance(is_closed_flag, bool):
        return not is_closed_flag

    # Fallback on status text
    return status in {"open", "active", "in progress"}


def _lc(x):
    return str(x).strip().lower() if x is not None else ""

def _first_present_lc(doc, *keys):
    for k in keys:
        if doc and k in doc and doc[k] is not None:
            return _lc(doc[k])
    return ""

def _kbb_flags_from(opportunity_doc: dict, fresh_opp: dict | None) -> dict:
    # prefer fresh_opp fields, fall back to opportunity doc
    src  = _first_present_lc(fresh_opp, "source")    or _first_present_lc(opportunity_doc, "source")
    st   = _first_present_lc(fresh_opp, "status")    or _first_present_lc(opportunity_doc, "status")
    sub  = (_first_present_lc(fresh_opp, "subStatus", "substatus")
            or _first_present_lc(opportunity_doc, "subStatus", "substatus"))
    upt  = (_first_present_lc(fresh_opp, "upType", "uptype")
            or _first_present_lc(opportunity_doc, "upType", "uptype"))
    return {"source": src, "status": st, "substatus": sub, "uptype": upt}

def _is_kbb_ico(doc_flags: dict) -> bool:
    return (
        doc_flags["source"] == "kbb instant cash offer" and
        doc_flags["status"] == "active" and
        doc_flags["substatus"] == "new" and
        doc_flags["uptype"] == "campaign"
    )


def _is_kbb_ico_new_active(doc: dict) -> bool:
    source    = _get_lc(doc, "source")
    status    = _get_lc(doc, "status")
    substatus = _get_lc(doc, "subStatus", "substatus")
    uptype    = _get_lc(doc, "upType", "uptype")

    print("KBB detect →", {
        "source": source,
        "status": status,
        "substatus": substatus,
        "uptype": uptype,
    })

    return (
        source in _KBB_SOURCES and
        status == "active" and
        uptype == "campaign" and
        substatus in {"new", "working"}
    )

def _is_assigned_to_kristin(doc: dict) -> bool:
    """
    Return True if Kristin Nowzek appears on the sales team by name or email.
    """
    sales = (doc.get("salesTeam") or [])
    log.debug(
        "Assign check: %s",
        [{"fn": (m.get("firstName") or ""), "ln": (m.get("lastName") or ""), "em": (m.get("email") or "")}
         for m in sales]
    )
    for m in sales:
        fn = (m.get("firstName") or "").strip().lower()
        ln = (m.get("lastName") or "").strip().lower()
        em = (m.get("email") or "").strip().lower()
        if (fn == "kristin" and ln == "nowzek") or em in {
            "knowzek@pattersonautos.com", "knowzek@gmail.com"
        }:
            return True
    return False

def _parse_iso_utc(x):
    if not x:
        return None
    try:
        return _dt.fromisoformat(str(x).replace("Z", "+00:00")).astimezone(_tz.utc)
    except Exception:
        return None

def process_general_lead_convo_reply(
    *,
    opportunity: dict,
    inquiry_text: str,
    token: str,
    subscription_id: str,
    rooftop_name: str,
    rooftop_sender: str | None,
    SAFE_MODE: bool,
    test_recipient: str | None,
    inbound_ts: str | None = None,
    inbound_subject: str | None = None,
):
    """
    Instant reply for NON-KBB leads when a customer emails back.
    Reuses the same GPT + normalization + footer logic already used in processNewData.
    Sends to test_recipient when SAFE_MODE is True.
    Returns: (state_dict_or_none, action_taken_str)
    """

    customer_name = ((opportunity.get("customer") or {}).get("firstName") or "").strip() or "there"
    messages = opportunity.get("messages") or []

    prompt = f"""
    You are Patti, a helpful internet leads assistant for {rooftop_name}.
    Reply to the customer's latest message using the thread.

    Customer's latest message:
    \"\"\"{inquiry_text}\"\"\"

    Thread (Python list of dicts):
    {messages}

    Write a short email reply. Do not include any signature/footer; it will be appended.
    """

    response = run_gpt(
        prompt,
        customer_name,
        rooftop_name,
        prevMessages=True
    )
    subject   = response["subject"]
    body_html = response["body"]

    body_html = normalize_patti_body(body_html)
    body_html = _patch_address_placeholders(body_html, rooftop_name)

    # If your general-lead logic has appointment/booked logic, reuse it.
    # If not, default to soft CTA:
    body_html = append_soft_schedule_sentence(body_html, rooftop_name)

    body_html = _PREFS_RE.sub("", body_html).strip()
    body_html = body_html + build_patti_footer(rooftop_name)

    # Persist into thread
    opportunity.setdefault("messages", []).append({
        "msgFrom": "patti",
        "subject": subject,
        "body": body_html,
        "date": inbound_ts or _dt.now(_tz.utc).isoformat(),
        "action": response.get("action"),
        "notes": response.get("notes"),
    })

    # === SEND ===
    # When SAFE_MODE: send to your test inbox, NOT the customer
    # When not SAFE_MODE: send to customer and log/send activity as normal
    to_addr = None
    if SAFE_MODE:
        to_addr = test_recipient
    else:
        # pull customer email from opportunity.customer.emails
        cust = opportunity.get("customer") or {}
        emails = cust.get("emails") or []
        for e in emails:
            if e.get("doNotEmail"):
                continue
            if e.get("isPreferred"):
                to_addr = e.get("address")
                break
        if not to_addr and emails:
            to_addr = emails[0].get("address")

    if not to_addr:
        raise ValueError("No recipient email resolved for general lead reply")

    from patti_mailer import send_patti_email
    send_patti_email(
        token=token,
        subscription_id=subscription_id,
        opp_id=opportunity.get("opportunityId") or opportunity.get("id"),
        rooftop_name=rooftop_name,
        rooftop_sender=rooftop_sender,
        to_addr=to_addr,
        subject=subject,
        body_html=body_html,
        cc_addrs=[],
        # if your wrapper supports flags, pass safe/test metadata here
    )

    return ({"mode": "convo", "last_customer_msg_at": inbound_ts}, "replied_general_convo")


def checkActivities(opportunity, currDate, rooftop_name, activities_override=None):
    if activities_override is not None:
        activities = activities_override
    elif OFFLINE_MODE:
        activities = opportunity.get('completedActivitiesTesting', [])
    else:
        activities = opportunity.get('completedActivities', [])

    activities = sortActivities(activities)

    alreadyProcessedActivities = opportunity.get('alreadyProcessedActivities', {})
    currDate_iso = (currDate.astimezone(_tz.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Ensure checkedDict is always a dict on the opportunity
    checkedDict = opportunity.get('checkedDict') or {}
    if not isinstance(checkedDict, dict):
        checkedDict = {}
    opportunity["checkedDict"] = checkedDict  # <-- make it live on the opp
    
    subscription_id = opportunity.get('_subscription_id')
    messages = opportunity.get("messages", [])
    customerInfo = opportunity.get('customer', {})


    # Get a single token for this function, if needed
    if OFFLINE_MODE or DEBUGMODE:
        token = None
    else:
        token = get_token(subscription_id)

    for act in activities:
        activityId = act.get("activityId")
        if activityId in alreadyProcessedActivities:
            continue

        comments = (act.get("comments") or "")
        activityName = (act.get("activityName") or "").strip().lower()
        activityType = act.get("activityType")

        # 1) Our sentinel in any prior comment?
        if PATTI_FIRST_REPLY_SENTINEL in comments:
            checkedDict["patti_already_contacted"] = True
            continue

        if activityName == "read email" or activityType == 20:
            fullAct = act
            has_msg_body = bool(((act.get("message") or {}).get("body") or "").strip())
            
            if (not has_msg_body) and (not DEBUGMODE) and (not OFFLINE_MODE):
                fullAct = get_activity_by_id_v1(activityId, token, subscription_id)


            # --- KBB-style normalization: top reply only + plain-text fallback ---
            customerMsg = (fullAct.get("message") or {})
            raw_body_html = (customerMsg.get("body") or "").strip()
            customer_body = _top_reply_only(raw_body_html)

            if not customer_body:
                # Simple HTML → text fallback if _top_reply_only returns empty
                import re as _re
                no_tags = _re.sub(r"(?is)<[^>]+>", " ", raw_body_html)
                customer_body = _re.sub(r"\s+", " ", no_tags).strip()

            customerMsgDict = {
                "msgFrom": "customer",
                "customerName": customerInfo.get("firstName"),
                "subject": customerMsg.get("subject"),
                "body": customer_body,          # <-- use cleaned top-reply text
                "date": fullAct.get("completedDate"),
            }

            # append the customer's message to the thread
            opportunity.setdefault('messages', []).append(customerMsgDict)
            messages = opportunity['messages']
            checkedDict["last_msg_by"] = "customer"
            opportunity['checkedDict'] = checkedDict  # ensure persisted even if it was missing
            
            # 🚫 Unified opt-out / decline check — re-use KBB logic on the CLEANED body
            if _kbb_is_optout_text(customer_body) or _kbb_is_decline(customer_body):

                log.info("Customer opted out or declined interest. Marking opportunity inactive.")
                opportunity['isActive'] = False
                checkedDict['exit_reason'] = customer_body[:250]
                checkedDict['exit_type'] = "customer_declined"
                opportunity['checkedDict'] = checkedDict

                # mark this activity as processed with a minimal stub
                apa = opportunity.get("alreadyProcessedActivities") or {}
                if not isinstance(apa, dict):
                    apa = {}
                apa[activityId] = {
                    "activityId": activityId,
                    "completedDate": fullAct.get("completedDate"),
                    "activityType": fullAct.get("activityType"),
                    "activityName": fullAct.get("activityName"),
                }
                opportunity["alreadyProcessedActivities"] = apa

                if not OFFLINE_MODE:
                    opportunity["followUP_date"] = None
                    airtable_save(opportunity, extra_fields={"follow_up_at": None})

                wJson(opportunity, f"jsons/process/{opportunity['opportunityId']}.json")
                return
            
            # --- Step 2: try to auto-schedule an appointment from this reply ---
            created_appt_ok = False
            appt_human = None
            try:
                # Skip if we already know about a future appointment
                patti_meta = opportunity.get("patti") or {}
                appt_due_utc = patti_meta.get("appt_due_utc")
                already_scheduled = False
                if appt_due_utc:
                    try:
                        appt_dt = _dt.fromisoformat(str(appt_due_utc).replace("Z", "+00:00"))
                        if appt_dt > _dt.now(_tz.utc):
                            already_scheduled = True
                    except Exception:
                        pass

                appt_iso = ""
                conf = 0.0
                if not already_scheduled:
                    proposed = extract_appt_time(customer_body or "", tz="America/Los_Angeles")
                    appt_iso = (proposed.get("iso") or "").strip()
                    conf = float(proposed.get("confidence") or 0.0)


                if appt_iso and conf >= 0.60:
                    try:
                        dt_local = _dt.fromisoformat(appt_iso.replace("Z", "+00:00"))
                        due_dt_iso_utc = dt_local.astimezone(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                        
                        appt_human = fmt_local_human(dt_local)
                        
                        schedule_appointment_with_notify(
                            token,
                            subscription_id,
                            opportunity['opportunityId'],
                            due_dt_iso_utc=due_dt_iso_utc,
                            activity_name="Sales Appointment",
                            activity_type="Appointment",
                            comments=f"Auto-scheduled from Patti based on customer reply: {customer_body[:200]}",
                            opportunity=opportunity,
                            fresh_opp=fresh_opp if "fresh_opp" in locals() else {},
                            rooftop_name=rooftop_name,
                            appt_human=appt_human,
                            customer_reply=customer_body,
                        )
                        
                        created_appt_ok = True

                        
                        patti_meta["mode"] = "scheduled"
                        patti_meta["appt_due_utc"] = due_dt_iso_utc
                        # GPT reply will confirm this, so mark to prevent duplicates.
                        patti_meta["appt_confirm_email_sent"] = True
                        opportunity["patti"] = patti_meta

                        
                        log.info(
                            "✅ Auto-scheduled appointment from reply for %s at %s (conf=%.2f)",
                            opportunity['opportunityId'],
                            appt_human,
                            conf,
                        )
                    except Exception as e:
                        log.error(
                            "Failed to auto-schedule appointment from reply for %s (appt_iso=%r): %s",
                            opportunity['opportunityId'],
                            appt_iso,
                            e,
                        )
            except Exception as e:
                log.warning(
                    "Reply-based appointment detection failed for %s: %s",
                    opportunity.get('opportunityId'),
                    e,
                )

            # ✅ continue with GPT reply generation
            if created_appt_ok and appt_human:
                prompt = f"""
            The customer and Patti have been emailing about a potential sales appointment.

            Patti has just scheduled an appointment in the CRM based on the most recent customer reply.
            Appointment time (local dealership time): {appt_human}.

            Write Patti's next email reply using the messages list below. Patti should:
            - Warmly confirm the appointment for {appt_human}
            - Thank the customer and set expectations for the visit
            - NOT ask the customer to choose a time again.

            Here are the messages (Python list of dicts):
            {messages}
            """
            else:
                prompt = f"""
            You are replying to an ACTIVE email thread (not a first welcome email).
            
            Hard rules:
            - If the guest proposes a visit time (including casual phrasing like "tomorrow around 4"), confirm it confidently.
            - Do NOT ask them what day/time works best if they already proposed one.
            - Do NOT mention store hours unless the guest asks OR the proposed time is outside store hours.
            - Always include the address in the confirmation.
            
            Address: 28 B Auto Center Dr, Tustin, CA 92782
            
            Return ONLY valid JSON with keys: subject, body.
            
            Messages (python list of dicts):
            {messages}
            """.strip()

            response = run_gpt(
                prompt,
                customerInfo.get('firstName'),
                rooftop_name,
                prevMessages=True
            )

            
            subject   = response["subject"]
            body_html = response["body"]
            
            # strip any duplicated Patti signature the model added
            body_html = re.sub(
                r"(?is)(?:\n\s*)?patti\s*(?:\r?\n)+virtual assistant.*?$",
                "",
                body_html
            )

            # --- Normalize Patti body & add CTA + footer (same as initial email) ---
            
            # Clean up paragraphs / bullets
            body_html = normalize_patti_body(body_html)
            
            # Patch rooftop/address placeholders (e.g. LegacySalesApptSchLink, dealership name)
            body_html = _patch_address_placeholders(body_html, rooftop_name)
            
            # Decide which CTA behavior to use based on appointment state
            patti_meta = opportunity.get("patti") or {}
            mode = (patti_meta.get("mode") or "").strip().lower()
            
            sub_status = (
                (opportunity.get("subStatus") or opportunity.get("substatus") or "")
            ).strip().lower()
            
            has_booked_appt = (
                mode == "scheduled"
                or ("appointment" in sub_status)
                or bool(patti_meta.get("appt_due_utc"))
            )

            if has_booked_appt:
                body_html = rewrite_sched_cta_for_booked(body_html)
                body_html = _SCHED_ANY_RE.sub("", body_html).strip()
            else:
                body_html = body_html.strip()
            
            # Strip any extraneous prefs/unsubscribe footer GPT might add
            body_html = _PREFS_RE.sub("", body_html).strip()
            
            # Add Patti’s signature/footer with the Tustin Kia logo
            body_html = body_html + build_patti_footer(rooftop_name)

            
            opportunity['messages'].append({
                "msgFrom": "patti",
                "subject": subject,
                "body": body_html,
                "date": currDate_iso,
                "action": response.get("action"),
                "notes": response.get("notes"),
            })
            
            checkedDict['last_msg_by'] = "patti"
            opportunity['checkedDict'] = checkedDict
            
            # mark this Read Email activity as processed (stub only)
            apa = opportunity.get("alreadyProcessedActivities") or {}
            if not isinstance(apa, dict):
                apa = {}
            apa[activityId] = {
                "activityId":   fullAct.get("id") or activityId,
                "completedDate": fullAct.get("completedDate"),
                "activityType":  fullAct.get("activityType"),
                "activityName":  fullAct.get("activityName"),
            }
            opportunity["alreadyProcessedActivities"] = apa
            
            nextDate = currDate + _td(hours=24)
            opportunity['followUP_date']  = nextDate.isoformat()
            opportunity['followUP_count'] = 0
            
            # 🔔 NEW: send the follow-up email + persist to ES, then stop
            if not OFFLINE_MODE:
                # figure out sender from rooftop
                rt = get_rooftop_info(subscription_id)
                rooftop_sender = rt.get("sender") or TEST_FROM
            
                # ✅ Use Airtable-provided customer_email (no opp_json fallback)
                customer_email = (opportunity.get("customer_email") or "").strip() or None

                if not customer_email:
                    log.warning(
                        "No customer_email on opp %s (subscription_id=%s) — cannot send",
                        opportunity.get("opportunityId"),
                        subscription_id,
                    )

                if customer_email:
                    try:
                        from patti_mailer import send_patti_email

                        send_patti_email(
                            token=token,
                            subscription_id=subscription_id,
                            opp_id=opportunity["opportunityId"],
                            rooftop_name=rooftop_name,
                            rooftop_sender=rooftop_sender,
                            to_addr=customer_email,
                            subject=subject,
                            body_html=body_html,
                            cc_addrs=[],
                        )

                    except Exception as e:
                        log.warning(
                            "Failed to send Patti follow-up email for opp %s: %s",
                            opportunity["opportunityId"],
                            e,
                        )
            
                # persist updated opportunity (messages, followUP_date, etc.)
                airtable_save(opportunity)
            
            # write debug json + stop processing this opp for this run
            wJson(opportunity, f"jsons/process/{opportunity['opportunityId']}.json")
            return

def _derive_appointment_from_sched_activities(opportunity, tz_name="America/Los_Angeles"):
    """Inspect scheduledActivities for a future appointment and, if found,
    update opportunity['patti']['mode'] / ['appt_due_utc'] so Patti will
    pause cadence nudges once an appointment is on the books.
    Returns True if state was updated, False otherwise.
    """
    try:
        sched = opportunity.get("scheduledActivities") or []
        if not isinstance(sched, list):
            return False

        # If we already have a future appt_due_utc recorded, don't override it.
        patti_meta = opportunity.get("patti") or {}
        appt_due_utc = patti_meta.get("appt_due_utc")
        if appt_due_utc:
            try:
                existing_dt = _dt.fromisoformat(str(appt_due_utc).replace("Z", "+00:00"))
                if existing_dt > _dt.now(_tz.utc):
                    return False
            except Exception:
                # fall through and allow re-deriving if parsing fails
                pass

        now_utc = _dt.now(_tz.utc)
        candidates = []

        for a in sched:
            raw_name = (a.get("activityName") or a.get("name") or "").strip().lower()
            t = a.get("activityType")

            # Treat anything clearly labeled as an appointment as such
            t_str = str(t).strip().lower() if t is not None else ""
            is_appt = (
                "appointment" in raw_name
                or t_str in ("2", "appointment")
            )
            if not is_appt:
                continue

            # Many booking-link activities use dueDateTime / startDateTime
            due_raw = (
                a.get("dueDateTime")
                or a.get("dueDate")
                or a.get("startDateTime")
                or a.get("activityDate")
                or a.get("completedDate")
            )
            if not due_raw:
                continue
            try:
                due_dt = _dt.fromisoformat(str(due_raw).replace("Z", "+00:00"))
            except Exception:
                continue


            if due_dt > now_utc:
                candidates.append(due_dt)

        if not candidates:
            return False

        # Use the earliest future appointment
        due_dt = min(candidates)
        due_dt_iso_utc = due_dt.astimezone(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        patti_meta["mode"] = "scheduled"
        patti_meta["appt_due_utc"] = due_dt_iso_utc
        opportunity["patti"] = patti_meta

        return True
    except Exception as e:
        # Never break the main job because of a best-effort helper
        try:
            log.warning("Failed to derive appointment from scheduledActivities for %s: %s",
                        opportunity.get("opportunityId"), e)
        except Exception:
            pass
        return False


def processHit(hit):
    currDate = _dt.now(_tz.utc)
    currDate_iso = currDate.strftime("%Y-%m-%dT%H:%M:%SZ")

    inquiry_text = None  # ensure defined

    # Airtable mode: hit is an Airtable record
    fields = (hit.get("fields") or {})
    opportunityId = fields.get("opp_id")  # canonical

    if not opportunityId:
        log.warning(
            "Skipping Airtable record missing opp_id rec_id=%s fields_keys=%s fields=%r",
            hit.get("id"),
            sorted(list(fields.keys())),
            fields,
        )
        return

    # ✅ Build the working opportunity first (so we can safely reference it)
    opportunity = opp_from_record(hit)

    # Sanity: ensure ids
    opportunity["opportunityId"] = opportunity.get("opportunityId") or opportunityId
    opportunity["id"] = opportunity.get("id") or opportunity["opportunityId"]

    # --- HARD STOP: Needs Human Review ---
    # (covers cases where the Airtable checkbox exists AND cases where opp_from_record hydrated needs_human_review)
    needs_hr = bool(fields.get("Needs Human Review")) or bool(opportunity.get("needs_human_review"))
    if needs_hr:
        log.warning("Skipping opp %s: Needs Human Review is checked", opportunityId)
    
        # Keep snapshot consistent too
        p = opportunity.setdefault("patti", {})
        if isinstance(p, dict):
            p["mode"] = "handoff"
            if fields.get("Human Review Reason"):
                p["handoff"] = {"reason": fields.get("Human Review Reason"), "at": currDate_iso}
    
        if not OFFLINE_MODE:
            try:
                airtable_save(opportunity, extra_fields={
                    "follow_up_at": None,
                    # optional if you have this Airtable column:
                    # "mode": "handoff",
                })
            except Exception as e:
                log.warning("Failed to clear follow_up_at for HR opp=%s: %s", opportunityId, e)
    
        wJson(opportunity, f"jsons/process/{opportunityId}.json")
        return


    # (optional debug)
    opportunity["_airtable_fields"] = fields

    # ✅ IMPORTANT: processNewData expects followUP_date and isActive on the opp object
    if fields.get("follow_up_at") and not opportunity.get("followUP_date"):
        opportunity["followUP_date"] = fields.get("follow_up_at")

    if "is_active" in fields and "isActive" not in opportunity:
        opportunity["isActive"] = bool(fields.get("is_active"))

    # Optional convenience hydration (safe)
    if fields.get("customer_email") and not opportunity.get("customer_email"):
        opportunity["customer_email"] = (fields.get("customer_email") or "").strip()

    if fields.get("Customer First Name") and not opportunity.get("customer_first_name"):
        opportunity["customer_first_name"] = (fields.get("Customer First Name") or "").strip()

    if fields.get("Customer Last Name") and not opportunity.get("customer_last_name"):
        opportunity["customer_last_name"] = (fields.get("Customer Last Name") or "").strip()

    if opportunity.get("customer_email") and not isinstance(opportunity.get("customer"), dict):
        opportunity["customer"] = {"emails": [{"address": opportunity["customer_email"]}]}

    if not opportunity.get("isActive", True):
        print("pass...")
        return

    subscription_id = opportunity.get("_subscription_id") or fields.get("subscription_id") or fields.get("Subscription ID")
    if not subscription_id:
        log.warning("Skipping opp %s (missing subscription_id)", opportunityId)
        return


    # Reuse a single token for this whole processHit run
    if OFFLINE_MODE:
        token = None
    else:
        token = get_token(subscription_id)

    # EARLY SKIP: avoid API calls for opps already marked inactive
    patti_meta = opportunity.get("patti") or {}
    if patti_meta.get("skip") and patti_meta.get("skip_reason") == "inactive_opportunity":
        log.info("Skipping opp %s (inactive_opportunity in ES).", opportunityId)
        return


    # --- Normalize testing arrays so live runs never use them for logic ---
    if OFFLINE_MODE:
        opp_messages = (opportunity.get("completedActivitiesTesting")
                        or opportunity.get("messages") or [])
    else:
        opportunity.pop("completedActivitiesTesting", None)
        # keep messages only for display/logs; don't base behavior on it
        opp_messages = []


    checkedDict = opportunity.get("checkedDict", {})

    # remove it later
    # if f"{opportunityId}.json" in already_processed:
    #     return

    # --- Customer: tolerate missing + self-heal from Fortellis ---
    customer = opportunity.get("customer") or {}

    # ✅ Prefer Airtable-hydrated name first
    customer_name = (opportunity.get("customer_first_name") or "").strip() or customer.get("firstName") or "there"

    customerId = customer.get("id")
    
    if not customerId and not OFFLINE_MODE:
        try:
            fresh_opp = get_opportunity(opportunityId, token, subscription_id)
            if isinstance(fresh_opp, dict):
                # hydrate missing customer
                if fresh_opp.get("customer"):
                    opportunity["customer"] = fresh_opp.get("customer") or {}
                    customer = opportunity["customer"]
                    customerId = customer.get("id")
    
                # hydrate other commonly-missing fields
                if fresh_opp.get("salesTeam") is not None:
                    opportunity["salesTeam"] = fresh_opp.get("salesTeam") or []
                if fresh_opp.get("source") is not None:
                    opportunity["source"] = fresh_opp.get("source")
                if fresh_opp.get("upType") is not None:
                    opportunity["upType"] = fresh_opp.get("upType")
                if fresh_opp.get("status") is not None:
                    opportunity["status"] = fresh_opp.get("status")
                if fresh_opp.get("subStatus") is not None:
                    opportunity["subStatus"] = fresh_opp.get("subStatus")
                if fresh_opp.get("isActive") is not None:
                    opportunity["isActive"] = fresh_opp.get("isActive")
    
                # persist once so future runs are clean
                try:
                    airtable_save(opportunity)
                except Exception as e:
                    log.warning("Airtable save failed opp=%s (continuing): %s",
                                opportunity.get("opportunityId") or opportunity.get("id"), e)

    
        except Exception as e:
            log.warning("Customer hydrate failed opp=%s err=%s", opportunityId, e)
    
    # final safety gate
    if not customerId:
        log.warning("Opp %s missing customer.id after hydrate; skipping.", opportunityId)
        return

    # ✅ Customer email should come ONLY from Airtable
    customer_email = (opportunity.get("customer_email") or "").strip() or None
    
    # ✅ Customer name should come from Airtable first-name first
    customer_name = (opportunity.get("customer_first_name") or "").strip() or customer.get("firstName") or "there"
    
    # Optional safety: if Fortellis marks THIS SAME address as doNotEmail, respect it
    if customer_email:
        for e in (customer.get("emails") or []):
            if not isinstance(e, dict):
                continue
            if (e.get("address") or "").strip().lower() == customer_email.lower() and e.get("doNotEmail"):
                log.info("doNotEmail flagged for %s opp=%s", customer_email, opportunityId)
                customer_email = None
                break


    # --- Getting primary salesperson (robust) ---
    salesTeam = opportunity.get("salesTeam") or []
    if not isinstance(salesTeam, list):
        salesTeam = []
    
    salesPersonObj = None
    for sales in salesTeam:
        if not isinstance(sales, dict):
            continue
        if str(sales.get("isPrimary")).lower() in ("true", "1", "yes"):
            salesPersonObj = sales
            break
    
    # fallback if nothing found
    if not isinstance(salesPersonObj, dict):
        log.warning("No valid primary salesperson found for opp_id=%s", opportunity.get("id"))
        salesPersonObj = (salesTeam[0] if salesTeam and isinstance(salesTeam[0], dict) else {})
    
    first_name = (salesPersonObj.get("firstName") or "").strip()
    last_name  = (salesPersonObj.get("lastName") or "").strip()
    full_name  = (f"{first_name} {last_name}").strip()
    
    salesperson = (
        SALES_PERSON_MAP.get(first_name)
        or SALES_PERSON_MAP.get(full_name)
        or full_name
        or "our team"
    )


    source = opportunity.get("source", "")
    sub_source = opportunity.get("subSource", "")

    # --- Rooftop resolution (from Subscription-Id) ---
    rt = get_rooftop_info(subscription_id)
    rooftop_name   = rt.get("name")   or "Patterson Auto Group"
    rooftop_sender = rt.get("sender") or TEST_FROM
    rooftop_addr   = rt.get("address") or ""

    dealership = (
        DEALERSHIP_MAP.get(first_name)
        or DEALERSHIP_MAP.get(full_name)
        or DEALERSHIP_MAP.get(source)
        or DEALERSHIP_MAP.get(sub_source)
        or rooftop_name
    )
    
    # 🔒 Fresh active-check from Fortellis (ES can be stale)

    try:
        # token was fetched once at the top of processHit
        fresh_opp = get_opportunity(opportunityId, token, subscription_id) if not OFFLINE_MODE else opportunity

        # Clear any prior transient_error now that the fetch succeeded
        if not OFFLINE_MODE:
            opportunity.setdefault("patti", {})["transient_error"] = None
            try:
                airtable_save(opportunity)
            except Exception as e:
                log.warning("Airtable save failed opp=%s (continuing): %s",
                            opportunity.get("opportunityId") or opportunity.get("id"), e)



    except Exception as e:
        # Downgrade to a transient error so we retry next run (no hard skip)
        log.warning("Transient get_opportunity failure for %s: %s", opportunityId, str(e)[:200])
        if not OFFLINE_MODE:
            # increment a lightweight failure counter using what we already have in memory
            prev = (opportunity.get("patti") or {}).get("transient_error") or {}
            fail_count = (prev.get("count") or 0) + 1
            # update in-memory opp then persist to Airtable
            patti = opportunity.setdefault("patti", {})
            patti["transient_error"] = {
                "code": "get_opportunity_failed",
                "message": str(e)[:200],
                "at": _dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "count": fail_count,
            }
            patti["skip"] = False
            patti["skip_reason"] = None
            
            # Persist blob to Airtable (instead of ES partial update)
            if not OFFLINE_MODE:
                try:
                    airtable_save(opportunity)
                except Exception as e:
                    log.warning("Airtable save failed opp=%s (continuing): %s",
                                opportunity.get("opportunityId") or opportunity.get("id"), e)

        # We can’t proceed without fresh_opp; exit gracefully and let the next run retry.
        return
    
    # keep this — we still skip inactive opps
    if not is_active_opp(fresh_opp):
        log.info("Skipping opp %s (inactive from Fortellis).", opportunityId)
        if not OFFLINE_MODE:
            patti = opportunity.setdefault("patti", {})
            patti["skip"] = True
            patti["skip_reason"] = "inactive_opportunity"
            patti["transient_error"] = None
            patti["inactive_at"] = _dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            patti["inactive_snapshot"] = {
                "status": fresh_opp.get("status"),
                "subStatus": fresh_opp.get("subStatus"),
                "isActive": fresh_opp.get("isActive"),
            }
            
            # optional (but recommended): also mark inactive at the top level so your Due Now view stops pulling it
            opportunity["isActive"] = False
            
            if not OFFLINE_MODE:
                # also clear follow-up so it won't keep showing as due
                opportunity["followUP_date"] = None
                try:
                    airtable_save(opportunity)
                except Exception as e:
                    log.warning(
                        "Airtable save failed opp=%s (continuing): %s",
                        opportunity.get("opportunityId") or opportunity.get("id"),
                        e,
                    )
        return

    
    # === KBB routing ===
    flags = _kbb_flags_from(opportunity, fresh_opp)
    log.info("KBB detect → %s", flags)
    is_kbb = _is_exact_kbb_ico_flags(flags, opportunity)
    
    # Ensure is_kbb is always defined before use
    if is_kbb and not RUN_KBB:
        log.info("Skipping KBB on this service (RUN_KBB=0). opp=%s", opportunityId)
        return  # skip this opp and stop processing this hit
    

    # --- BEGIN: ensure dateIn is present for cadence math (no external vars required) ---
    def _parse_iso_safe(s):
        try:
            return _dt.fromisoformat(str(s).replace("Z", "+00:00"))
        except Exception:
            return None
    
    def _to_iso_utc(dt):
        return dt.astimezone(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    if not opportunity.get("dateIn"):
        # Try to use a locally available activity history dict if present; otherwise fall back to fields on the opp.
        acts = None
        try:
            acts = activity_history  # optional; only if you actually have this in scope
        except NameError:
            acts = None
    
        # Pull completed/scheduled from the best available source
        if isinstance(acts, dict):
            comp  = acts.get("completedActivities")  or []
            sched = acts.get("scheduledActivities")  or []
        else:
            comp  = opportunity.get("completedActivities")  or []
            sched = opportunity.get("scheduledActivities")  or []
    
        candidates = []
    
        # 1) firstActivity.completedDate if present on the opp
        fa_dt = _parse_iso_safe(((opportunity.get("firstActivity") or {}).get("completedDate")))
        if fa_dt:
            candidates.append(fa_dt)
    
        # 2) earliest completed activity timestamp
        for a in comp:
            adt = _parse_iso_safe(a.get("completedDate") or a.get("activityDate"))
            if adt:
                candidates.append(adt)
    
        # 3) earliest scheduled dueDate that's already in the past (as a last resort)
        now_utc = _dt.now(_tz.utc)
        for a in sched:
            due = _parse_iso_safe(a.get("dueDate") or a.get("dueDateTime"))
            if due and due <= now_utc:
                candidates.append(due)
    
        if candidates:
            derived_datein_dt = min(candidates)
            opportunity["dateIn"] = _to_iso_utc(derived_datein_dt)
            log.info(
                "KBB dateIn derived → %s (opp=%s)",
                opportunity["dateIn"],
                opportunity.get("opportunityId") or opportunity.get("id")
            )
    # --- END: ensure dateIn is present for cadence math ---

    
    # Persona routing for exact KBB (ICO/ServiceDrive)
    if _is_exact_kbb_ico_flags(flags, opportunity):
        # Lead age (safe default)
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

        # --- DEBUG: lead-age source + math ---
        try:
            # Identify which field actually supplied created_raw
            if opportunity.get("dateIn"):
                _age_src = "dateIn"
            elif opportunity.get("createdDate"):
                _age_src = "createdDate"
            elif opportunity.get("created_at"):
                _age_src = "created_at"
            elif (opportunity.get("firstActivity") or {}).get("completedDate"):
                _age_src = "firstActivity.completedDate"
            else:
                _age_src = "None"
        
            log.info(
                "KBB age calc → src=%s created_raw=%r lead_age_days=%s opp=%s",
                _age_src,
                created_raw,
                lead_age_days,
                opportunity.get("opportunityId") or opportunity.get("id")
            )
        except Exception as _e:
            log.warning("KBB age calc debug failed: %s", _e)
        # --- /DEBUG ---
            
        # Try to surface any inquiry text we may already have; safe default to ""
        inquiry_text_safe = (opportunity.get("inquiry_text_body") or "").strip()
    
        # Hand off to the KBB ICO flow (templates + stop-on-reply convo)
        try:
            tok = None
            if not OFFLINE_MODE:
                tok = token

            state, action_taken = process_kbb_ico_lead(
                opportunity=opportunity,
                lead_age_days=lead_age_days,
                rooftop_name=rooftop_name,
                inquiry_text=inquiry_text_safe,
                token=tok,
                trigger="cron",
                subscription_id=subscription_id,
                SAFE_MODE=os.getenv("SAFE_MODE", "1") in ("1","true","True"),
                rooftop_sender=rooftop_sender,
            )
        
            # Always persist returned state back onto opp (some versions mutate, some return)
            if isinstance(state, dict):
                opportunity["_kbb_state"] = state
        
            # -----------------------------
            # Schedule next follow-up in Airtable (cadence only)
            # -----------------------------
            from kbb_cadence import CADENCE
        
            def _next_cadence_day(after_day: int) -> int | None:
                days = sorted(int(d) for d in CADENCE.keys())
                for d in days:
                    if d > int(after_day):
                        return d
                return None
        
            now_utc = _dt.now(_tz.utc)
        
            mode = (state.get("mode") if isinstance(state, dict) else None) or (opportunity.get("_kbb_state") or {}).get("mode")
        
            # If customer engaged, stop nudges (don’t keep it "Due Now")
            if mode == "convo":
                # Park follow-up far out (or set inactive if you prefer)
                opportunity["followUP_date"] = (now_utc + _td(days=365)).isoformat()
                opportunity.setdefault("checkedDict", {})["exit_type"] = opportunity.get("checkedDict", {}).get("exit_type") or "customer_engaged"
                log.info("KBB ICO: convo mode → parked followUP_date=%s opp=%s",
                         opportunity["followUP_date"], opportunityId)
        
            else:
                # Cadence mode: schedule next based on last_template_day_sent (preferred)
                last_sent_day = None
                if isinstance(state, dict):
                    last_sent_day = state.get("last_template_day_sent")
        
                # Fallback to lead_age_days if state didn't update
                anchor_day = int(last_sent_day) if isinstance(last_sent_day, int) and last_sent_day > 0 else int(lead_age_days or 0)
        
                next_day = _next_cadence_day(anchor_day)
        
                if next_day is not None:
                    delta_days = int(next_day) - int(anchor_day)
                    if delta_days <= 0:
                        delta_days = 1
        
                    # Anchor next due on the last template send time (or last agent send), NOT "now"
                    anchor_iso = (state.get("last_template_sent_at")
                                  or state.get("last_agent_msg_at")
                                  or opportunity.get("created_at"))
                    
                    anchor_dt = None
                    try:
                        if anchor_iso:
                            anchor_dt = _dt.fromisoformat(str(anchor_iso).replace("Z", "+00:00"))
                    except Exception:
                        anchor_dt = None
                    
                    if anchor_dt is None:
                        anchor_dt = now_utc  # fallback only if we truly have nothing
                    
                    next_due = (anchor_dt + _td(days=delta_days)).astimezone(_tz.utc).isoformat()
                    
                    # Only update if missing OR clearly wrong (prevents "rolling forward" every run)
                    curr_due_dt = _parse_iso_utc(opportunity.get("followUP_date"))
                    if (curr_due_dt is None) or (abs((curr_due_dt - _parse_iso_utc(next_due)).total_seconds()) > 60):
                        opportunity["followUP_date"] = next_due
                    
                    log.info("KBB ICO: scheduled next followUP_date=%s opp=%s anchor_day=%s next_day=%s anchor_iso=%s",
                             opportunity["followUP_date"], opportunityId, anchor_day, next_day, anchor_iso)

                else:
                    # No more nudges
                    opportunity["isActive"] = False
                    opportunity.setdefault("checkedDict", {})["exit_type"] = "cadence_complete"
                    # optional: park the date anyway for sorting/views
                    opportunity["followUP_date"] = (now_utc + _td(days=365)).isoformat()
                    log.info("KBB ICO: cadence complete → set inactive opp=%s", opportunityId)
        
            # Optional: write compact state note if we acted
            if action_taken:
                compact = {
                    "mode": state.get("mode") if isinstance(state, dict) else None,
                    "last_template_day_sent": state.get("last_template_day_sent") if isinstance(state, dict) else None,
                    "nudge_count": state.get("nudge_count") if isinstance(state, dict) else None,
                    "last_customer_msg_at": state.get("last_customer_msg_at") if isinstance(state, dict) else None,
                    "last_agent_msg_at": state.get("last_agent_msg_at") if isinstance(state, dict) else None,
                }
                note_txt = f"[PATTI_KBB_STATE] {json.dumps(compact, separators=(',',':'))}"
                if not OFFLINE_MODE:
                    add_opportunity_comment(tok, subscription_id, opportunityId, note_txt)
        
            # Persist updates (writes follow_up_at from followUP_date)
            if not OFFLINE_MODE:
                try:
                    airtable_save(opportunity)
                except Exception as e:
                    log.warning("Airtable save failed opp=%s (continuing): %s",
                                opportunity.get("opportunityId") or opportunity.get("id"), e)
        
        except Exception as e:
            log.exception("KBB ICO handler failed for opp %s: %s", opportunityId, e)
        
        # Do not fall through to general flow
        wJson(opportunity, f"jsons/process/{opportunityId}.json")
        return

    
    # === if we got here, proceed with the normal (non-KBB) flow ===

    # ========= Getting new activities from Fortellis (NON-KBB only) =====

    if OFFLINE_MODE:
        local_completed = opportunity.get("completedActivitiesTesting", []) or []
        activities = {"scheduledActivities": [], "completedActivities": local_completed}
    else:
        activities = get_activities(opportunityId, customerId, token, subscription_id)
    
    # Safety: if anything upstream handed us a list, coerce to the dict shape we expect
    if isinstance(activities, list):
        activities = {"scheduledActivities": [], "completedActivities": activities}
    
    currDate = _dt.now(_tz.utc)

    docToUpdate = {
        "scheduledActivities": activities.get("scheduledActivities", []),
        "completedActivities": activities.get("completedActivities", []),
        "updated_at": currDate.strftime("%Y-%m-%dT%H:%M:%SZ")  # or currDate.isoformat()
    }
    opportunity.update(docToUpdate)

    
    # Best-effort: if the CRM already has a future appointment scheduled
    # (for example, via a booking link), mirror that into Patti's state so
    # she pauses cadence nudges but continues to watch for replies.
    has_appt = _derive_appointment_from_sched_activities(opportunity)
    patti_meta = opportunity.get("patti") or {}
    if has_appt:
        patti_meta["mode"] = "scheduled"
        # if _derive_appointment_from_sched_activities returns / sets due date somewhere, store it:
        # patti_meta["appt_due_utc"] = derived_due_utc
        opportunity["patti"] = patti_meta

    
    # If we now know there’s an appointment, flip the CRM substatus in Fortellis
    if has_appt and not OFFLINE_MODE:
        try:
            resp = set_opportunity_substatus(
                token,
                subscription_id,
                opportunityId,
                sub_status="Appointment Set",
            )
            log.info(
                "Non-KBB appt: SubStatus update response: %s",
                getattr(resp, "status_code", "n/a"),
            )
        except Exception as e:
            log.warning("Non-KBB appt: set_opportunity_substatus failed: %s", e)

    # 🚫 Global guard: if this opp is already appointment-set, stop Patti's cadence
    patti_meta = opportunity.get("patti") or {}
    mode = patti_meta.get("mode")
    sub_status = (
        (fresh_opp.get("subStatus") or fresh_opp.get("substatus") or "")
        or (opportunity.get("subStatus") or opportunity.get("substatus") or "")
    ).strip().lower()

    has_booked_appt = (mode == "scheduled") or ("appointment" in sub_status)

    if has_booked_appt:
        log.info(
            "Opp %s has booked appointment (mode=%r, subStatus=%r); "
            "suppressing Patti follow-up cadence.",
            opportunityId,
            mode,
            sub_status,
        )
        opportunity["patti"] = patti_meta

        if not OFFLINE_MODE:
            opportunity.pop("completedActivitiesTesting", None)
            try:
                airtable_save(opportunity)
            except Exception as e:
                log.warning("Airtable save failed opp=%s (continuing): %s",
                            opportunity.get("opportunityId") or opportunity.get("id"), e)

        wJson(opportunity, f"jsons/process/{opportunityId}.json")
        return

    # normal ES cleanup when there is *no* appointment yet
    if not OFFLINE_MODE:
        opportunity.pop("completedActivitiesTesting", None)
        try:
            airtable_save(opportunity)
        except Exception as e:
            log.warning("Airtable save failed opp=%s (continuing): %s",
                        opportunity.get("opportunityId") or opportunity.get("id"), e)


    # === Vehicle & SRP link =============================================
    soughtVehicles = opportunity.get('soughtVehicles') or []
    if not isinstance(soughtVehicles, list):
        soughtVehicles = []
    vehicleObj = None
    for vehicle in soughtVehicles:
        if not vehicle.get('isPrimary'):
            continue
        vehicleObj = vehicle
        break

    if not vehicleObj:
        vehicleObj = (soughtVehicles[0] if soughtVehicles and isinstance(soughtVehicles[0], dict) else {})

    make  = str(vehicleObj.get("make") or "")
    model = str(vehicleObj.get("model") or "")
    year  = str(vehicleObj.get("yearFrom") or vehicleObj.get("year") or "")
    trim  = str(vehicleObj.get("trim") or "")
    stock = str(vehicleObj.get("stockNumber") or "")

    vehicle_str = f"{year} {make} {model} {trim}".strip() or "one of our vehicles"
    base_url = DEALERSHIP_URL_MAP.get(dealership)
    if base_url and (make and model):
        vehicle_str = f'<a href="{base_url}?make={make}&model={model}">{vehicle_str}</a>'

    from airtable_store import already_contacted_airtable, should_suppress_all_sends_airtable, get_mode_airtable, is_customer_replied_airtable

    completedActivities = activities.get("completedActivities", [])  # keep; you still need this for firstActivity parsing
    
    # ⛔ Global suppression (Airtable brain)
    stop, why = should_suppress_all_sends_airtable(opportunity)  # let helper decide now_utc
    if stop:
        log.info("⛔ Suppressed/blocked opp=%s — skipping sends (%s)", opportunityId, why)
        wJson(opportunity, f"jsons/process/{opportunityId}.json")
        return
    
    # ✅ Airtable-only "already contacted"
    patti_already_contacted = already_contacted_airtable(opportunity)


    if not patti_already_contacted:

        firstActivity = getFirstActivity(completedActivities)
        opportunity['firstActivity'] = firstActivity
    
        if firstActivity:
            firstActivityFull = None  # define up front for both branches
    
            if not OFFLINE_MODE:
                firstActivityFull = get_activity_by_id_v1(firstActivity['activityId'], token, subscription_id)
                firstActivityMessageBody = (firstActivityFull.get('message') or {}).get('body', '') or ''
            else:
                # OFFLINE: derive a body from newest local activity
                newest = (completedActivities[-1] if completedActivities else {}) or {}
                msg = newest.get("message") or {}
    
                firstActivityMessageBody = (msg.get("body") or newest.get("notes") or "").strip()
    
                # Create an offline "full" act so the rest of the code can store it
                
                firstActivityFull = {
                    "activityId": newest.get("activityId") or newest.get("id") or f"offline-{uuid.uuid4().hex[:8]}",
                    "completedDate": newest.get("completedDate")
                        or _dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "message": {"subject": newest.get("subject", ""), "body": firstActivityMessageBody},
                    "activityType": newest.get("activityType", 20),
                    "activityName": newest.get("activityName", "Read Email"),
                }
    
                # Keep firstActivity in sync with the id we will store
                firstActivity['activityId'] = firstActivityFull['activityId']
    
            # Parse the first message into ADF → plain text inquiry
            #firstActivityAdfDict = adf_to_dict(firstActivityMessageBody or "")
            #opportunity['firstActivityAdfDict'] = firstActivityAdfDict
            #inquiry_text_body = getInqueryUsingAdf(firstActivityAdfDict) or ""
            #opportunity['inquiry_text_body'] = inquiry_text_body

            raw_body = (firstActivityMessageBody or "").strip()
            
            def _looks_like_adf(s: str) -> bool:
                s0 = s.lstrip()
                return s0.startswith("<") and ("<adf" in s0.lower() or "<customer" in s0.lower())
            
            inquiry_text_body = ""
            firstActivityAdfDict = {}
            
            if raw_body and _looks_like_adf(raw_body):
                try:
                    firstActivityAdfDict = adf_to_dict(raw_body)
                    inquiry_text_body = getInqueryUsingAdf(firstActivityAdfDict) or ""
                except Exception:
                    # Not valid ADF—fallback to plaintext by stripping tags
                    inquiry_text_body = re.sub(r"<[^>]+>", "", raw_body)
            else:
                # Not XML/ADF—use plaintext (strip any HTML tags)
                inquiry_text_body = re.sub(r"<[^>]+>", "", raw_body)
            
            opportunity['firstActivityAdfDict'] = firstActivityAdfDict
            opportunity['inquiry_text_body'] = inquiry_text_body

            customerFirstMsgDict: dict = getCustomerMsgDict(inquiry_text_body)
            opportunity['customerFirstMsgDict'] = customerFirstMsgDict

            
            # Record this activity as processed (safe handling for list/dict types)
            apa = opportunity.get("alreadyProcessedActivities")
            if isinstance(apa, list):
                # Convert list of activities to dict keyed by activityId/id/index
                apa = {
                    str((a or {}).get("activityId") or (a or {}).get("id") or i): (a or {})
                    for i, a in enumerate(apa)
                    if isinstance(a, dict)
                }
            elif not isinstance(apa, dict) or apa is None:
                apa = {}
            opportunity["alreadyProcessedActivities"] = apa
            
            # Build a reliable key for this activity
            act_id = str(
                (firstActivity or {}).get("activityId")
                or (firstActivityFull or {}).get("activityId")
                or (firstActivity or {}).get("id")
                or (firstActivityFull or {}).get("id")
                or f"unknown-{uuid4().hex}"
            )
            
            # Save ONLY a minimal stub for this activity
            src = (firstActivityFull or firstActivity or {}) or {}
            opportunity["alreadyProcessedActivities"][act_id] = {
                "activityId": src.get("activityId") or src.get("id") or act_id,
                "completedDate": src.get("completedDate"),
                "activityType": src.get("activityType"),
                "activityName": src.get("activityName"),
            }

            # --- ensure the seeded customer message exists and is visible to the UI ---

            raw_inquiry = (opportunity.get('inquiry_text_body') or "").strip()
            if not raw_inquiry:
                raw_inquiry = (
                    ((firstActivityFull or {}).get('message', {}) or {}).get('subject', '') or
                    (firstActivityFull or {}).get('notes') or
                    (firstActivity or {}).get('title') or
                    "Hi! I'm interested in this vehicle and had a few questions."
                ).strip()
            
            # Find or initialize conversation array
            conv = (opportunity.get('messages')
                    or opportunity.get('conversation')
                    or opportunity.get('thread')
                    or [])
            if not isinstance(conv, list):
                conv = []
            
            # Append seed message if missing
            already = any(
                isinstance(m, dict) and m.get('role') == 'customer' and m.get('source') == 'seed'
                for m in conv
            )
            if not already:
                conv.append({
                    "id": f"cust-{act_id}",
                    "role": "customer",
                    "text": raw_inquiry,
                    "source": "seed",
                    "createdAt": currDate.isoformat()
                })
            
            # Write back to all likely keys so the UI sees it
            opportunity['messages'] = conv
            opportunity['conversation'] = conv
            opportunity['thread'] = conv
            
            # Optional: log for debugging
            print(f"[SEED] Added seed customer message. len={len(conv)} act_id={act_id}")

            try:
                inquiry_text = customerFirstMsgDict.get('customerMsg', None)
            except:
                pass

            # --- unified opt-out check on the very first inbound ---
            from patti_common import _is_optout_text, _is_decline
            
            if inquiry_text and (_is_optout_text(inquiry_text) or _is_decline(inquiry_text)):
                log.info("❌ Customer opted out on first message. Marking inactive.")
            
                checkedDict = opportunity.get("checkedDict") or {}
                checkedDict["exit_type"] = "customer_declined"
                checkedDict["exit_reason"] = (inquiry_text or "")[:250]
                opportunity["checkedDict"] = checkedDict
            
                opportunity["isActive"] = False
                opportunity["followUP_date"] = None    
            
                patti_meta = opportunity.get("patti") or {}
                patti_meta["email_blocked_do_not_email"] = True
                opportunity["patti"] = patti_meta
            
                if not OFFLINE_MODE:
                    airtable_save(opportunity, extra_fields={"follow_up_at": None})
            
                    try:
                        from fortellis import set_opportunity_inactive, set_customer_do_not_email
                        set_opportunity_inactive(
                            token,
                            subscription_id,
                            opportunityId,
                            sub_status="Not In Market",
                            comment="Customer opted out of communication."
                        )
                        set_customer_do_not_email(token, subscription_id, opportunityId)
                    except Exception as e:
                        log.error(f"Failed to set CRM inactive / do-not-email: {e}")
            
                wJson(opportunity, f"jsons/process/{opportunityId}.json")
                return
            
            if customerFirstMsgDict.get('salesAlreadyContact', False):
                opportunity['isActive'] = False
                opportunity["followUP_date"] = None   
                opportunity['checkedDict']['is_sales_contacted'] = True
                if not OFFLINE_MODE:
                    airtable_save(opportunity, extra_fields={"follow_up_at": None})

            
                wJson(opportunity, f"jsons/process/{opportunityId}.json")
                return


            # --- Step 3: try to auto-schedule an appointment from the inquiry text ---
            proposed = extract_appt_time(inquiry_text or "", tz="America/Los_Angeles")
            appt_iso = (proposed.get("iso") or "").strip()
            conf = float(proposed.get("confidence") or 0)

            created_appt_ok = False
            appt_human = None
            due_dt_iso_utc = None

            if appt_iso and conf >= 0.60:
                try:
                    # parse the local time and convert to UTC ISO
                    dt_local = _dt.fromisoformat(appt_iso.replace("Z", "+00:00"))
                    due_dt_iso_utc = dt_local.astimezone(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                    appt_human = fmt_local_human(dt_local)

                    schedule_appointment_with_notify(
                        token,
                        subscription_id,
                        opportunityId,
                        due_dt_iso_utc=due_dt_iso_utc,
                        activity_name="Sales Appointment",
                        activity_type="Appointment",
                        comments=f"Auto-scheduled from customer email: {inquiry_text[:180]}",
                        opportunity=opportunity,
                        fresh_opp=fresh_opp if "fresh_opp" in locals() else {},
                        rooftop_name=rooftop_name,
                        appt_human=appt_human,
                        customer_reply=inquiry_text,
                    )
                    
                    created_appt_ok = True
                    # 🔐 Store appointment state so future runs know this opp is scheduled
                    patti_meta = opportunity.get("patti") or {}
                    patti_meta["mode"] = "scheduled"
                    patti_meta["appt_due_utc"] = due_dt_iso_utc
                    # Patti will confirm this appointment in the outgoing email,
                    # so mark the confirmation as sent to avoid duplicates later.
                    patti_meta["appt_confirm_email_sent"] = True
                    opportunity["patti"] = patti_meta


                    log.info(
                        "✅ Auto-scheduled appointment for %s at %s (conf=%.2f)",
                        opportunityId,
                        appt_human,
                        conf,
                    )
                except Exception as e:
                    log.error(
                        "Failed to auto-schedule appointment for %s (appt_iso=%r): %s",
                        opportunityId,
                        appt_iso,
                        e,
                    )

        # Fallback inquiry text so we can still email even if parsing failed
        if not inquiry_text:
            inquiry_text = (opportunity.get("inquiry_text_body") or "").strip() or None

        sent_ok = send_first_touch_email(
            opportunity=opportunity,
            fresh_opp=fresh_opp,
            token=token,
            subscription_id=subscription_id,
            rooftop_name=rooftop_name,
            rooftop_sender=rooftop_sender,
            customer_name=customer_name,
            customer_email=customer_email,
            source=source,
            vehicle_str=vehicle_str,
            salesperson=salesperson,
            inquiry_text=inquiry_text,
            created_appt_ok=created_appt_ok,
            appt_human=appt_human,
            currDate=currDate,
            currDate_iso=currDate_iso,
            opportunityId=opportunityId,
            OFFLINE_MODE=OFFLINE_MODE,
        )
        
    else:
        # handle follow-ups messages
        checkActivities(opportunity, currDate, rooftop_name)
    
        # --- One-time confirmation for appointments booked via the online link ---
        patti_meta = opportunity.get("patti") or {}
        appt_due_utc = patti_meta.get("appt_due_utc")
        appt_confirm_sent = patti_meta.get("appt_confirm_email_sent", False)

        # If we see a scheduled appointment but Patti never confirmed it,
        # assume it came from the booking link and send a confirmation now.
        if appt_due_utc and not appt_confirm_sent:
            try:
                # Convert stored UTC ISO to local time for human-friendly text
                appt_dt_utc = _dt.fromisoformat(str(appt_due_utc).replace("Z", "+00:00"))
                # TODO: if you have per-rooftop timezones, swap this out
                local_tz = ZoneInfo("America/Los_Angeles")
                appt_dt_local = appt_dt_utc.astimezone(local_tz)

                appt_dt_local = appt_dt_utc.astimezone(local_tz)
                appt_human = fmt_local_human(appt_dt_local)
            except Exception:
                appt_human = appt_due_utc

            messages = opportunity.get("messages") or []
            prompt = f"""
            The customer used the online booking link and there is now a scheduled
            sales appointment in the CRM.

            Appointment time (local dealership time): {appt_human}.

            Write Patti's next email reply using the messages list below. Patti should:
            - Warmly confirm the appointment for {appt_human}
            - Thank the guest and set expectations for the visit
            - NOT ask the customer to choose a time again.

            Here are the messages (Python list of dicts):
            {messages}
            """

            response = run_gpt(
                prompt,
                customer_name,
                rooftop_name,
                prevMessages=True
            )
            subject   = response["subject"]
            body_html = response["body"]

            # Normalize + patch + CTA + footer (same as other Patti emails)
            body_html = normalize_patti_body(body_html)
            body_html = _patch_address_placeholders(body_html, rooftop_name)

            patti_meta = opportunity.get("patti") or {}
            mode = (patti_meta.get("mode") or "").strip().lower()
            
            sub_status = (
                (fresh_opp.get("subStatus") or fresh_opp.get("substatus") or "")
                or (opportunity.get("subStatus") or opportunity.get("substatus") or "")
            ).strip().lower()
            
            has_booked_appt = (mode == "scheduled") or ("appointment" in sub_status) or bool(patti_meta.get("appt_due_utc"))
            
            if has_booked_appt:
                body_html = rewrite_sched_cta_for_booked(body_html)
                body_html = _SCHED_ANY_RE.sub("", body_html).strip()
            else:
                if variant != VARIANT_SHORT:
                    body_html = append_soft_schedule_sentence(body_html, rooftop_name)

            body_html = _PREFS_RE.sub("", body_html).strip()
            body_html = body_html + build_patti_footer(rooftop_name)

            # Append to thread
            opportunity.setdefault("messages", []).append({
                "msgFrom": "patti",
                "subject": subject,
                "body": body_html,
                "date": currDate_iso,
                "action": response.get("action"),
                "notes": response.get("notes"),
            })

            checkedDict["last_msg_by"] = "patti"
            opportunity["checkedDict"] = checkedDict

            # Mark confirmation as sent so we never do this twice
            patti_meta["appt_confirm_email_sent"] = True
            opportunity["patti"] = patti_meta

            # Send email through Fortellis + persist
            if not OFFLINE_MODE:
                rt = get_rooftop_info(subscription_id)
                rooftop_sender = rt.get("sender") or TEST_FROM

                # ✅ Use customer email directly, no SAFE_MODE for production
                actual_to = resolve_customer_email(
                    opportunity,
                    SAFE_MODE=False,
                    test_recipient=None
                )
                
                if actual_to:
                    try:
                        from patti_mailer import send_patti_email
                
                        send_patti_email(
                            token=token,
                            subscription_id=subscription_id,
                            opp_id=opportunity["opportunityId"],
                            rooftop_name=rooftop_name,
                            rooftop_sender=rooftop_sender,
                            to_addr=actual_to,
                            subject=subject,
                            body_html=body_html,
                            cc_addrs=[],
                            force_mode="convo",          # ✅ this is a thread reply / confirmation
                            next_follow_up_at=None,      # ✅ no cadence scheduling here
                        )
                        sent_ok = True
                    except Exception as e:
                        log.warning("Failed to send appt confirmation opp=%s: %s", opportunityId, e)
                else:
                    log.warning("No recipient resolved for appt confirmation opp=%s", opportunityId)

                try:
                    airtable_save(opportunity)
                except Exception as e:
                    log.warning("Airtable save failed opp=%s (continuing): %s",
                                opportunity.get("opportunityId") or opportunity.get("id"), e)


            # Debug JSON + stop this run
            wJson(opportunity, f"jsons/process/{opportunity['opportunityId']}.json")
            return

        # ✅ NUMBER 3: Convo/reply gates (Airtable brain)
        mode = get_mode_airtable(opportunity)
        replied = is_customer_replied_airtable(opportunity)
        
        if mode == "convo" or replied:
            log.info("⏸ Skipping CADENCE follow-ups (mode=%r replied=%s) opp=%s", mode, replied, opportunityId)
            wJson(opportunity, f"jsons/process/{opportunityId}.json")
            return
        
        # ✅ Airtable cadence timing (follow_up_at is the brain)
        due_iso = (opportunity.get("follow_up_at") or "").strip()
        
        if not due_iso:
            seed_iso = (_dt.now(_tz.utc) + _td(hours=24)).replace(microsecond=0).isoformat()
            opportunity["follow_up_at"] = seed_iso
            if not OFFLINE_MODE:
                airtable_save(opportunity, extra_fields={"follow_up_at": seed_iso})
            wJson(opportunity, f"jsons/process/{opportunityId}.json")
            return
        
        try:
            due_dt = _dt.fromisoformat(due_iso.replace("Z", "+00:00"))
            if due_dt.tzinfo is None:
                due_dt = due_dt.replace(tzinfo=_tz.utc)
        except Exception:
            seed_iso = (_dt.now(_tz.utc) + _td(hours=24)).replace(microsecond=0).isoformat()
            opportunity["follow_up_at"] = seed_iso
            if not OFFLINE_MODE:
                airtable_save(opportunity, extra_fields={"follow_up_at": seed_iso})
            wJson(opportunity, f"jsons/process/{opportunityId}.json")
            return
        
        now_utc = _dt.now(_tz.utc)
        if due_dt > now_utc:
            wJson(opportunity, f"jsons/process/{opportunityId}.json")
            return
        
        # ✅ If we reach here: cadence is due now
        # Read followUP_count from Airtable record first, fallback to patti JSON
        followUP_count = opportunity.get("followUP_count")
        if followUP_count is None:
            followUP_count = (opportunity.get("patti") or {}).get("followUP_count")
        followUP_count = int(followUP_count or 0)
        
        # Read last_template_day_sent from Airtable record first, fallback to patti JSON
        last_template_day_sent = opportunity.get("last_template_day_sent")
        if last_template_day_sent is None:
            last_template_day_sent = (opportunity.get("patti") or {}).get("last_template_day_sent")
        
        # DAY3 DEBUG: Log values before condition check
        log.info("DAY3 DEBUG: mode=%s last_template_day_sent=%s followUP_count=%s due_dt=%s now=%s",
                 mode, last_template_day_sent, followUP_count, due_dt, now_utc)

    
        # --- Step 4A: Tustin Kia GM Day-2 email (send even if appointment exists) ---
        # Day-2 in your system = first follow-up run when due_dt is due
        if due_dt <= now_utc and followUP_count == 0:

            sent_gm = maybe_send_tk_gm_day2_email(
                opportunity=opportunity,
                opportunityId=opportunityId,
                token=token,
                subscription_id=subscription_id,
                rooftop_name=rooftop_name,
                rooftop_sender=rooftop_sender,
                customer_name=customer_name,
                currDate=currDate,
                currDate_iso=currDate_iso,
            )
        
            if sent_gm:
                # Advance cadence like a normal follow-up, so we don't also send GPT follow-up today
                next_due = (now_utc + _td(days=1)).replace(microsecond=0).isoformat()
                opportunity["follow_up_at"] = next_due
                opportunity["followUP_count"] = int(opportunity.get("followUP_count") or 0) + 1
        
                if not OFFLINE_MODE:
                    try:
                        extra = {"follow_up_at": next_due}
                        first_sent = opportunity.get("first_email_sent_at")
                        if first_sent:
                            extra["first_email_sent_at"] = first_sent
                        airtable_save(opportunity, extra_fields=extra)
                    except Exception as e:
                        log.warning(
                            "Airtable save failed opp=%s (continuing): %s",
                            opportunity.get("opportunityId") or opportunity.get("id"),
                            e
                        )

                wJson(opportunity, f"jsons/process/{opportunityId}.json")
                return

        # --- Step 4A.2: Tustin Kia Day-3 Walk-around Video email ---
        # Day 3 triggers when: mode=cadence, last_template_day_sent=2, not already sent
        day3_ready = (
            mode == "cadence"
            and last_template_day_sent == 2
            and not bool(opportunity.get("TK Day 3 Walkaround Sent"))
        )
        
        if day3_ready:
            log.info("DAY3 TRIGGER: Attempting Day 3 walkaround for opp=%s", opportunityId)

            sent_day3 = maybe_send_tk_day3_walkaround(
                opportunity=opportunity,
                opportunityId=opportunityId,
                token=token,
                subscription_id=subscription_id,
                rooftop_name=rooftop_name,
                rooftop_sender=rooftop_sender,
                customer_name=customer_name,
                currDate=currDate,
                currDate_iso=currDate_iso,
                SAFE_MODE=os.getenv("SAFE_MODE", "1") in ("1","true","True"),
                test_recipient=test_recipient,
            )

            if sent_day3:
                # Day 3 function already saved to Airtable and set next_due
                wJson(opportunity, f"jsons/process/{opportunityId}.json")
                return
        
        # --- Step 4B: pause cadence if there is an upcoming appointment (normal behavior) ---
        patti_meta = opportunity.get("patti") or {}
        appt_due_utc = patti_meta.get("appt_due_utc")
        if appt_due_utc:
            try:
                appt_dt = _dt.fromisoformat(str(appt_due_utc).replace("Z", "+00:00"))
                now_utc = _dt.now(_tz.utc)
                if appt_dt > now_utc:
                    log.info(
                        "⏸ Skipping cadence follow-up for %s — appointment already scheduled at %s",
                        opportunityId,
                        appt_dt.isoformat(),
                    )
                    wJson(opportunity, f"jsons/process/{opportunityId}.json")
                    return
            except Exception as e:
                log.warning(
                    "Failed to parse appt_due_utc %r for %s: %s",
                    appt_due_utc,
                    opportunityId,
                    e,
                )
    
        last_by = (opportunity.get('checkedDict') or {}).get('last_msg_by', '')

        if last_by == "customer":
            # customer replied; don't send an automated nudge
            wJson(opportunity, f"jsons/process/{opportunityId}.json")
            return

        patti = opportunity.get("patti") or {}
        idx = int(patti.get("salesai_email_idx") or -1)
        if due_dt <= now_utc and idx >= (len(SALES_AI_EMAIL_DAYS) - 1):
            opportunity['isActive'] = False
            opportunity["followUP_date"] = None   
            if not OFFLINE_MODE:
                try:
                    airtable_save(opportunity, extra_fields={"follow_up_at": None})
                except Exception as e:
                    log.warning("Airtable save failed opp=%s (continuing): %s",
                                opportunity.get("opportunityId") or opportunity.get("id"), e)
            wJson(opportunity, f"jsons/process/{opportunityId}.json")
            return

        elif due_dt <= now_utc:
            # Use full thread history but be explicit that this is NOT a first email.
            messages = opportunity.get("messages") or []
        
            prompt = f"""
        You are generating a FOLLOW-UP email, not a first welcome message.
        
        Context:
        - The guest originally inquired about: {vehicle_str}
        - Patti has already been in touch with the guest.

        Use the full message history below to see what’s already been discussed,
        then write the next short follow-up from Patti that makes sense given
        where the conversation left off.
        
        messages between Patti and the customer (python list of dicts):
        {messages}
        
        Follow-up requirements:
        - Do NOT repeat the full opener or dealership Why Buys from the first email.
        - Assume they already read your original message.
        - Keep it short: 2–4 sentences max.
        - Sound like you’re checking in on a thread you already started
          (e.g., “I wanted to follow up on my last note about the Sportage.”).
        - Make one simple, low-pressure ask (e.g., “Are you still considering the Sportage?” or
          “Would you like me to check availability or options for you?”).
        - Use a subject line that clearly looks like a follow-up on their vehicle inquiry,
          not a brand-new outreach.
        
        Return ONLY valid JSON with keys: subject, body.
            """.strip()
        
            response = run_gpt(
                prompt,
                customer_name,
                rooftop_name,
                prevMessages=True,
            )


            subject   = response["subject"]
            body_html = response["body"]

            body_html = re.sub(
                r"(?is)(?:\n\s*)?patti\s*(?:\r?\n)+virtual assistant.*?$",
                "",
                body_html
            )

            # ✅ Normalize + CTA + footer (match first-touch formatting)
            body_html = normalize_patti_body(body_html)
            body_html = _patch_address_placeholders(body_html, rooftop_name)
            body_html = append_soft_schedule_sentence(body_html, rooftop_name)
            body_html = _PREFS_RE.sub("", body_html).strip()
            body_html = body_html + build_patti_footer(rooftop_name)

            # ✅ Calculate next_due for cadence scheduling
            patti = opportunity.get("patti") or {}
            idx = int(patti.get("salesai_email_idx") or -1)
            created_iso = (
                patti.get("salesai_created_iso")
                or opportunity.get("created_at")
                or opportunity.get("dateIn")
                or opportunity.get("createdDate")
                or opportunity.get("updated_at")
            )
            next_due = _next_salesai_due_iso(created_iso=created_iso, last_idx=idx + 1) if created_iso else None
            if not next_due:
                # Fallback: schedule 2 days from now
                next_due = (_dt.now(_tz.utc) + _td(days=2)).replace(microsecond=0).isoformat()

            # ✅ SEND the follow-up (currently missing)
            sent_ok = False
            
            if not OFFLINE_MODE:
                from patti_mailer import send_patti_email  # wrapper: Outlook send + CRM comment
            
                # ✅ Use customer email directly, no SAFE_MODE for production cadence
                actual_to = resolve_customer_email(
                    opportunity,
                    SAFE_MODE=False,
                    test_recipient=None
                )
                
                if actual_to:
                    try:
                        send_patti_email(
                            token=token,
                            subscription_id=subscription_id,
                            opp_id=opportunityId,
                            rooftop_name=rooftop_name,
                            rooftop_sender=rooftop_sender,
                            to_addr=actual_to,
                            subject=subject,
                            body_html=body_html,
                            cc_addrs=[],
                        
                            force_mode="cadence",                 # ✅ tells Airtable this is cadence
                            next_follow_up_at=next_due,           # ✅ advances follow_up_at
                            template_day=int(patti.get("salesai_email_idx") or -1) + 2,
                        )
                        sent_ok = True
                    except Exception as e:
                        log.warning("Follow-up send failed for opp %s: %s", opportunityId, e)
                else:
                    log.warning("No customer email resolved for opp %s; skipping follow-up send", opportunityId)

            
            # Only record + advance cadence if we actually sent (or you're in OFFLINE_MODE)
            if sent_ok or OFFLINE_MODE:
                opportunity.setdefault("messages", []).append(
                    {
                        "msgFrom": "patti",
                        "subject": subject,
                        "body": body_html,
                        "date": currDate_iso,
                        "action": response.get("action"),
                        "notes": response.get("notes"),
                    }
                )
                opportunity.setdefault("checkedDict", {})["last_msg_by"] = "patti"
            
                # ✅ SalesAI cadence advance (instead of +1 day)
                patti = opportunity.setdefault("patti", {})
                idx = int(patti.get("salesai_email_idx") or -1)
                patti["salesai_email_idx"] = idx + 1
            
                patti = opportunity.get("patti") or {}
                created_iso = (
                    patti.get("salesai_created_iso")     # ✅ authoritative anchor
                    or opportunity.get("created_at")
                    or opportunity.get("dateIn")
                    or opportunity.get("createdDate")
                    or opportunity.get("updated_at")     # last resort
                    or currDate_iso
                )

                next_due = _next_salesai_due_iso(created_iso=created_iso, last_idx=idx + 1)
            
                if next_due is None:
                    opportunity["isActive"] = False
                    opportunity["followUP_date"] = None
                    if not OFFLINE_MODE:
                        try:
                            airtable_save(opportunity, extra_fields={"follow_up_at": None})
                        except Exception as e:
                            log.warning("Airtable save failed opp=%s (continuing): %s",
                                        opportunity.get("opportunityId") or opportunity.get("id"), e)
                else:
                    opportunity["followUP_date"] = next_due
                    opportunity["followUP_count"] = int(opportunity.get("followUP_count") or 0) + 1
                    if not OFFLINE_MODE:
                        try:
                            airtable_save(opportunity, extra_fields={"follow_up_at": next_due})
                        except Exception as e:
                            log.warning("Airtable save failed opp=%s (continuing): %s",
                                        opportunity.get("opportunityId") or opportunity.get("id"), e)
            
                if not OFFLINE_MODE:
                    try:
                        airtable_save(opportunity)
                    except Exception as e:
                        log.warning("Airtable save failed opp=%s (continuing): %s",
                                    opportunity.get("opportunityId") or opportunity.get("id"), e)

    
    wJson(opportunity, f"jsons/process/{opportunityId}.json")

_CARFAX_EMAIL_RE = re.compile(r"(?i)\bEmail:\s*([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})\b")

def _first_email_in_text(text: str) -> str | None:
    if not text:
        return None
    m = _CARFAX_EMAIL_RE.search(text) or EMAIL_RE.search(text)
    return m.group(1).strip() if m else None

def _build_email_context(*, opportunity: dict, fresh_opp: dict, subscription_id: str, token: str | None):
    """
    Resilient context derivation:
    - customer_name, customer_email (with fallbacks for internet leads)
    - salesperson
    - rooftop_name, rooftop_sender
    - vehicle_str
    - source/sub_source
    """
    # --- Customer ---
    customer = opportunity.get("customer") or {}

    # ✅ Prefer Airtable-hydrated name first
    customer_name = (opportunity.get("customer_first_name") or "").strip() or customer.get("firstName") or "there"

    # 1) Preferred email from opportunity.customer.emails
    customer_email = None
    customer_emails = customer.get("emails", []) or []
    for e in customer_emails:
        if not isinstance(e, dict):
            continue
        if e.get("doNotEmail") or not e.get("isPreferred"):
            continue
        if e.get("address"):
            customer_email = e.get("address")
            break
    if not customer_email:
        for e in customer_emails:
            if not isinstance(e, dict):
                continue
            if e.get("doNotEmail"):
                continue
            if e.get("address"):
                customer_email = e.get("address")
                break

    # 2) Try fresh_opp customer-ish fields (varies by endpoint/schema)
    if not customer_email:
        cust = (
            fresh_opp.get("customer")
            or fresh_opp.get("customerInfo")
            or fresh_opp.get("customer_info")
            or {}
        )
        if isinstance(cust, dict):
            # common variants
            for key in ("email", "emailAddress", "primaryEmail", "email_address"):
                val = cust.get(key)
                if val and isinstance(val, str) and "@" in val:
                    customer_email = val.strip()
                    break
            # sometimes emails are a list of dicts too
            if not customer_email:
                emails2 = cust.get("emails") or []
                if isinstance(emails2, list):
                    for e in emails2:
                        if isinstance(e, dict) and e.get("address") and not e.get("doNotEmail"):
                            customer_email = str(e["address"]).strip()
                            break

    # 3) Parse from message history (CARFAX bodies contain "Email: ...")
    if not customer_email:
        msgs = opportunity.get("messages") or []
        for m in reversed(msgs):
            if not isinstance(m, dict):
                continue
            txt = " ".join([
                str(m.get("body") or ""),
                str(m.get("body_text") or ""),
                str(m.get("text") or ""),
            ]).strip()
            found = _first_email_in_text(txt)
            if found:
                customer_email = found
                break

    # --- Salesperson ---
    salesTeam = opportunity.get("salesTeam") or []
    if not isinstance(salesTeam, list):
        salesTeam = []

    salesPersonObj = None
    for s in salesTeam:
        if not isinstance(s, dict):
            continue
        if str(s.get("isPrimary")).lower() in ("true", "1", "yes"):
            salesPersonObj = s
            break
    if not isinstance(salesPersonObj, dict):
        salesPersonObj = (salesTeam[0] if salesTeam and isinstance(salesTeam[0], dict) else {})

    first_name = (salesPersonObj.get("firstName") or "").strip()
    last_name  = (salesPersonObj.get("lastName") or "").strip()
    full_name  = (f"{first_name} {last_name}").strip()

    salesperson = (
        SALES_PERSON_MAP.get(first_name)
        or SALES_PERSON_MAP.get(full_name)
        or full_name
        or "our team"
    )

    # --- Rooftop ---
    rt = get_rooftop_info(subscription_id) or {}
    rooftop_name   = rt.get("name")   or "Patterson Auto Group"
    rooftop_sender = rt.get("sender") or TEST_FROM

    # --- Vehicle ---
    soughtVehicles = opportunity.get("soughtVehicles") or []
    if not isinstance(soughtVehicles, list):
        soughtVehicles = []

    vehicleObj = None
    for v in soughtVehicles:
        if isinstance(v, dict) and v.get("isPrimary"):
            vehicleObj = v
            break
    if not vehicleObj:
        vehicleObj = (soughtVehicles[0] if soughtVehicles and isinstance(soughtVehicles[0], dict) else {})

    make  = str(vehicleObj.get("make") or "")
    model = str(vehicleObj.get("model") or "")
    year  = str(vehicleObj.get("yearFrom") or vehicleObj.get("year") or "")
    trim  = str(vehicleObj.get("trim") or "")

    vehicle_str = f"{year} {make} {model} {trim}".strip() or "one of our vehicles"

    dealership = rooftop_name
    base_url = DEALERSHIP_URL_MAP.get(dealership)
    if base_url and (make and model):
        vehicle_str = f'<a href="{base_url}?make={make}&model={model}">{vehicle_str}</a>'

    source = opportunity.get("source", "") or (fresh_opp.get("source") or "")
    sub_source = opportunity.get("subSource", "") or (fresh_opp.get("subSource") or "")

    return {
        "customer_name": customer_name,
        "customer_email": customer_email,
        "salesperson": salesperson,
        "rooftop_name": rooftop_name,
        "rooftop_sender": rooftop_sender,
        "vehicle_str": vehicle_str,
        "source": source,
        "sub_source": sub_source,
    }

def send_first_touch_email(
    *,
    opportunity: dict,
    fresh_opp: dict,
    token: str,
    subscription_id: str,
    rooftop_name: str,
    rooftop_sender: str,
    customer_name: str,
    customer_email: str | None,
    source: str,
    vehicle_str: str,
    salesperson: str,
    inquiry_text: str | None,
    created_appt_ok: bool,
    appt_human: str | None,
    currDate,
    currDate_iso: str,
    opportunityId: str,
    OFFLINE_MODE: bool,
    SAFE_MODE: bool = False,
    test_recipient: str | None = None,
) -> bool:
    """
    Returns sent_ok (True only if actually sent or OFFLINE_MODE).
    Mutates opportunity in-place like your existing code.
    """

    if opportunity.get("needs_human_review") is True:
        log.warning(
            "Blocked first-touch send: Needs Human Review checked opp=%s",
            opportunity.get("opportunityId") or opportunity.get("id")
        )
        return False
        
    log.info("send_first_touch_email inputs: opp=%s salesperson=%r customer=%r",
         opportunity.get("opportunityId") or opportunityId,
         salesperson,
         customer_email)

    # --- HARD FIRST-TOUCH IDEMPOTENCY GATE ---
    if opportunity.get("first_email_sent_at"):
        log.info(
            "Skipping first-touch email: already sent at %s opp=%s",
            opportunity.get("first_email_sent_at"),
            opportunity.get("opportunityId") or opportunity.get("id"),
        )
        return False


    # --- DEDUPE: don't send first-touch again if we've already welcomed this email recently (same rooftop)
    if customer_email and _already_sent_first_touch_recently(
        customer_email=customer_email,
        subscription_id=subscription_id,
        current_opp_id=opportunity.get("opportunityId") or opportunityId,
        lookback_hours=72,
    ):
        log.info(
            "Skipping duplicate first-touch for %s (already welcomed recently) opp=%s",
            customer_email, opportunity.get("opportunityId") or opportunityId
        )
        # Optional: still mark record so it doesn't look "untouched"
        opportunity.setdefault("patti", {})
        opportunity["patti"]["skip_first_touch"] = True
        opportunity["patti"]["skip_first_touch_reason"] = "duplicate_lead_same_email_recent_first_touch"
        if not OFFLINE_MODE:
            airtable_save(opportunity)
        return False


    variant = get_or_assign_ab_variant(opportunity)

    VARIANT_LONG = "A_long"
    VARIANT_SHORT = "B_short"
    
    if variant == VARIANT_SHORT:
        subject = f"Quick question about the {vehicle_str} at {rooftop_name}"
        body_html = (
            f"<p>Hi {customer_name},</p>"
            "<p>Thank you for your internet inquiry. I’d love to set up a time for you to come by and visit our showroom - is there a day and time that works best for you?</p>"
        )

    if variant != VARIANT_SHORT:
        # === Compose with GPT ===============================================
        fallback_mode = not inquiry_text or inquiry_text.strip().lower() in ["", "request a quote", "interested", "info", "information", "looking"]
    
        SUBJECT_RULES = f"""
        IMPORTANT — SUBJECT LINE RULES:
        This is the FIRST email in a new conversation thread.
        
        - Do NOT reuse, reference, or paraphrase the inbound lead email subject.
        - Do NOT use words like "lead", "listing"
        
        Write a short, friendly, customer-facing subject line that feels like a human reaching out.
        
        Preferred formats:
        - "Quick question about the {vehicle_str} at {rooftop_name}"
        - "Your interest in the {vehicle_str} at {rooftop_name}"
        - "Hi {customer_name} - your vehicle inquiry at {rooftop_name}"
        
        """
    
    
        if fallback_mode:
            prompt = f"""
        You are Patti, a helpful sales assistant for {rooftop_name}.
        Your job is to write personalized, dealership-branded emails from Patti.
        The guest submitted a lead through {source}. They’re interested in: {vehicle_str}. Salesperson: {salesperson}
        They didn’t leave a detailed message.
    
        Please write a warm, professional email reply that:
        - Begin with exactly `Hi {customer_name},`
        - Immediately acknowledge their inquiry in ONE sentence, like: "Thanks for your inquiry on our {vehicle_str}."
        - Start with 1–2 appealing vehicle features or dealership Why Buys
        - Welcome the guest and highlight our helpfulness
        - Invite specific questions or preferences
        - The goal in your responses is to be helpful but also encourage the person to book an appointment to see the vehicle without sounding salesly or high-pressure
        - Mention the salesperson by name
    
        Do not include any signature, dealership contact block, address, phone number, or URL in your reply; I will append it.
    
        """
        else:
            prompt = f"""
        You are Patti, a helpful sales assistant for {rooftop_name}.
        Your job is to write personalized, dealership-branded emails from Patti.
    
        When writing:
        - Begin with exactly `Hi {customer_name},`
        - Immediately acknowledge their inquiry in ONE sentence, like: "Thanks for your inquiry on our {vehicle_str}."
        - Lead with value (features / Why Buy)
        - If a specific vehicle is mentioned, answer directly and link if possible
        - If a specific question exists, answer it first
        - The goal in your responses is to be helpful but also encourage the person to book an appointment to see the vehicle without sounding salesly or high-pressure
        - Keep it warm, clear, and human
    
        Info (may None):
        - salesperson’s name: {salesperson}
        - vehicle: {vehicle_str}
    
        Guest inquiry:
        \"\"\"{inquiry_text}\"\"\"
    
        Do not include any signature, dealership contact block, address, phone number, or URL in your reply; I will append it.
        """
        prompt += SUBJECT_RULES
        
        # --- NEW: if Patti auto-scheduled an appointment, tell GPT to confirm it ---
        if created_appt_ok and appt_human:
            prompt += f"""
    
    IMPORTANT APPOINTMENT CONTEXT (do not skip):
    - The guest proposed a time and Patti already scheduled a dealership appointment for {appt_human}.
    
    In your email:
    - Clearly confirm that date and time in plain language.
    - Thank them for scheduling.
    - Invite them to reply if they need to adjust the time or have any questions.
    - Do NOT ask them to pick a time; the appointment is already scheduled. Focus on confirming it.
    """
            
        # === Inventory recommendations =====================================
    
        # Get live inventory XML
        # NOTE: when you need to use just uncomment and uncomment in import section also
        # try:
        #     inventory_xml = get_vehicle_inventory_xml("Patterson2", "FjX^PGwk63", "ZE", "ZE7")
        # except Exception as e:
        #     # log.warning(f"❌ Could not retrieve inventory XML: {e}")
        #     inventory_xml = None
    
        # 🔁 Use the same inquiry text you already computed.
        # If it's empty (fallback mode), feed a lightweight hint from the parsed vehicle fields.
        if inquiry_text and inquiry_text.strip():
            customer_email_text = inquiry_text
        else:
            # If inquiry_text is empty, feed the vehicle string as a hint
            import re
    
            plain_vehicle = re.sub(r"<[^>]+>", "", vehicle_str or "").strip()
            customer_email_text = (inquiry_text or "").strip() or plain_vehicle or "SUV car"
        
        recommendation_text = ""
    
        # NOTE: (cont with line: 523)when you need to use just uncomment and uncomment in import section also
        # if inventory_xml:
        #     try:
        #         recommendation_text = recommend_from_xml(inventory_xml, customer_email_text).strip()
        #         if recommendation_text:
        #             prompt += f"\n\nInventory suggestions to include:\n{recommendation_text}\n"
        #             # log.info("✅ Added inventory suggestions to prompt.")
        #     except Exception as e:
        #         pass
        #         # log.warning(f"Recommendation failed: {e}")
            
        response  = run_gpt(prompt, customer_name, rooftop_name)
        subject   = response["subject"]
        body_html = response["body"]
    
    # --- Normalize Patti body ---
    body_html = normalize_patti_body(body_html)
    
    # --- patch the rooftop/address placeholders ---
    body_html = _patch_address_placeholders(body_html, rooftop_name)
    
    # Decide which CTA behavior to use based on appointment state
    patti_meta = opportunity.get("patti") or {}
    mode = (patti_meta.get("mode") or "").strip().lower()
    
    sub_status = (
        (fresh_opp.get("subStatus") or fresh_opp.get("substatus") or "")
        or (opportunity.get("subStatus") or opportunity.get("substatus") or "")
    ).strip().lower()
    
    has_booked_appt = (mode == "scheduled") or ("appointment" in sub_status) or bool(patti_meta.get("appt_due_utc"))
    
    if has_booked_appt:
        body_html = rewrite_sched_cta_for_booked(body_html)
        body_html = _SCHED_ANY_RE.sub("", body_html).strip()
    else:
        if variant != VARIANT_SHORT:
            body_html = append_soft_schedule_sentence(body_html, rooftop_name)
    
    # Strip GPT footer if added
    body_html = _PREFS_RE.sub("", body_html).strip()
    
    # --- add Patti’s signature/footer (same as KBB) ---
    body_html = body_html + build_patti_footer(rooftop_name)
    
    opportunity["body_html"] = body_html
    
    # Append message to opportunity log
    msg_entry = {
        "msgFrom": "patti",
        "subject": subject,
        "body": body_html,
        "date": currDate_iso
    }
    
    if "messages" in opportunity:
        opportunity["messages"].append(msg_entry)
    else:
        opportunity["messages"] = [msg_entry]
    
    # ---------------------------
    #   FIX: Only mark as sent if actual success
    # ---------------------------
    sent_ok = False
    if OFFLINE_MODE:
        sent_ok = True
    
    # Decide recipient (Safe Mode reroutes to test inbox)
    actual_to = customer_email
    if SAFE_MODE:
        actual_to = test_recipient or os.getenv("INTERNET_TEST_EMAIL") or os.getenv("TEST_TO")
        log.warning(
            "SAFE_MODE enabled: rerouting email for opp %s from %r -> %r",
            opportunityId, customer_email, actual_to,
        )
    
    if actual_to and not OFFLINE_MODE:
        try:
            from patti_mailer import send_patti_email
    
            # ✅ Decide cadence anchor BEFORE sending (authoritative)
            patti = opportunity.setdefault("patti", {})
            created_iso = (
                patti.get("salesai_created_iso")
                or opportunity.get("Lead Created At")   # Airtable brain (preferred)
                or opportunity.get("created_at")
                or opportunity.get("dateIn")
                or opportunity.get("createdDate")
                or currDate_iso
            )
    
            idx = int(patti.get("salesai_email_idx") if patti.get("salesai_email_idx") is not None else -1)
            next_due = _next_salesai_due_iso(created_iso=created_iso, last_idx=idx)
    
            sent_ok = send_patti_email(
                token=token,
                subscription_id=subscription_id,
                opp_id=opportunityId,
                rooftop_name=rooftop_name,
                rooftop_sender=rooftop_sender,
                to_addr=actual_to,
                subject=subject,
                body_html=body_html,
                cc_addrs=[],
                force_mode="cadence",
                next_follow_up_at=next_due,
            
                # ✅ new
                template_day=1,          # first-touch
            )

        except Exception as e:
            log.warning("Failed to send Patti general lead email for opp %s: %s", opportunityId, e)
            sent_ok = False
    
    elif not actual_to:
        log.warning(
            "No recipient resolved for opp %s (customer_email=%r SAFE_MODE=%r test_recipient=%r)",
            opportunityId, customer_email, SAFE_MODE, test_recipient
        )
    
    # ---------------------------
    #   Only update Patti's state IF sent_ok is True
    # ---------------------------
    if sent_ok or OFFLINE_MODE:
        checkedDict = opportunity.get("checkedDict") or {}
        checkedDict["patti_already_contacted"] = True
        checkedDict["last_msg_by"] = "patti"
        opportunity["checkedDict"] = checkedDict
    
        # ✅ set first_email_sent_at ONLY AFTER success
        now_iso = _dt.now(_tz.utc).replace(microsecond=0).isoformat()
        opportunity["first_email_sent_at"] = opportunity.get("first_email_sent_at") or now_iso
    
        # cadence enrollment
        patti_meta = opportunity.setdefault("patti", {})
        patti_meta.setdefault("salesai_email_idx", -1)
    
        created_iso = (
            patti_meta.get("salesai_created_iso")
            or opportunity.get("Lead Created At")
            or opportunity.get("created_at")
            or opportunity.get("dateIn")
            or opportunity.get("createdDate")
            or currDate_iso
        )
        patti_meta["salesai_created_iso"] = created_iso
    
        opportunity["followUP_count"] = 0
    
        # ✅ Only persist fields NOT already handled by mark_ai_email_sent() inside send_patti_email
        if not OFFLINE_MODE:
            airtable_save(
                opportunity,
                extra_fields={
                    "ab_variant": variant,
                    "first_email_sent_at": opportunity["first_email_sent_at"],
                    "mode": "cadence",
                }
            )
    
    else:
        log.warning(
            "Did NOT mark Patti as contacted for opp %s because sendEmail failed.",
            opportunityId,
        )
        if not OFFLINE_MODE:
            airtable_save(opportunity)
    
    return bool(sent_ok or OFFLINE_MODE)



def send_thread_reply_now(
    *,
    opportunity: dict,
    fresh_opp: dict,
    token: str | None,
    subscription_id: str,
    trigger: str = "webhook_reply",
    SAFE_MODE: bool = False,
    test_recipient: str | None = None,
    inbound_ts: str | None = None,
    inbound_subject: str | None = None,
) -> tuple[bool, dict]:

    currDate = _dt.now(_tz.utc)
    currDate_iso = currDate.strftime("%Y-%m-%dT%H:%M:%SZ")

    opportunityId = opportunity.get("opportunityId") or opportunity.get("id")
    checkedDict = opportunity.get("checkedDict", {}) or {}

    if opportunity.get("needs_human_review") is True:
        log.warning(
            "Blocked thread reply: Needs Human Review checked opp=%s",
            opportunity.get("opportunityId") or opportunity.get("id")
        )
        state = opportunity.get("_internet_state") or {}
        return False, opportunity


    ctx = _build_email_context(opportunity=opportunity, fresh_opp=fresh_opp, subscription_id=subscription_id, token=token)
    customer_name   = ctx["customer_name"]
    customer_email  = ctx["customer_email"]
    rooftop_name    = ctx["rooftop_name"]
    rooftop_sender  = ctx["rooftop_sender"]
    vehicle_str     = ctx["vehicle_str"]

    messages = opportunity.get("messages") or []

    # --- Step 1: get latest customer message text (for appointment detection) ---
    def _latest_customer_body(msgs: list[dict]) -> str:
        for m in reversed(msgs or []):
            # Outbound is commonly marked msgFrom="patti"
            if (m.get("msgFrom") or "").strip().lower() != "patti":
                return (m.get("body") or m.get("body_text") or m.get("text") or "").strip()
        return ""
    
    customer_body = _latest_customer_body(messages)

    # --- Step 1.5 — TRIAGE before any reply (pricing/OTD/finance/trade/etc.) ---
    try:
        # Detect KBB vs non-KBB (non-KBB always triages per should_triage())
        src = (opportunity.get("source") or "").strip().lower()
        is_kbb = ("kbb" in src) or ("instant cash offer" in src) or ("ico" in src)

        if should_triage(is_kbb=is_kbb) and customer_body:
            triage = classify_inbound_email(customer_body)

            cls = (triage.get("classification") or "").strip().upper()

            if cls == "EXPLICIT_OPTOUT":
                log.info("✅ Triage EXPLICIT_OPTOUT — suppressing and blocking reply opp=%s", opportunityId)

                opportunity.setdefault("patti", {})["skip"] = True
                opportunity.setdefault("patti", {})["skip_reason"] = "explicit_opt_out"
            
                try:
                    mark_unsubscribed(opportunity, reason=triage.get("reason") or "Explicit opt-out")
                except Exception as e:
                    log.warning("mark_unsubscribed failed opp=%s: %s", opportunityId, e)
            
                # Clear follow-up so it doesn't keep showing as due
                opportunity["followUP_date"] = None
                opportunity["isActive"] = False
                p = opportunity.setdefault("patti", {})
                if isinstance(p, dict):
                    p["skip"] = True
                    p["skip_reason"] = "explicit_opt_out"
                    p["opted_out_at"] = inbound_ts or currDate_iso
                
                try:
                    save_opp(opportunity)
                except Exception:
                    pass
                    
                return False, opportunity  # 🚫 stop: no reply, no appt logic, no handoff
            
            if cls == "NON_LEAD":
                log.info("Triage NON_LEAD — ignoring opp=%s", opportunityId)
                return False, opportunity

            if cls == "HUMAN_REVIEW_REQUIRED":
                # Flag in-memory so we block any further replies in this run
                opportunity["needs_human_review"] = True

                # Persist flag in Airtable "brain" (so it sticks)
                try:
                    airtable_save(
                        opportunity,
                        extra_fields={
                            "Needs Human Review": True,
                            "Human Review Reason": triage.get("reason") or "Triage: HUMAN_REVIEW_REQUIRED",
                        },
                    )
                except Exception:
                    pass

                # Fire the escalation email (salesperson + HUMAN_REVIEW_CC), log activity, etc.
                handoff_to_human(
                    opportunity=opportunity,
                    fresh_opp=fresh_opp,
                    token=token,
                    subscription_id=subscription_id,
                    rooftop_name=rooftop_name,
                    inbound_subject=inbound_subject or "",
                    inbound_text=customer_body,
                    inbound_ts=inbound_ts,
                    triage=triage,
                )

                log.info(
                    "✅ Triage triggered HUMAN REVIEW — blocking customer reply opp=%s reason=%s",
                    opportunityId,
                    triage.get("reason"),
                )
                return False, opportunity

    except Exception as e:
        log.warning("Triage gate failed (continuing without triage) opp=%s: %s", opportunityId, e)

    
    # --- Step 2: try to auto-schedule an appointment from this reply (WEBHOOK PATH) ---
    created_appt_ok = False
    appt_human = None
    
    # NOTE: no SAFE_MODE gating here by request (SAFE_MODE can still create Fortellis appts)
    if (not OFFLINE_MODE) and token and subscription_id and opportunityId:
        try:
            patti_meta = opportunity.get("patti") or {}
    
            # Skip if we already know about a future appointment
            appt_due_utc = patti_meta.get("appt_due_utc")
            already_scheduled = False
            if appt_due_utc:
                try:
                    appt_dt = _dt.fromisoformat(str(appt_due_utc).replace("Z", "+00:00"))
                    if appt_dt > _dt.now(_tz.utc):
                        already_scheduled = True
                except Exception:
                    pass
    
            appt_iso = ""
            conf = 0.0
            
            if (not already_scheduled) and customer_body:
                proposed = extract_appt_time(customer_body, tz="America/Los_Angeles")
                appt_iso = (proposed.get("iso") or "").strip()
                conf = float(proposed.get("confidence") or 0.0)

            if appt_iso and conf >= 0.60:
                # appt_iso is expected to be parseable by fromisoformat when Z->+00:00
                dt_local = _dt.fromisoformat(appt_iso.replace("Z", "+00:00"))
    
                due_dt_iso_utc = dt_local.astimezone(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
                appt_human = fmt_local_human(dt_local)

                schedule_appointment_with_notify(
                    token,
                    subscription_id,
                    opportunityId,
                    due_dt_iso_utc=due_dt_iso_utc,
                    activity_name="Sales Appointment",
                    activity_type="Appointment",
                    comments=f"Auto-scheduled from Patti based on customer reply: {customer_body[:200]}",
                    opportunity=opportunity,
                    fresh_opp=fresh_opp if "fresh_opp" in locals() else {},
                    rooftop_name=rooftop_name,
                    appt_human=appt_human,
                    customer_reply=customer_body,
                )
                
                created_appt_ok = True
    
                # Persist appointment state in Airtable “brain”
                patti_meta["mode"] = "scheduled"
                patti_meta["appt_due_utc"] = due_dt_iso_utc
                # We’re about to confirm in the reply, so mark it to prevent duplicates.
                opportunity["patti"] = patti_meta
    
                log.info(
                    "✅ Auto-scheduled appointment from webhook reply for %s at %s (conf=%.2f)",
                    opportunityId,
                    appt_human,
                    conf,
                )
    
        except Exception as e:
            log.warning(
                "Webhook reply appointment detection failed opp=%s: %s",
                opportunityId,
                e,
            )
            
    skip_gpt = bool(created_appt_ok and appt_human)
    
    # --- Step 3: choose the right reply (short confirmation vs normal reply) ---
    skip_footer = False
    response = {}  # <--- IMPORTANT: always defined
    
    if created_appt_ok and appt_human:
        subject = inbound_subject or f"Re: {vehicle_str}"
        body_html = (
            f"<p>Hi {customer_name},</p>"
            f"<p>Perfect — you’re all set for <strong>{appt_human}</strong> at {rooftop_name}.</p>"
            f"<p>If anything changes, just reply here and we’ll adjust.</p>"
        )
        skip_footer = True
    else:
        prompt = f"""
    You are replying to an ACTIVE email thread (not a first welcome message).
    
    Context:
    - The guest originally inquired about: {vehicle_str}
    
    Hard rules:
    - If the guest proposes a visit time (including casual phrasing like "tomorrow around 4"), CONFIRM it.
    - Do NOT ask "what day/time works best?" after they already proposed a time.
    - Do NOT mention store hours unless (a) the guest asks, or (b) the proposed time is outside store hours.
    - Never invent store hours. Use only the store hours provided below.
    - Always include the address in the confirmation sentence.
    
    Store hours (local time):
    Mon: 9 AM–7 PM
    Tue: 9 AM–7 PM
    Wed: 9 AM–7 PM
    Thu: 9 AM–7 PM
    Fri: 9 AM–7 PM
    Sat: 9 AM–8 PM
    Sun: 10 AM–6 PM
    
    Address: 28 B Auto Center Dr, Tustin, CA 92782
    
    messages between Patti and the customer (python list of dicts):
    {messages}
    
    Return ONLY valid JSON with keys: subject, body.
    """.strip()


    
        response = run_gpt(prompt, customer_name, rooftop_name, prevMessages=True)
        subject   = response["subject"]
        body_html = response["body"]

    body_html = normalize_patti_body(body_html)
    body_html = _patch_address_placeholders(body_html, rooftop_name)

    if not skip_footer:
        
        # CTA rules (same as elsewhere)
        patti_meta = opportunity.get("patti") or {}
        mode = (patti_meta.get("mode") or "").strip().lower()
        sub_status = (
            (fresh_opp.get("subStatus") or fresh_opp.get("substatus") or "")
            or (opportunity.get("subStatus") or opportunity.get("substatus") or "")
        ).strip().lower()
        has_booked_appt = (mode == "scheduled") or ("appointment" in sub_status) or bool(patti_meta.get("appt_due_utc"))
    
        if has_booked_appt:
            body_html = rewrite_sched_cta_for_booked(body_html)
            body_html = _SCHED_ANY_RE.sub("", body_html).strip()
        else:
            body_html = body_html.strip()
    
        body_html = _PREFS_RE.sub("", body_html).strip()
        body_html = body_html + build_patti_footer(rooftop_name)

    else:
        # still strip prefs, but keep it short
        body_html = _PREFS_RE.sub("", body_html).strip()

    opportunity.setdefault("messages", []).append({
        "msgFrom": "patti",
        "subject": subject,
        "body": body_html,
        "date": currDate_iso,
        "action": response.get("action"),
        "notes": response.get("notes"),
        "trigger": trigger,
        "inbound_subject": inbound_subject,
        "inbound_ts": inbound_ts,
    })

    to_addr = resolve_customer_email(
        opportunity,
        SAFE_MODE=SAFE_MODE,
        test_recipient=test_recipient,
    )
    
    if not to_addr:
        log.warning(
            "No customer email resolved; blocking send and escalating opp=%s",
            opportunityId,
        )
        opportunity["needs_human_review"] = True
        airtable_save(
            opportunity,
            extra_fields={
                "Needs Human Review": True,
                "Human Review Reason": "Missing customer email for reply",
            },
        )
        return False, opportunity

    
    log.info("📨 Thread reply composed opp=%s subject=%s short_confirm=%s", opportunityId, subject, bool(created_appt_ok and appt_human))
    log.info("📤 Thread reply send attempt opp=%s to_addr=%s SAFE_MODE=%s test_recipient=%s EMAIL_MODE=%s",
             opportunityId, to_addr, SAFE_MODE, test_recipient, os.getenv("EMAIL_MODE", "crm"))
    
    sent_ok = False
    if OFFLINE_MODE:
        sent_ok = True
    else:
        if to_addr:
            try:
                from patti_mailer import send_patti_email
                sent_ok = bool(send_patti_email(
                    token=token,
                    subscription_id=subscription_id,
                    opp_id=opportunityId,
                    rooftop_name=rooftop_name,
                    rooftop_sender=rooftop_sender,
                    to_addr=to_addr,
                    subject=subject,
                    body_html=body_html,
                    cc_addrs=[],
                ))
            except Exception as e:
                log.warning("Thread reply send failed opp %s: %s", opportunityId, e)

    if sent_ok:
        if created_appt_ok and appt_human:
            patti_meta = opportunity.get("patti") or {}
            patti_meta["appt_confirm_email_sent"] = True
            opportunity["patti"] = patti_meta
    
        if not OFFLINE_MODE:
            airtable_save(opportunity)

    return sent_ok, opportunity


# ---- Airtable-driven cadence runner ----
if __name__ == "__main__":
    test_opp_id = (os.getenv("TEST_OPPORTUNITY_ID") or "").strip()

    # -------------------------
    # Test mode: single opp_id
    # -------------------------
    if test_opp_id:
        log.info("TEST_OPPORTUNITY_ID=%s set; running single-opportunity test mode", test_opp_id)

        rec = find_by_opp_id(test_opp_id)
        if not rec:
            log.warning("TEST_OPPORTUNITY_ID %s not found in Airtable; exiting.", test_opp_id)
        else:
            rec_id = rec.get("id")  # Airtable rec id (recXXXX...)
            token = acquire_lock(rec, lock_minutes=10)
            if not token:
                log.warning("Could not acquire lock for test record %s; exiting.", rec_id)
            else:
                try:
                    # IMPORTANT: processHit expects an Airtable record shaped like:
                    # {"id": "...", "fields": {...}}
                    processHit(rec)
                finally:
                    release_lock(rec_id, token)

    # -------------------------
    # Normal mode: hourly cron
    # -------------------------
    else:
        if OFFLINE_MODE:
            log.info("OFFLINE_MODE=true; skipping Airtable cadence run.")
        else:
            records = query_view("Due Now", max_records=200) or []
            log.info("Pulled %d records from Airtable view 'Due Now'", len(records))

            for rec in records:
                rec_id = rec.get("id")
                if not rec_id:
                    log.warning("Skipping Airtable item with no record id: %r", rec)
                    continue

                token = acquire_lock(rec, lock_minutes=10)
                if not token:
                    continue

                try:
                    # IMPORTANT: pass Airtable record into processHit
                    processHit(rec)
                finally:
                    release_lock(rec_id, token)
