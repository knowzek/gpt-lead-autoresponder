# lead_router.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
import re


@dataclass(frozen=True)
class LeadRule:
    name: str
    from_contains: Tuple[str, ...] = ()
    from_equals: Tuple[str, ...] = ()  # exact match, case-insensitive
    subject_contains: Tuple[str, ...] = ()
    subject_equals: Tuple[str, ...] = ()  # exact match, case-insensitive


def _s(v: Any) -> str:
    return (v or "").strip()


def _email_addr_from_outlook_obj(v: Any) -> str:
    """
    PA sometimes sends `from` as an object.
    Try to normalize to just the address string.
    """
    if isinstance(v, dict):
        # typical O365 trigger format:
        # {"emailAddress": {"name": "...", "address": "foo@bar.com"}, ...}
        ea = v.get("emailAddress") or {}
        if isinstance(ea, dict):
            return _s(ea.get("address"))
        # some variants:
        return _s(v.get("address")) or _s(v.get("email")) or _s(v.get("value"))
    return _s(v)


def _ci_eq(a: str, b: str) -> bool:
    return (a or "").strip().lower() == (b or "").strip().lower()


def _ci_contains(hay: str, needles: Tuple[str, ...]) -> bool:
    if not hay or not needles:
        return False
    h = hay.lower()
    return any((n or "").lower() in h for n in needles if n)


def match_rule(rule: LeadRule, inbound: Dict[str, Any]) -> bool:
    frm = _email_addr_from_outlook_obj(inbound.get("from")).lower()
    subj = _s(inbound.get("subject")).lower()

    # exact match groups
    if rule.from_equals and any(_ci_eq(frm, x) for x in rule.from_equals):
        return True
    if rule.subject_equals and any(_ci_eq(subj, x) for x in rule.subject_equals):
        return True

    # contains groups
    if rule.from_contains and _ci_contains(frm, rule.from_contains):
        return True
    if rule.subject_contains and _ci_contains(subj, tuple(s.lower() for s in rule.subject_contains)):
        return True

    return False


# ------------------------------------------------------------
# RULES based on your screenshots
# ORDER MATTERS: more specific → more general
# ------------------------------------------------------------
LEAD_SOURCE_RULES: List[LeadRule] = [
    # Carfax (From equals NoReplyLead@carfax.com)
    LeadRule(
        name="carfax",
        from_equals=("NoReplyLead@carfax.com",),
    ),

    # Cars.com (Subject contains New/Used lead)
    LeadRule(
        name="cars.com",
        subject_contains=(
            "Cars.com New Car Lead",
            "Cars.com Used Car Lead",
        ),
    ),

    # Apollo Special Leads (Pre-Qual / Value Your Trade)
    LeadRule(
        name="apollo_special",
        subject_contains=(
            "Apollo Website Lead-Pre-Qual VDP",
            "Apollo Website Lead-Trade - Value Your Trade",
        ),
    ),

    # Apollo (general)
    LeadRule(
        name="apollo",
        subject_contains=(
            "Apollo Website Lead",
        ),
    ),

    # AutoTrader
    LeadRule(
        name="autotrader",
        from_contains=("autotrader.com",),
        subject_contains=(
            "Lead: Autotrader Vehicle:",
        ),
    ),

    # CarGurus (From contains dealer-leads@messages.cargurus.com)
    LeadRule(
        name="cargurus",
        from_contains=("dealer-leads@messages.cargurus.com",),
    ),

    # CarNOW / TrueCar (From contains these)
    LeadRule(
        name="carnow_or_truecar",
        from_contains=("adf-no-reply@carnow.com", "truecarmail.com"),
    ),
]


def detect_lead_source(inbound: Dict[str, Any]) -> Optional[str]:
    subj = _s(inbound.get("subject")).lower()

    # Explicit exclusions first
    if (
        "apollo website lead-schedule a service" in subj
        or "chat transcript" in subj
    ):
        return None

    for rule in LEAD_SOURCE_RULES:
        if match_rule(rule, inbound):
            if rule.name == "carnow_or_truecar":
                frm = _email_addr_from_outlook_obj(inbound.get("from")).lower()
                if "truecarmail.com" in frm:
                    return "truecar"
                return "carnow"

            if rule.name == "apollo_special":
                return "Team Velocity - Pre-Qualification"

            return rule.name

    return None


def detect_lead_type(inbound: Dict[str, Any]) -> str:
    """
    Only used for Apollo Special Leads in your screenshots.
    """
    subj = _s(inbound.get("subject")).lower()
    if "apollo website lead-pre-qual vdp" in subj:
        return "pre_qual"
    if "apollo website lead-trade - value your trade" in subj:
        return "value_your_trade"
    return ""
