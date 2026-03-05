# lead_router.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional, Dict, Any, List, Tuple
import re


@dataclass(frozen=True)
class LeadRule:
    """
    A single lead-source rule. You will populate these from your PA conditions.
    Matching is OR across fields, unless you add custom logic later.
    """
    name: str

    # simple "contains" OR lists (case-insensitive)
    from_contains: Tuple[str, ...] = ()
    subject_contains: Tuple[str, ...] = ()
    body_contains: Tuple[str, ...] = ()

    # optional regex matches (case-insensitive)
    from_regex: Tuple[str, ...] = ()
    subject_regex: Tuple[str, ...] = ()
    body_regex: Tuple[str, ...] = ()

    # optional: if set, require at least one match in THIS field group
    # (keeps rules from being too loose)
    require_any: Tuple[str, ...] = ()  # e.g. ("from",) or ("subject", "from")


def _ci_contains(hay: str, needles: Tuple[str, ...]) -> bool:
    if not hay or not needles:
        return False
    h = hay.lower()
    return any((n or "").lower() in h for n in needles if n)


def _ci_regex(hay: str, patterns: Tuple[str, ...]) -> bool:
    if not hay or not patterns:
        return False
    for p in patterns:
        if not p:
            continue
        if re.search(p, hay, flags=re.IGNORECASE):
            return True
    return False


def match_rule(rule: LeadRule, inbound: Dict[str, Any]) -> bool:
    frm = (inbound.get("from") or "").strip()
    subj = (inbound.get("subject") or "").strip()
    body = (inbound.get("body_text") or "").strip() or (inbound.get("body_html") or "").strip()

    hits = {
        "from": _ci_contains(frm, rule.from_contains) or _ci_regex(frm, rule.from_regex),
        "subject": _ci_contains(subj, rule.subject_contains) or _ci_regex(subj, rule.subject_regex),
        "body": _ci_contains(body, rule.body_contains) or _ci_regex(body, rule.body_regex),
    }

    # If rule specifies require_any, enforce it
    if rule.require_any:
        if not any(hits.get(k, False) for k in rule.require_any):
            return False

    # Default: any hit qualifies
    return any(hits.values())


# -------------------------------------------------------------------
# TODO: You will paste your PA rules next, and we will fill this list.
# -------------------------------------------------------------------
LEAD_SOURCE_RULES: List[LeadRule] = [
    # Example (replace later):
    # LeadRule(
    #     name="carnow",
    #     from_contains=("adf-no-reply@carnow.com", "truecarmail.com"),
    #     require_any=("from",),
    # ),
]


def detect_lead_source(inbound: Dict[str, Any]) -> Optional[str]:
    """
    Returns the rule name of the first matching lead source, else None.
    Order matters — keep more specific rules first.
    """
    for rule in LEAD_SOURCE_RULES:
        if match_rule(rule, inbound):
            return rule.name
    return None
