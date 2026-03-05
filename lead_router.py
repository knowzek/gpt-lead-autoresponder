# lead_router.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
import re


@dataclass(frozen=True)
class LeadRule:
    """
    One vendor/source rule. We’ll fill these from your PA conditions next.
    Order matters: keep most specific rules first.
    """
    name: str
    from_contains: Tuple[str, ...] = ()
    subject_contains: Tuple[str, ...] = ()
    body_contains: Tuple[str, ...] = ()

    from_regex: Tuple[str, ...] = ()
    subject_regex: Tuple[str, ...] = ()
    body_regex: Tuple[str, ...] = ()

    # If set, at least one match must occur in one of these buckets:
    # values allowed: "from", "subject", "body"
    require_any: Tuple[str, ...] = ()


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
    # prefer body_text if you have it; fall back to html
    body = (inbound.get("body_text") or "").strip() or (inbound.get("body_html") or "").strip()

    hits = {
        "from": _ci_contains(frm, rule.from_contains) or _ci_regex(frm, rule.from_regex),
        "subject": _ci_contains(subj, rule.subject_contains) or _ci_regex(subj, rule.subject_regex),
        "body": _ci_contains(body, rule.body_contains) or _ci_regex(body, rule.body_regex),
    }

    if rule.require_any:
        if not any(hits.get(k, False) for k in rule.require_any):
            return False

    return any(hits.values())


# --------------------------------------------------------------------
# We will fill this from your PA rules next.
# --------------------------------------------------------------------
LEAD_SOURCE_RULES: List[LeadRule] = [
    # Example (we’ll replace with your real rules):
    # LeadRule(
    #     name="carnow",
    #     from_contains=("adf-no-reply@carnow.com", "truecarmail.com"),
    #     require_any=("from",),
    # ),
]


def detect_lead_source(inbound: Dict[str, Any]) -> Optional[str]:
    for rule in LEAD_SOURCE_RULES:
        if match_rule(rule, inbound):
            return rule.name
    return None
