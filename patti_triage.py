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

def
