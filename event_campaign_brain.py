# event_campaign_brain.py
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict

from openai import OpenAI

log = logging.getLogger("patti.event_campaign_brain")

OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
EVENT_QA_MODEL = (
    os.getenv("EVENT_QA_OPENAI_MODEL")
    or os.getenv("SMS_OPENAI_MODEL")
    or os.getenv("OPENAI_MODEL")
    or "gpt-4o-mini"
).strip()

_oai = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

HANDOFF_REASONS = {
    "pricing",
    "inventory",
    "appointment",
    "event_unknown",
    "complaint",
    "angry",
    "other",
}

PRICING_RE = re.compile(
    r"(?i)\b(price|pricing|msrp|payment|payments|lease|apr|rate|discount|rebate|otd|out the door)\b"
)
INVENTORY_RE = re.compile(
    r"(?i)\b(in stock|inventory|available units|availability|how many|do you have one|colors?|trim|package)\b"
)
APPT_RE = re.compile(
    r"(?i)\b(appointment|appt|schedule|book|reserve a time|test drive|come in|stop by at)\b"
)
ANGRY_RE = re.compile(
    r"(?i)\b(angry|upset|annoyed|frustrated|ridiculous|terrible|complaint|manager)\b"
)

SYSTEM_PROMPT = """You are Patti, a dealership event assistant.

Your job is to answer guest questions about a dealership event using ONLY the provided event context.
If the answer is not explicitly supported by the event context, you must NOT invent it.

Voice:
- warm, professional, concise
- clear and natural
- no emojis
- no hype
- ask at most one question in the reply

Rules:
- Use ONLY facts explicitly given in EVENT CONTEXT.
- If the guest asks something not covered by EVENT CONTEXT, do not guess.
- Instead, politely say you do not want to give the wrong information and that you are looping in a team member.
- Never invent event rules, RSVP rules, food details, parking details, inventory counts, pricing, offers, or special activities.
- Never quote pricing, payments, APR, lease terms, trade values, inventory counts, or vehicle-specific availability unless explicitly given in EVENT CONTEXT.
- If the guest is clearly asking to attend, RSVP, or confirm attendance, answer directly and naturally.
- If the guest asks a factual event question that IS covered by EVENT CONTEXT, answer directly.
- If the guest asks something outside the context, escalate.
- Output ONLY valid JSON.

Return exactly:
{
  "reply": "string",
  "needs_handoff": true,
  "handoff_reason": "pricing|inventory|appointment|event_unknown|complaint|angry|other"
}
or
{
  "reply": "string",
  "needs_handoff": false,
  "handoff_reason": ""
}
"""

def _safe_json(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if not text:
        return {}

    try:
        return json.loads(text)
    except Exception:
        pass

    a = text.find("{")
    b = text.rfind("}")
    if a != -1 and b != -1 and b > a:
        try:
            return json.loads(text[a:b+1])
        except Exception:
            return {}

    return {}

def _fallback_unknown(first_name: str = "") -> Dict[str, Any]:
    prefix = f"{first_name}, " if first_name else ""
    return {
        "reply": (
            f"{prefix}I don’t want to give you the wrong information about the event. "
            "I’m looping in a team member so they can help with that directly."
        ),
        "needs_handoff": True,
        "handoff_reason": "event_unknown",
    }

def _build_event_context(event_fields: dict) -> str:
    def g(key: str) -> str:
        return str(event_fields.get(key) or "").strip()

    benefits = []
    for key in ("Benefit 1", "Benefit 2", "Benefit 3", "Benefit 4"):
        v = g(key)
        if v:
            benefits.append(v)

    lines = [
        f"Event name: {g('Event Name')}",
        f"Store: {g('Store')}",
        f"Brand: {g('Brand')}",
        f"Model year: {g('Model Year')}",
        f"Model: {g('Model')}",
        f"Event date display: {g('Event Date Display')}",
        f"Event date raw: {g('Event Date')}",
        f"Event start time: {g('Event Start Time')}",
        f"Event end time: {g('Event End Time')}",
        f"Event location: {g('Event Location')}",
        f"RSVP URL: {g('RSVP URL') or g('Calendly URL') or g('Landing Page URL')}",
        f"Email subject 1: {g('Email Subject 1')}",
        f"Email subject 2: {g('Email Subject 2')}",
        f"Email subject 3: {g('Email Subject 3')}",
        f"SMS copy 1: {g('SMS Copy 1')}",
        f"SMS copy 2: {g('SMS Copy 2')}",
        f"SMS copy 3: {g('SMS Copy 3')}",
        f"SMS copy 4: {g('SMS Copy 4')}",
    ]

    if benefits:
        lines.append("Benefits:")
        lines.extend([f"- {b}" for b in benefits])

    return "\n".join(lines)

def generate_event_reply(
    *,
    first_name: str,
    event_fields: dict,
    guest_message: str,
    channel: str = "sms",
) -> Dict[str, Any]:
    text = (guest_message or "").strip()
    if not text:
        return _fallback_unknown(first_name)

    # Deterministic guardrails first
    if PRICING_RE.search(text):
        return {
            "reply": (
                f"{first_name + ', ' if first_name else ''}"
                "I’m not able to quote pricing or payment details over this event text thread. "
                "I’m looping in a team member to help with that."
            ),
            "needs_handoff": True,
            "handoff_reason": "pricing",
        }

    if INVENTORY_RE.search(text):
        return {
            "reply": (
                f"{first_name + ', ' if first_name else ''}"
                "I don’t want to guess on exact inventory or availability. "
                "I’m looping in a team member to help with that."
            ),
            "needs_handoff": True,
            "handoff_reason": "inventory",
        }

    if APPT_RE.search(text):
        return {
            "reply": (
                f"{first_name + ', ' if first_name else ''}"
                "I’m happy to help with that. I’m looping in a team member so they can confirm the best time with you."
            ),
            "needs_handoff": True,
            "handoff_reason": "appointment",
        }

    if ANGRY_RE.search(text):
        return {
            "reply": (
                f"{first_name + ', ' if first_name else ''}"
                "I’m sorry about that. I’m looping in a team member now so they can help directly."
            ),
            "needs_handoff": True,
            "handoff_reason": "complaint",
        }

    if not _oai:
        return _fallback_unknown(first_name)

    event_context = _build_event_context(event_fields)

    user_prompt = f"""
CHANNEL: {channel}

EVENT CONTEXT:
{event_context}

GUEST MESSAGE:
{text}
""".strip()

    try:
        resp = _oai.chat.completions.create(
            model=EVENT_QA_MODEL,
            response_format={"type": "json_object"},
            temperature=0.2,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
        )
        raw = (resp.choices[0].message.content or "").strip()
        out = _safe_json(raw)
    except Exception:
        log.exception("Event GPT reply generation failed")
        return _fallback_unknown(first_name)

    reply = (out.get("reply") or "").strip()
    needs_handoff = bool(out.get("needs_handoff"))
    reason = (out.get("handoff_reason") or "").strip().lower()

    if not reply:
        return _fallback_unknown(first_name)

    if needs_handoff and reason not in HANDOFF_REASONS:
        reason = "event_unknown"

    return {
        "reply": reply,
        "needs_handoff": needs_handoff,
        "handoff_reason": reason if needs_handoff else "",
    }
