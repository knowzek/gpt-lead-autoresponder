# sms_brain.py
"""
Impel-style SMS brain (prompt + reply helper).

Goal (v1): generate *one* short, helpful SMS reply that feels like Impel/Sierra:
- professional, warm, direct
- always steers toward an appointment (or a quick call first)
- avoids long paragraphs, emojis, hype
- no pricing/OTD quotes (handoff elsewhere)
- opt-out footer only on outbound "nudge"/"first touch" (caller decides)
"""

from __future__ import annotations

import os
import json
import logging
from typing import Any, Dict, List, Optional

from openai import OpenAI

log = logging.getLogger("patti.sms_brain")

OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
SMS_MODEL = (os.getenv("SMS_OPENAI_MODEL") or os.getenv("OPENAI_MODEL") or "gpt-4o-mini").strip()

_oai = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


SYSTEM_PROMPT = """You are Patti, an AI online relations assistant for a car dealership.

Voice / vibe (match Impel "Sierra" examples):
- Friendly, professional, confident.
- Short, clean sentences.
- No emojis. No slang. No exclamation spamming.
- Sound human and helpful, not robotic.

Primary goal:
- Move the customer to the next step: schedule an appointment OR offer a quick call first.

Hard rules:
- Never quote pricing, OTD, payments, APR, lease terms, discounts, trade values, incentives. If the customer asks, respond with a brief "team is checking" message and mark handoff.
- If the customer indicates they already bought, not interested, or wants to stop, be polite and stop.
- If the customer says STOP/UNSUBSCRIBE/END/QUIT, confirm opt-out.
- Do not ask more than ONE question in a single SMS.
- Keep replies under ~320 characters unless asked a complex question.

Output format:
Return ONLY valid JSON with keys:
{
  "reply": string,
  "intent": "reply"|"handoff"|"opt_out"|"close",
  "needs_handoff": boolean,
  "handoff_reason": ""|"pricing"|"policy"|"other",
  "include_optout_footer": boolean
}
"""


def _safe_json_loads(s: str) -> Dict[str, Any]:
    try:
        return json.loads(s)
    except Exception:
        return {}


def build_user_prompt(
    *,
    rooftop_name: str,
    customer_first_name: str,
    customer_phone: str,
    salesperson: str,
    vehicle: str,
    last_inbound: str,
    thread_snippet: Optional[List[Dict[str, str]]] = None,
    include_optout_footer: bool = False,
) -> str:
    rooftop_name = rooftop_name or "our dealership"
    customer_first_name = customer_first_name or "there"
    salesperson = salesperson or "our team"
    vehicle = vehicle or "the vehicle you asked about"
    last_inbound = (last_inbound or "").strip()

    recent = ""
    if thread_snippet:
        lines = []
        for m in thread_snippet[-6:]:
            frm = (m.get("from") or "").strip().lower() or "unknown"
            txt = (m.get("text") or "").strip()
            if not txt:
                continue
            if len(txt) > 220:
                txt = txt[:220] + "…"
            lines.append(f"{frm}: {txt}")
        if lines:
            recent = "\n\nRecent thread:\n" + "\n".join(lines)

    return (
        f"Context:\n"
        f"- Rooftop: {rooftop_name}\n"
        f"- Customer first name: {customer_first_name}\n"
        f"- Customer phone: {customer_phone}\n"
        f"- Assigned rep (human): {salesperson}\n"
        f"- Vehicle: {vehicle}\n"
        f"- include_optout_footer: {include_optout_footer}\n"
        f"\n"
        f"Customer inbound SMS:\n{last_inbound}\n"
        f"{recent}\n"
        f"\n"
        f"Write Patti's next SMS."
    )


def generate_sms_reply(
    *,
    rooftop_name: str,
    customer_first_name: str,
    customer_phone: str,
    salesperson: str,
    vehicle: str,
    last_inbound: str,
    thread_snippet: Optional[List[Dict[str, str]]] = None,
    include_optout_footer: bool = False,
) -> Dict[str, Any]:
    if not _oai:
        return {
            "reply": "Thanks — what day/time works best for you to come in?",
            "intent": "reply",
            "needs_handoff": False,
            "handoff_reason": "",
            "include_optout_footer": bool(include_optout_footer),
        }

    user_prompt = build_user_prompt(
        rooftop_name=rooftop_name,
        customer_first_name=customer_first_name,
        customer_phone=customer_phone,
        salesperson=salesperson,
        vehicle=vehicle,
        last_inbound=last_inbound,
        thread_snippet=thread_snippet,
        include_optout_footer=include_optout_footer,
    )

    try:
        resp = _oai.chat.completions.create(
            model=SMS_MODEL,
            temperature=0.3,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
        )
        content = (resp.choices[0].message.content or "").strip()
        data = _safe_json_loads(content)
    except Exception as e:
        log.warning("sms_brain OpenAI call failed: %r", e)
        data = {}

    reply = (data.get("reply") or "").strip()
    intent = (data.get("intent") or "reply").strip()
    needs_handoff = bool(data.get("needs_handoff"))
    handoff_reason = (data.get("handoff_reason") or "").strip()
    footer = bool(data.get("include_optout_footer"))

    if not reply:
        reply = "Thanks — what day/time works best for you to come in?"
    if intent not in ("reply", "handoff", "opt_out", "close"):
        intent = "reply"

    return {
        "reply": reply,
        "intent": intent,
        "needs_handoff": needs_handoff,
        "handoff_reason": handoff_reason,
        "include_optout_footer": footer,
    }
