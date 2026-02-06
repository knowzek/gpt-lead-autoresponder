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

STORE_HOURS = (os.getenv("STORE_HOURS_TEXT") or """Friday 9 AM–7 PM
Saturday 9 AM–8 PM
Sunday 10 AM–6 PM
Monday 9 AM–7 PM
Tuesday 9 AM–7 PM
Wednesday 9 AM–7 PM
Thursday 9 AM–7 PM""").strip()


_oai = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

SYSTEM_PROMPT = """You are Patti, an AI online relations assistant for a car dealership.

Voice / vibe (match Impel "Sierra" examples):
- Friendly, professional, confident.
- Short, clean sentences.
- No emojis. No slang. No exclamation spamming.
- Sound human and helpful, not robotic.

Primary goal:
- Answer the customer’s question clearly and briefly.
- Then (if appropriate) suggest a next step: schedule an appointment OR offer a quick call.

Hard rules:
- Never quote pricing, OTD, payments, APR, lease terms, discounts, trade values, incentives.
- If the customer indicates they already bought, not interested, wrong number, or wants to stop, be polite and stop.
- If the customer says STOP/UNSUBSCRIBE/END/QUIT, confirm opt-out.
- Do not ask more than ONE question in a single SMS.
- Only use intent="close" if the customer clearly ends the conversation (e.g., not interested, bought elsewhere, wrong number).
- If you asked a question and the customer answered it, acknowledge it and ask the next single best question OR tell them what happens next.
- If they say "no thanks" after you offered an appointment/call, treat it as declining that option and continue helping.
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

STOP_TOKENS = ("stop", "unsubscribe", "end", "quit")
PRICING_TOKENS = (
    "otd", "out the door", "out-the-door",
    "price", "pricing", "best price",
    "payment", "monthly", "per month",
    "lease", "apr", "interest"
)


def _contains_any(text: str, tokens: tuple[str, ...]) -> bool:
    t = (text or "").lower()
    return any(tok in t for tok in tokens)



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
        for m in thread_snippet[-10:]:
            role = (m.get("role") or "").strip().lower()
            txt = (m.get("content") or "").strip()
            if not txt:
                continue
            if len(txt) > 260:
                txt = txt[:260] + "…"
            who = "Customer" if role == "user" else "Patti"
            lines.append(f"{who}: {txt}")
        if lines:
            recent = "\n\nConversation so far (most recent last):\n" + "\n".join(lines)


    return (
        f"Context:\n"
        f"- Rooftop: {rooftop_name}\n"
        f"- Customer first name: {customer_first_name}\n"
        f"- Customer phone: {customer_phone}\n"
        f"- Assigned rep (human): {salesperson}\n"
        f"- Store hours:\n{STORE_HOURS}\n"
        f"- Vehicle: {vehicle}\n"
        f"- include_optout_footer: {include_optout_footer}\n"
        f"\n"
        f"Customer inbound SMS:\n{last_inbound}\n"
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

      # ✅ Tiny deterministic gates for compliance
    log.info("sms_brain VERSION=2026-01-30-OTD-GATE-A")

    inbound = (last_inbound or "").strip()

    if _contains_any(inbound, STOP_TOKENS):
        log.info("sms_brain GATE=stop inbound=%r", inbound[:120])
        return {
            "reply": "You’re all set — we’ll stop texting you. Reply START if you change your mind.",
            "intent": "opt_out",
            "needs_handoff": False,
            "handoff_reason": "",
            "include_optout_footer": False,
        }

    txt = inbound.lower()
    looks_like_ask = ("?" in txt) or any(x in txt for x in ("how much", "what", "best", "price", "otd", "out the door"))
    if looks_like_ask and _contains_any(inbound, PRICING_TOKENS):
        # Pricing/OTD → always handoff; never let the model decide this
        log.info("sms_brain GATE=pricing inbound=%r", inbound[:120])

        return {
            "reply": "Totally — our team is checking the out-the-door numbers now. Are you paying cash or financing?",
            "intent": "handoff",
            "needs_handoff": True,
            "handoff_reason": "pricing",
            "include_optout_footer": False,
        }

    user_prompt = build_user_prompt(
        rooftop_name=rooftop_name,
        customer_first_name=customer_first_name,
        customer_phone=customer_phone,
        salesperson=salesperson,
        vehicle=vehicle,
        last_inbound=last_inbound,
        thread_snippet=thread_snippet,  # ok to keep; optional
        include_optout_footer=include_optout_footer,
    )

    # ✅ Build chat messages properly (system -> thread -> final instruction)
    messages: List[Dict[str, str]] = [{"role": "system", "content": SYSTEM_PROMPT}]

    # If we have thread context, pass it as actual conversation turns
    if thread_snippet:
        for m in thread_snippet[-12:]:
            role = (m.get("role") or "").strip().lower()
            content = (m.get("content") or "").strip()
            if not content:
                continue
            if role not in ("user", "assistant"):
                continue
            messages.append({"role": role, "content": content})

    # Then add the final instruction as the last user message
    messages.append({"role": "user", "content": user_prompt})

    try:
        resp = _oai.chat.completions.create(
            model=SMS_MODEL,
            temperature=0.3,
            messages=messages,
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

    log.info("SMS brain decision intent=%s handoff=%s reply=%r", intent, needs_handoff, reply[:160])

    return {
        "reply": reply,
        "intent": intent,
        "needs_handoff": needs_handoff,
        "handoff_reason": handoff_reason,
        "include_optout_footer": footer,
    }
