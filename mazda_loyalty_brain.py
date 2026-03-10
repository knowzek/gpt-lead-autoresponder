# mazda_loyalty_brain.py
"""
Mazda Loyalty (CX-5) inbound reply brain — EMAIL style ("B"):

- concise, structured, professional
- pushes toward next step: verify voucher, inventory, test drive
- NEVER quotes pricing/OTD/payments/lease/finance
- escalates to human on pricing, trade, finance, angry/complaint, complex asks
- returns JSON-like dict with reply_text/reply_html + handoff flags
"""

from __future__ import annotations

import os
import re
import json
import logging
from typing import Any, Dict, Optional

from openai import OpenAI

log = logging.getLogger("patti.mazda_loyalty_brain")

OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
EMAIL_MODEL = (os.getenv("EMAIL_OPENAI_MODEL") or os.getenv("OPENAI_MODEL") or "gpt-4o-mini").strip()

_oai = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

STOP_TOKENS = ("stop", "unsubscribe", "end", "quit", "do not contact", "dont contact")
PRICING_TOKENS = (
    "otd", "out the door", "out-the-door", "price", "pricing", "best price",
    "payment", "monthly", "per month", "lease", "apr", "interest", "rate",
    "trade", "trade-in", "trade in", "value my trade", "down payment", "down"
)

# “I can’t find / didn’t receive my code” intent
CODE_NOT_FOUND_TOKENS = [
    "didn't receive", "didnt receive", "did not receive", "never received",
    "didn't get", "didnt get", "did not get", "never got",
    "can't find", "cant find", "cannot find", "can't locate", "cant locate",
    "can't see", "cant see", "missing", "lost",
    "where is my code", "where's my code", "wheres my code",
    "no code", "not received", "haven't received", "havent received",
    "can't find my voucher", "cant find my voucher",
    "can't find my code", "cant find my code",
    "didn't get my voucher", "didnt get my voucher",
]

_BUYING_INTENT_RE = re.compile(
    r"""(?ix)
    \b(
        i\s+want\s+to\s+use\s+it|
        i\s+want\s+to\s+use\s+the\s+(?:reward|voucher|loyalty\s+reward)|
        want\s+to\s+redeem|
        redeem\s+it|
        use\s+my\s+(?:reward|voucher|code)|
        use\s+the\s+(?:reward|voucher|loyalty\s+reward)|
        apply\s+the\s+(?:reward|voucher)|
        i'd\s+like\s+to\s+use\s+it|
        i\s+will\s+like\s+to\s+redeem|
        i\s+would\s+like\s+to\s+redeem
    )\b
    """
)

_TRANSFER_INTENT_RE = re.compile(
    r"""(?ix)
    \b(
        transfer|
        gift\s+it|
        give\s+it\s+to|
        family\s+member|
        friend
    )\b
    """
)

_SERVICE_CREDIT_INTENT_RE = re.compile(
    r"""(?ix)
    \b(
        service\s+credit|
        service\s+and\s+parts\s+credit|
        parts\s+credit|
        redeem\s+it\s+for\s+\$?100
    )\b
    """
)

def _looks_like_buying_intent(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return False
    if _BUYING_INTENT_RE.search(t):
        return True

    # light heuristic: mentions using/redeeming voucher/reward without transfer/service wording
    if (
        any(x in t for x in ("use", "redeem", "apply"))
        and any(x in t for x in ("reward", "voucher", "code", "loyalty"))
        and not _TRANSFER_INTENT_RE.search(t)
        and not _SERVICE_CREDIT_INTENT_RE.search(t)
    ):
        return True

    return False

def _looks_like_transfer_intent(text: str) -> bool:
    return bool(_TRANSFER_INTENT_RE.search(text or ""))

def _looks_like_service_credit_intent(text: str) -> bool:
    return bool(_SERVICE_CREDIT_INTENT_RE.search(text or ""))

def _looks_like_code_not_found(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return False

    if _contains_any(t, CODE_NOT_FOUND_TOKENS):
        return True

    # Extra light heuristic: mentions "code/voucher" + a negative
    if ("code" in t or "voucher" in t) and any(x in t for x in ("can't", "cant", "didn't", "didnt", "not", "never", "missing", "lost")):
        return True

    return False
# 16-digit voucher code (allow spaces/dashes)
VOUCHER_RE = re.compile(r"\b(\d[ -]?){15}\d\b")

HANDOFF_REASONS = {
    "pricing",
    "trade",
    "finance",
    "angry",
    "complaint",
    "voucher_lookup",
    "other",
}

SYSTEM_PROMPT = """You are Patti, a virtual assistant for a Mazda dealership.

This email thread is about the Mazda Loyalty CX-5 Reward program.

Voice:
- professional, concise, human, no hype, no emojis
- short paragraphs
- ask at most ONE direct question per email

Program context:
- Customers received a Mazda Loyalty Voucher related to their Mazda CX-5.
- The voucher can typically be used toward a qualifying vehicle purchase.
- If the customer does not want or need a vehicle right now, there are still two helpful options:
  1. They may transfer the voucher to a family member or friend.
  2. They may redeem it for a $100 Service & Parts credit at {rooftop_name} in exchange for the loyalty code.
- When referencing the service credit, describe it as a "$100 Service & Parts credit at {rooftop_name}."

Program guardrails:
- Do NOT invent program rules, deadlines, eligibility criteria, or availability.
- If the customer provides a voucher code, acknowledge and say you will verify it and follow up shortly.
- If they haven’t received the voucher, offer to help track it down and ask for the best email/phone if missing.
- If they want to transfer/gift it, confirm that you can help and ask for the recipient name and best contact information.
- If they show buying intent, move to the next step (inventory list or test drive).
- Never quote pricing, OTD, payments, APR, lease terms, or trade values. Escalate those to a human.
- If the customer says they didn’t receive or can’t find their voucher code, tell them the code should have been emailed from Mazda (mazdaemail@dealers-mazdausa.com) and to search Inbox, Spam, Promotions, and All Mail/Archive.

Special handling for customers who already purchased or are not in the market:
If the customer message includes statements like:

- "I already bought a car"
- "Just purchased a car"
- "I'm not in the market"
- "I don't need a car"
- "Already replaced it"
- "Not interested in buying right now"
- "We already bought something"

or similar language indicating they already purchased or are not shopping:

Do NOT continue sales messaging.

Instead:
1. Acknowledge their situation naturally.
2. If they mention buying a vehicle, briefly congratulate them.
3. Explain that the voucher can be transferred to a family member or friend.
4. Mention that they can also redeem it for a $100 Service & Parts credit at {rooftop_name} in exchange for the loyalty code.
5. Offer to help with either transfer or redemption.

Tone guidance for this scenario:
- Be helpful, not salesy.
- Do not push inventory or shopping steps unless the customer reopens that topic.
- Keep the message short and friendly.

Output format:
Return ONLY valid JSON:
{{
  "reply_text": string,
  "reply_html": string,
  "needs_handoff": boolean,
  "handoff_reason": "pricing"|"trade"|"finance"|"angry"|"complaint"|"other"
}}
"""

def _contains_any(text: str, tokens: tuple[str, ...]) -> bool:
    t = (text or "").lower()
    return any(tok in t for tok in tokens)

def _extract_voucher_code(text: str) -> str:
    m = VOUCHER_RE.search(text or "")
    if not m:
        return ""
    raw = m.group(0)
    digits = "".join(ch for ch in raw if ch.isdigit())
    return digits if len(digits) == 16 else ""

def _safe_json(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        # attempt to pull the first {...}
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(text[start:end+1])
            except Exception:
                return {}
        return {}

def _as_html(text: str) -> str:
    # simple safe HTML: paragraphs on blank lines, <br> on single newlines
    chunks = [c.strip() for c in (text or "").split("\n\n") if c.strip()]
    if not chunks:
        return ""
    parts = []
    for c in chunks:
        parts.append(f"<p style='margin:0 0 12px 0;'>{c.replace(chr(10), '<br>')}</p>")
    return "".join(parts)

_PURCHASE_VERBS_RE = re.compile(
    r"\b(purchased|bought|leased|lease(d)?|picked up|took delivery|delivered|closed|signed)\b",
    re.I
)

_TIME_HINT_RE = re.compile(
    r"\b(this month|last month|this week|last week|today|yesterday|recently|just|already|earlier|few days ago)\b",
    re.I
)

_MODEL_YEAR_CONTEXT_RE = re.compile(
    r"\b(20\d{2})\b",  # model years like 2025 / 2026
    re.I
)

def _looks_like_recent_purchase(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False

    # Strong “purchase already happened” indicator
    if _PURCHASE_VERBS_RE.search(t) and (_TIME_HINT_RE.search(t) or "we purchased" in t.lower()):
        return True

    # Often they mention model year + purchase verb without explicit time
    if _PURCHASE_VERBS_RE.search(t) and _MODEL_YEAR_CONTEXT_RE.search(t):
        return True

    return False

_NOT_IN_MARKET_RE = re.compile(
    r"\b("
    r"not in the market|"
    r"don't need a car|dont need a car|"
    r"not interested right now|"
    r"not interested in buying right now|"
    r"already replaced it|"
    r"already replaced the vehicle|"
    r"we already bought something|"
    r"don't need another car|dont need another car"
    r")\b",
    re.I,
)

def _looks_like_not_in_market(text: str) -> bool:
    return bool(_NOT_IN_MARKET_RE.search(text or ""))

def generate_mazda_loyalty_email_reply(
    *,
    first_name: str,
    bucket: str,
    rooftop_name: str,
    last_inbound: str,
) -> Dict[str, Any]:
    """
    Returns:
      {
        "reply_text": str,
        "reply_html": str,
        "needs_handoff": bool,
        "handoff_reason": str
      }
    """

    inbound = (last_inbound or "").strip()
    first_name = (first_name or "").strip()
    rooftop_name = (rooftop_name or "").strip()
    bucket = (bucket or "").strip()

    # ---- deterministic pre-guards ----
    if _contains_any(inbound, STOP_TOKENS):
        txt = "Understood, we’ll stop reaching out. If you need anything in the future, just reply here."
        return {
            "reply_text": txt,
            "reply_html": _as_html(txt),
            "needs_handoff": False,
            "handoff_reason": "other",
        }

    if _contains_any(inbound, PRICING_TOKENS):
        txt = (
            f"{'Hi ' + first_name + ',' if first_name else 'Hi there,'}\n\n"
            "Thanks — I can help with that. I’m looping in a team member to get you the most accurate details.\n\n"
            "What’s the best number to reach you if a quick call is easier?"
        ).strip()
        return {
            "reply_text": txt,
            "reply_html": _as_html(txt),
            "needs_handoff": True,
            "handoff_reason": "pricing",
        }
        
    # ---- Already bought / not in market ----
    if _looks_like_recent_purchase(inbound) or _looks_like_not_in_market(inbound):
        opening = (
            "Congratulations on your new vehicle."
            if _looks_like_recent_purchase(inbound)
            else "Totally understand."
        )
        txt = (
            f"{'Hi ' + first_name + ',' if first_name else 'Hi there,'}\n\n"
            f"{opening}\n\n"
            f"If you do not need the voucher for yourself, it may still be transferred to a family member or friend. "
            f"You may also be able to redeem it for a $100 Service & Parts credit at {rooftop_name or 'Patterson Autos Mazda dealership'} "
            "in exchange for the loyalty code.\n\n"
            "If you’d like, I can help with either option."
        ).strip()
        return {
            "reply_text": txt,
            "reply_html": _as_html(txt),
            "needs_handoff": False,
            "handoff_reason": "other",
        }

    # ---- “can't find / didn't receive my code” ----
    if _looks_like_code_not_found(inbound):
        txt = (
            f"{'Hi ' + first_name + ',' if first_name else 'Hi there,'}\n\n"
            "No problem — the loyalty voucher code is typically emailed directly from Mazda.\n\n"
            "Please search your inbox for an email from:\n"
            "mazdaemail@dealers-mazdausa.com\n\n"
            "Also check Spam, Promotions, and All Mail/Archive (it can land there).\n\n"
            "If you still can’t find it after searching those folders, tell me and I’ll loop in a team member to help you get it sorted."
        ).strip()
        return {
            "reply_text": txt,
            "reply_html": _as_html(txt),
            "needs_handoff": False,   # keep it informational unless you want to escalate immediately
            "handoff_reason": "other",
        }

    # ---- Clear buying intent: do NOT push transfer/service credit ----
    if _looks_like_buying_intent(inbound):
        txt = (
            f"{'Hi ' + first_name + ',' if first_name else 'Hi there,'}\n\n"
            "I can help with that.\n\n"
            "If you already have your 16-digit voucher code, send it here. "
            "If not, I can help you figure out where to find it."
        ).strip()
        return {
            "reply_text": txt,
            "reply_html": _as_html(txt),
            "needs_handoff": False,
            "handoff_reason": "other",
        }

    # ---- Explicit transfer intent ----
    if _looks_like_transfer_intent(inbound):
        txt = (
            f"{'Hi ' + first_name + ',' if first_name else 'Hi there,'}\n\n"
            "I can help with that.\n\n"
            "Please send the recipient’s name and best phone or email."
        ).strip()
        return {
            "reply_text": txt,
            "reply_html": _as_html(txt),
            "needs_handoff": False,
            "handoff_reason": "other",
        }

    # ---- Explicit service-credit intent ----
    if _looks_like_service_credit_intent(inbound):
        txt = (
            f"{'Hi ' + first_name + ',' if first_name else 'Hi there,'}\n\n"
            "I can help with that.\n\n"
            "If you already have your 16-digit voucher code, send it here. "
            "If not, I can help you figure out where to find it."
        ).strip()
        return {
            "reply_text": txt,
            "reply_html": _as_html(txt),
            "needs_handoff": False,
            "handoff_reason": "other",
        }

    code = _extract_voucher_code(inbound)
    if code:
        # ✅ Voucher lookup requires a human (Patti cannot verify)
        txt = (
            f"{'Hi ' + first_name + ',' if first_name else 'Hi there,'}\n\n"
            "Thanks — I got your voucher code.\n"
            "I’m looping in a team member now to confirm eligibility and make sure everything is set up correctly.\n\n"
            "Are you planning to use it for yourself, or gift it to someone?"
        ).strip()
        return {
            "reply_text": txt,
            "reply_html": _as_html(txt),
            "needs_handoff": True,
            "handoff_reason": "voucher_lookup",
        }

    # ---- GPT for everything else ----
    if not _oai:
        # fail-open, minimal helpful reply
        txt = (
            f"{'Hi ' + first_name + ',' if first_name else 'Hi there,'}\n\n"
            "I can help with that.\n\n"
            "If you already have your 16-digit voucher code, send it here. "
            "If not, I can help you figure out where to find it."
        ).strip()
        return {
            "reply_text": txt,
            "reply_html": _as_html(txt),
            "needs_handoff": False,
            "handoff_reason": "other",
        }

    tier_line = "Highest tier ($1,000) — 2+ Loyalists" if ("2+" in bucket or "1,000" in bucket or "1000" in bucket) else "Brand Advocate tier ($500)"

    user_prompt = (
        f"Context:\n"
        f"- Rooftop: {rooftop_name or 'Patterson Autos Mazda dealership'}\n"
        f"- Customer first name: {first_name or 'there'}\n"
        f"- Bucket/tier: {bucket or 'unknown'} ({tier_line})\n\n"
        f"Customer email:\n{inbound}\n\n"
        f"Write the best next email reply following the rules."
    )

    try:
        system_prompt = SYSTEM_PROMPT.format(
            rooftop_name=rooftop_name or "Patterson Autos Mazda dealership"
        )
        resp = _oai.chat.completions.create(
            model=EMAIL_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
        )
        raw = (resp.choices[0].message.content or "").strip()
        data = _safe_json(raw)

        reply_text = (data.get("reply_text") or "").strip()
        reply_html = (data.get("reply_html") or "").strip()
        needs_handoff = bool(data.get("needs_handoff"))
        handoff_reason = (data.get("handoff_reason") or "other").strip().lower()
        if handoff_reason not in HANDOFF_REASONS:
            handoff_reason = "other"

        if not reply_text:
            # fallback if model returns something odd
            reply_text = (
                f"{'Hi ' + first_name + ',' if first_name else 'Hi there,'}\n\n"
                "I can help with that.\n\n"
                "If you already have your 16-digit voucher code, send it here. "
                "If not, I can help you figure out where to find it."
            ).strip()
        if not reply_html:
            reply_html = _as_html(reply_text)

        return {
            "reply_text": reply_text,
            "reply_html": reply_html,
            "needs_handoff": needs_handoff,
            "handoff_reason": handoff_reason,
        }

    except Exception:
        log.exception("Mazda Loyalty GPT reply failed")
        txt = (
            f"{'Hi ' + first_name + ',' if first_name else 'Hi there,'}\n\n"
            "I can help with that.\n\n"
            "If you already have your 16-digit voucher code, send it here. "
            "If not, I can help you figure out where to find it."
        ).strip()
        return {
            "reply_text": txt,
            "reply_html": _as_html(txt),
            "needs_handoff": False,
            "handoff_reason": "other",
        }
