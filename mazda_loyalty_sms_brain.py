# mazda_loyalty_sms_brain.py
"""
Mazda Loyalty SMS reply brain:
- concise, human, not hype
- NEVER quotes pricing / payments / trade / APR
- flags handoff on pricing/trade/finance/angry
- supports voucher code capture
"""

from __future__ import annotations
import os, re, json, logging
from typing import Any, Dict

from openai import OpenAI

log = logging.getLogger("patti.mazda_loyalty_sms_brain")

OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
SMS_MODEL = (os.getenv("SMS_OPENAI_MODEL") or os.getenv("OPENAI_MODEL") or "gpt-4o-mini").strip()

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
    "where do i get the code", "where can i get the code",
    "how can i get the code", "how do i get the code",
    "how can i obtain the code", "how do i obtain the code",
    "how can i get my code", "how do i get my code",
    "where do i get my code", "where can i get my code",
    "where is my voucher", "where's my voucher", "wheres my voucher",
    "where do i get the voucher", "where can i get the voucher",
    "how can i get the voucher", "how do i get the voucher",
    "no code", "not received", "haven't received", "havent received",
    "can't find my voucher", "cant find my voucher",
    "can't find my code", "cant find my code",
    "didn't get my voucher", "didnt get my voucher",
    "didn't get my code", "didnt get my code",
]

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

VOUCHER_RE = re.compile(r"\b(\d[ -]?){15}\d\b")

SYSTEM = """You are Patti, a virtual assistant for a Mazda dealership.
This is SMS about the Mazda Loyalty CX-5 reward program.

Rules:
- Be brief (1–3 short lines).
- No emojis. No marketing hype.
- Do not quote pricing, payments, APR, lease terms, or trade values.
- If asked about pricing/trade/finance, escalate to a human.
- If user provides a 16-digit voucher code, acknowledge and say you'll verify it.
- Ask at most ONE question in the reply.

Program context:
- If the customer does not want or need a vehicle right now, there are still two helpful alternatives:
  1. They may transfer the voucher to a family member or friend.
  2. They may redeem it for a $100 Service & Parts credit at {rooftop_name} in exchange for the loyalty code.

Program guardrails:
- Do NOT invent program rules, deadlines, eligibility criteria, or availability.
- If the customer provides a voucher code, acknowledge it and say a team member will verify it.
- If the customer says they didn’t receive, can’t find, lost, or don’t know how to get their voucher code:
  - tell them the code should have been emailed from Mazda at mazdaemail@dealers-mazdausa.com
  - tell them to check Inbox, Spam, Promotions, and All Mail/Archive
  - do NOT tell them to come into the dealership to get the code
  - do NOT say the code is available in-store unless a human has confirmed that
  - do NOT invent alternate ways to retrieve the code
- If the customer wants to transfer/gift it, say you can help and ask for the recipient’s name and best contact info.
- If they show buying intent, move to the next step briefly and naturally.
- Never quote pricing, OTD, payments, APR, lease terms, or trade values. Escalate those to a human.

Special handling:
- If the customer says they already bought a car, just purchased, already replaced it, are not in the market, do not need a car, or are not interested right now:
  - do NOT continue sales messaging
  - briefly acknowledge their situation
  - if appropriate, briefly congratulate them
  - mention they can transfer the voucher to a family member or friend
  - mention they can redeem it for a $100 Service & Parts credit at {rooftop_name} in exchange for the loyalty code
  - offer help with either transfer or redemption
- Do not make this sound pushy.

Return ONLY JSON:
{{"reply": "...", "needs_handoff": true/false, "handoff_reason": "pricing|trade|finance|angry|complaint|other"}}
"""

def _contains_any(t: str, toks: tuple[str, ...]) -> bool:
    s = (t or "").lower()
    return any(x in s for x in toks)

def _extract_code(t: str) -> str:
    m = VOUCHER_RE.search(t or "")
    if not m:
        return ""
    digits = "".join(ch for ch in m.group(0) if ch.isdigit())
    return digits if len(digits) == 16 else ""

def _safe_json(s: str) -> Dict[str, Any]:
    s = (s or "").strip()
    try:
        return json.loads(s)
    except Exception:
        a, b = s.find("{"), s.rfind("}")
        if a != -1 and b != -1 and b > a:
            try:
                return json.loads(s[a:b+1])
            except Exception:
                return {}
        return {}

import re

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
    r"not in the market for (a|another) (car|vehicle)|"
    r"not looking to buy (a|another) (car|vehicle)|"
    r"not interested in buying (a|another)? (car|vehicle)? right now|"
    r"not interested right now|"
    r"don't need (a|another) (car|vehicle)|dont need (a|another) (car|vehicle)|"
    r"already replaced (it|the vehicle|the car)|"
    r"we already bought (something|a car|another car)"
    r")\b",
    re.I,
)

def _looks_like_not_in_market(text: str) -> bool:
    return bool(_NOT_IN_MARKET_RE.search(text or ""))

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

def generate_mazda_loyalty_sms_reply(
    *,
    first_name: str,
    bucket: str,
    rooftop_name: str,
    last_inbound: str,
    thread_snippet: list[dict] | None = None,
) -> Dict[str, Any]:
    inbound = (last_inbound or "").strip()
    first = (first_name or "").strip()

    # Stop keywords
    if _contains_any(inbound, STOP_TOKENS):
        return {"reply": "Got it — we’ll stop reaching out. If you need anything later, just text me here.", "needs_handoff": False, "handoff_reason": "other"}

    # Pricing/trade/finance => handoff
    if _contains_any(inbound, PRICING_TOKENS):
        prefix = f"{first}, " if first else ""
        return {
            "reply": f"{prefix}thanks — I’m looping in a team member to help with that so you get accurate details. What day/time were you hoping for?",
            "needs_handoff": True,
            "handoff_reason": "pricing",
        }

    # “can't find / didn't receive my code”
    if _looks_like_code_not_found(inbound):
        prefix = f"{first}, " if first else ""
        return {
            "reply": (
                f"{prefix}no worries — the loyalty voucher code is usually emailed from Mazda at "
                "mazdaemail@dealers-mazdausa.com. Please check your Inbox, Spam, Promotions, and All Mail/Archive. "
                "If you still can’t find it after searching, tell me and I’ll loop in a team member to help."
            ),
            "needs_handoff": False,
            "handoff_reason": "other",
        }
        
    # ---- Clear buying intent: do NOT push transfer/service credit ----
    if _looks_like_buying_intent(inbound):
        prefix = f"{first}, " if first else ""
        return {
            "reply": (
                f"{prefix}great — I can help with that. "
                "Do you already have your 16-digit Mazda loyalty voucher code?"
            ),
            "needs_handoff": False,
            "handoff_reason": "other",
        }

    # ---- Explicit transfer intent ----
    if _looks_like_transfer_intent(inbound):
        prefix = f"{first}, " if first else ""
        return {
            "reply": (
                f"{prefix}Perfect — I’ll have a team member handle the voucher transfer for you "
                "to make sure it’s done correctly. They’ll reach out shortly."
            ),
            "needs_handoff": True,
            "handoff_reason": "other",
        }

    # ---- Explicit service-credit intent ----
    if _looks_like_service_credit_intent(inbound):
        prefix = f"{first}, " if first else ""
        return {
            "reply": (
                f"{prefix}I can help with that. "
                "If you already have your 16-digit voucher code, text it here. "
                "If not, I can help you figure out where to find it."
            ),
            "needs_handoff": False,
            "handoff_reason": "other",
        }

    # ---- Already bought / not in market ----
    if _looks_like_recent_purchase(inbound) or _looks_like_not_in_market(inbound):
        prefix = f"{first}, " if first else ""
        opening = (
            "congrats on the new vehicle. "
            if _looks_like_recent_purchase(inbound)
            else "totally understand. "
        )
        return {
            "reply": (
                f"{prefix}{opening}"
                f"If you do not need the voucher for yourself, you may still be able to transfer it to a family member or friend, "
                f"or redeem it for a $100 Service & Parts credit at {rooftop_name or 'Patterson Autos Mazda dealership'} "
                "in exchange for the loyalty code. "
                "If you'd like, I can help with either option."
            ),
            "needs_handoff": False,
            "handoff_reason": "other",
        }

    # Voucher code present
    code = _extract_code(inbound)
    if code:
        # ✅ Voucher lookup requires a human (Patti cannot verify)
        prefix = f"{first}, " if first else ""
        return {
            "reply": (
                f"{prefix}thanks — I got your voucher code. "
                "I’m looping in a team member now to confirm eligibility and make sure everything is set up correctly. "
            ),
            "needs_handoff": True,
            "handoff_reason": "voucher_lookup",
        }


    # GPT for everything else
    if not _oai:
        prefix = f"{first}, " if first else ""
        return {
            "reply": f"{prefix}I can help. If you have your 16-digit voucher code, text it here and I’ll verify it for you.",
            "needs_handoff": False,
            "handoff_reason": "other",
        }

    tier = "highest tier ($1,000)" if ("2+" in (bucket or "") or "1,000" in (bucket or "") or "1000" in (bucket or "")) else "brand advocate tier ($500)"

    user = (
        f"Rooftop: {rooftop_name or 'Mazda'}\n"
        f"Customer first name: {first or 'there'}\n"
        f"Bucket/tier: {bucket or 'unknown'} ({tier})\n\n"
        f"Latest SMS:\n{inbound}\n\n"
        "Write the best next SMS reply following the rules."
    )

    system_prompt = SYSTEM.format(
        rooftop_name=rooftop_name or "Patterson Autos Mazda dealership"
    )
    
    try:
        resp = _oai.chat.completions.create(
            model=SMS_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user},
            ],
            temperature=0.3,
        )
        
        data = _safe_json(resp.choices[0].message.content or "")
        reply = (data.get("reply") or "").strip()
        needs = bool(data.get("needs_handoff"))
        reason = (data.get("handoff_reason") or "other").strip().lower()

        if not reply:
            prefix = f"{first}, " if first else ""
            reply = f"{prefix}I can help. If you have your 16-digit voucher code, text it here and I’ll verify it."

        return {"reply": reply, "needs_handoff": needs, "handoff_reason": reason}
    except Exception:
        log.exception("Mazda Loyalty SMS GPT failed")
        prefix = f"{first}, " if first else ""
        return {"reply": f"{prefix}I can help. If you have your 16-digit voucher code, text it here and I’ll verify it.", "needs_handoff": False, "handoff_reason": "other"}
