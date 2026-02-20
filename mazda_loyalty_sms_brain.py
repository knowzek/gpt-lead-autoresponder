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
    "no code", "not received", "haven't received", "havent received",
    "can't find my voucher", "cant find my voucher",
    "can't find my code", "cant find my code",
    "didn't get my voucher", "didnt get my voucher",
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
Return ONLY JSON:
{"reply": "...", "needs_handoff": true/false, "handoff_reason": "pricing|trade|finance|angry|complaint|other"}
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
                "mazdaemail@dealers-mazdausa.com. Please check your Inbox + Spam + Promotions + All Mail/Archive. "
                "If you still can’t find it after searching, tell me and I’ll loop in a team member to help."
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

    try:
        resp = _oai.chat.completions.create(
            model=SMS_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM},
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
