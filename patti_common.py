import re as _re

EXIT_KEYWORDS = [
    "not interested", "no longer interested", "bought elsewhere",
    "already purchased", "stop emailing", "unsubscribe",
    "please stop", "no thanks", "do not contact",
    "leave me alone", "sold my car", "found another dealer"
]

def is_exit_message(msg: str) -> bool:
    if not msg:
        return False
    msg_low = msg.lower()
    return any(k in msg_low for k in EXIT_KEYWORDS)

# === Decline detection ==========================================================

_DECLINE_RE = _re.compile(
    r'(?i)\b('
    r'not\s+interested|no\s+longer\s+interested|not\s+going\s+to\s+sell|'
    r'stop\s+email|do\s+not\s+contact|please\s+stop|unsubscribe|'
    r'take\s+me\s+off|remove\s+me|leave me alone|bought elsewhere|already purchased'
    r')\b'
)
def _is_decline(text: str) -> bool:
    return bool(_DECLINE_RE.search(text or ""))



def _is_optout_text(t: str) -> bool:
    t = (t or "").lower()
    return any(kw in t for kw in (
        "stop emailing me", "stop email", "do not email", "don't email",
        "unsubscribe", "remove me", "no further contact",
        "stop contacting", "opt out", "opt-out", "optout", "cease and desist"
    ))

def _latest_customer_optout(opportunity):
    """
    Return (found: bool, ts_iso: str|None, txt: str|None) for the newest customer msg
    that contains an opt-out phrase, regardless of what came after.
    """
    msgs = (opportunity.get("messages") or [])
    latest = None
    for m in reversed(msgs):
        if m.get("msgFrom") == "customer" and _is_optout_text(m.get("body")):
            # use message date if present, else None
            latest = (True, m.get("date"), m.get("body"))
            break
    return latest or (False, None, None)
