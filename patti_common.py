import re as _re
import re as _re2
from rooftops import ROOFTOP_INFO

from datetime import datetime as _dt
import zoneinfo as _zi

EXIT_KEYWORDS = [
    "not interested", "no longer interested", "bought elsewhere",
    "already purchased", "stop emailing", "unsubscribe",
    "please stop", "no thanks", "do not contact",
    "leave me alone", "sold my car", "found another dealer",
    "pass on the offer", "going to pass", "maybe later", "not right now", 
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
    r'going\s+to\s+pass|pass(?:ing)?\s+on(?:\s+the)?\s+offer|'
    r'stop\s+email|do\s+not\s+contact|please\s+stop|unsubscribe|'
    r'take\s+me\s+off|remove\s+me|leave me alone|bought elsewhere|already purchased'
    r')\b'
)
def _is_decline(text: str) -> bool:
    return bool(_DECLINE_RE.search(text or ""))


_OPT_OUT_RE = _re.compile(
    r"(?i)\b("
    r"stop|stop\s+all|stop\s+now|end|cancel|quit|"
    r"unsubscribe|remove\s+me|do\s+not\s+contact|do\s+not\s+email|don't\s+email|"
    r"no\s+further\s+contact|stop\s+contacting|stop\s+emailing|opt\s*out|opt-?out|"
    r"cease\s+and\s+desist"
    r")\b"
)

def _is_optout_text(t: str) -> bool:
    t = (t or "").strip()
    return bool(_OPT_OUT_RE.search(t))

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

def fmt_local_human(dt: _dt, tz_name: str = "America/Los_Angeles") -> str:
    """
    Return 'Friday, Nov 14 at 12:00 PM' in the given timezone.
    """
    try:
        z = _zi.ZoneInfo(tz_name)
        local = dt.astimezone(z)
    except Exception:
        local = dt

    time_str = local.strftime("%I:%M %p").lstrip("0")
    return f"{local.strftime('%A')}, {local.strftime('%b')} {local.day} at {time_str}"

# Detect any existing booking token/link so we don't double-insert a CTA
_SCHED_ANY_RE = _re2.compile(r'(?is)(LegacySalesApptSchLink|Schedule\s+Your\s+Visit</a>)')


def normalize_patti_body(body_html: str) -> str:
    """
    Tidy GPT output: strip stray Patti signatures and collapse whitespace.
    This is a simplified version of the KBB normalizer.
    """
    body_html = body_html or ""
    # Strip any trailing Patti signature junk if GPT adds it
    body_html = _re.sub(
        r'(?is)(?:\n\s*)?patti\s*(?:<br/?>|\r?\n)+.*?$', 
        '', 
        body_html.strip()
    )
    # Collapse multiple blank lines
    body_html = _re.sub(r'\n{2,}', '\n', body_html)
    return body_html


def append_soft_schedule_sentence(body_html: str, rooftop_name: str) -> str:
    """
    Append one polite scheduling CTA with a clickable booking link.
    - If body already contains any scheduler token/link, do nothing.
    - If no booking link is configured for the rooftop, do nothing (don't insert Legacy token).
    """
    body_html = body_html or ""

    # If they already have a booking token or scheduler link, skip
    if _SCHED_ANY_RE.search(body_html):
        return body_html

    rt = (ROOFTOP_INFO.get(rooftop_name) or {})
    href = (rt.get("booking_link") or rt.get("scheduler_url") or "").strip()

    # No configured booking link? Don't add anything.
    if not href or href.startswith("<{"):
        return body_html

    # Make it clickable
    link_html = (
        f'<a href="{href}" style="color:#0B66C3; text-decoration:none;" target="_blank" rel="noopener">'
        "schedule directly here"
        "</a>"
    )

    soft_line = f"<p>Let me know a time that works for you, or {link_html}.</p>"

    # If body has <p> tags, just append; otherwise wrap it
    if _re2.search(r'(?is)<p[^>]*>.*?</p>', body_html):
        return body_html.rstrip() + soft_line

    return f"<p>{body_html.strip()}</p>{soft_line}" if body_html.strip() else soft_line


def rewrite_sched_cta_for_booked(body_html: str) -> str:
    """
    Very simple rewrite: if there is a 'schedule' CTA, rewrite it into
    'reschedule' language for already-booked appointments.
    """
    body_html = body_html or ""
    # Basic phrase swaps; you can refine this later
    body_html = body_html.replace(
        "schedule directly here",
        "reschedule or confirm your visit here"
    )
    body_html = body_html.replace(
        "Schedule Your Visit",
        "Manage Your Visit"
    )
    return body_html


