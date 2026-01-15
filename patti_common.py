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

# Detect any scheduling token/link/CTA so we can skip inserting,
# or remove scheduling verbiage cleanly.
_SCHED_ANY_RE = _re.compile(
    r"(?is)("
    r"LegacySalesApptSchLink|"
    r"scheduleservice|"
    r"https?://[^\s\"']*(?:schedule|scheduleservice)[^\s\"']*|"
    r"schedule\s+directly(?:\s+here)?|"
    r"reserve\s+your\s+time|"
    r"schedule\s+(?:an\s+)?appointment|"
    r"schedule\s+your\s+visit|"
    r"(?:feel\s+free\s+to\s+)?let\s+me\s+know\s+(?:a\s+)?(?:day\s+and\s+)?time\s+that\s+works[^\.!\?]*[\.!\?]?|"
    r"please\s+let\s+us\s+know\s+a\s+convenient\s+time[^\.!\?]*[\.!\?]?"
    r")"
)


def enforce_standard_schedule_sentence(body_html: str) -> str:
    """Ensure exactly one standard CTA appears above visit/closing lines."""
    if not body_html:
        body_html = ""

    # 0) Normalize whitespace a bit so paragraph regex works better
    body_html = re.sub(r'\s+', ' ', body_html).strip()

    standard_html = (
        '<p>Please let us know a convenient time for you, or you can instantly reserve your time here: '
        '<{LegacySalesApptSchLink}></p>'
    )

    # 1) Remove any <p> paragraphs that already contain scheduling verbiage or the CRM token
    #    (so we don't leave behind "instantly here: ." fragments)
    PARA = r'(?is)<p[^>]*>.*?</p>'
    SCHED_PAT = r'(?i)(LegacySalesApptSchLink|reserve your time|schedule (an )?appointment|schedule your visit)'
    def _kill_sched_paras(m):
        para = m.group(0)
        return '' if re.search(SCHED_PAT, para) else para

    body_html = re.sub(PARA, _kill_sched_paras, body_html).strip()

    # 2) Split into paragraphs to position the CTA
    parts = re.findall(PARA, body_html)  # list of <p>…</p>
    if not parts:
        parts = [f"<p>{body_html}</p>"]  # fallback if model didn't use <p> tags

    # Insert CTA before the first visit/closing paragraph; else prepend
    insert_at = None
    for i, p in enumerate(parts):
        if re.search(r'(?i)(ready to visit|bring|looking forward)', p):
            insert_at = i
            break
    if insert_at is None:
        insert_at = 0
    parts.insert(insert_at, standard_html)

    # 3) Join and ensure we don't have duplicate CTAs
    combined = ''.join(parts)
    combined = re.sub(r'(?is)(<p>[^<]*LegacySalesApptSchLink[^<]*</p>)(.*?)\1', r'\1\2', combined).strip()
    return combined






def normalize_patti_body(body_html: str) -> str:
    """
    Tidy GPT output: strip stray Patti signatures/sign-offs and collapse whitespace.
    """
    body_html = (body_html or "").strip()

    # 1) Strip common plain-text sign-offs at the very end (Best, Patti / Thanks, Patti, etc.)
    body_html = _re.sub(
        r"(?is)\b(?:best|thanks|thank you|regards|sincerely|warmly|cheers)\b\s*,?\s*patti\s*$",
        "",
        body_html,
    ).strip()

    # 2) Strip HTML-ish sign-offs near the end (<br> Best,<br>Patti ...)
    body_html = _re.sub(
        r"(?is)(?:<br\s*/?>\s*){0,3}\b(?:best|thanks|thank you|regards|sincerely|warmly|cheers)\b\s*,?\s*(?:<br\s*/?>\s*){0,3}patti\s*(?:<br\s*/?>\s*)*$",
        "",
        body_html,
    ).strip()

    # 3) Your existing “Patti + Virtual Assistant …” cleanup (keep, but broaden slightly)
    body_html = _re.sub(
        r"(?is)(?:\n\s*)?patti\s*(?:<br/?>|\r?\n)+.*?$",
        "",
        body_html,
    ).strip()

    # Collapse multiple blank lines
    body_html = _re.sub(r"\n{2,}", "\n", body_html)
    return body_html



def append_soft_schedule_sentence(body_html: str, rooftop_name: str) -> str:
    """
    Append a simple, low-friction scheduling CTA (no link for now).

    Current behavior:
    - If body already contains any scheduler token/link, do nothing.
    - Always append a plain-text CTA asking for a day/time.

    NOTE:
    - The dynamic booking-link logic is intentionally commented out
      so it can be re-enabled later without rewriting this function.
    """
    body_html = body_html or ""

    # If they already have a booking token or scheduler link, skip
    if _SCHED_ANY_RE.search(body_html):
        return body_html

    # ------------------------------------------------------------------
    # 🔕 TEMPORARILY DISABLED: dynamic booking link logic
    # ------------------------------------------------------------------
    # rt = (ROOFTOP_INFO.get(rooftop_name) or {})
    # href = (rt.get("booking_link") or rt.get("scheduler_url") or "").strip()
    #
    # # No configured booking link? Previously we did nothing.
    # if not href or href.startswith("<{"):
    #     return body_html
    #
    # link_html = (
    #     f'<a href="{href}" style="color:#0B66C3; text-decoration:none;" '
    #     'target="_blank" rel="noopener">schedule directly here</a>'
    # )
    #
    # soft_line = f"<p>Let me know a time that works for you, or {link_html}.</p>"
    # ------------------------------------------------------------------

    # ✅ New simplified CTA (no link)
    soft_line = (
        "<p>"
        "I'd love to set up a time for you to come by and visit our showroom - is there a day and time that works best for you?"
        "</p>"
    )

    # If body already has <p> tags, just append; otherwise wrap it
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


