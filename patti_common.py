from rooftops import ROOFTOP_INFO
import re
import html


EMAIL_RE = re.compile(
    r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})",
    re.I
)

PHONE_RE = re.compile(
    r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"
)


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

PROVIDER_BOILERPLATE_LINES_RE = re.compile(
    r"(?im)^\s*(?:"
    r"new customer lead for|"
    r".+?\s+is\s+interested\s+in\s+one\s+of\s+your\s+carfax\s+car\s+listings|"
    r"here's how to contact this customer|"
    r"first name|last name|email|e-?mail|phone|telephone|"
    r"date submitted|lead id|listing|price|condition|stock|vin|"
    r"year/make/model|year\b|make\b|model\b|"
    r"interested in\b|"
    r"type of lead\b|"
    r"contact information\b|"
    r"offeramount\b|street\b|city\b|zip\b|"
    r"lead provided by|"
    r"for more information about your carfax account"
    r"see this shopper'?s other leads|"
    r"target budget|"
    r"\bvdp views?\b|"
    r"\bshopper'?s other leads\b|"
    r"\bview (?:this )?(?:shopper|lead|details|lead details)\b|"
    r"\bclick here\b|"
    r"\bview in (?:cars\.com|carfax)\b|"
    r"\bmanage lead\b|"
    r"\blead (?:details|summary)\b|"
    r"\bdealer (?:center|portal)\b|"
    r"\bonline activity\b|"
    r"\bvehicle details page\b|"
    r"\bprivacy policy\b|"
    r"\bdo not reply\b|"
    r"\bthis message was sent\b|"
    r"\bemail preferences\b|"
    r"\bunsubscribe\b|"
    r").*$"
)

_WS_RE = re.compile(r"\s+")

def get_next_template_day(
    *,
    last_template_day_sent: int | None,
    cadence_days: list[int],
) -> int | None:
    """
    Returns the next cadence day strictly greater than last_template_day_sent.
    If none remain, returns None.
    """
    if not cadence_days:
        return None

    if not last_template_day_sent:
        return cadence_days[0]

    for day in cadence_days:
        if day > last_template_day_sent:
            return day

    return None


def _norm_provider_line(s: str) -> str:
    """
    Normalize a single provider line for matching/filtering.
    Keep it conservative: strip, unescape HTML entities, remove weird whitespace.
    """
    if not s:
        return ""
    # convert HTML entities (&nbsp;, &amp;, etc.)
    s = html.unescape(s)

    # normalize common weird spaces
    s = s.replace("\u00a0", " ")   # nbsp
    s = s.replace("\u200b", "")   # zero-width space
    s = s.replace("\ufeff", "")   # BOM

    # collapse whitespace
    s = _WS_RE.sub(" ", s).strip()
    return s


def extract_customer_comment_from_provider(body_text: str) -> str:
    if not body_text:
        return ""

    lines = []
    for raw in body_text.splitlines():
        s = _norm_provider_line(raw)
        if not s:
            continue
        if PROVIDER_BOILERPLATE_LINES_RE.search(s):
            continue
        lines.append(s)

    if not lines:
        return ""

    # --- Pass 1: detect comment label, with or without colon ---
    captured = []
    in_comment_block = False
    saw_comment_label = False

    for s in lines:
        m = _COMMENT_LABEL_RE.match(s)
        if m:
            label = (m.group(1) or "").strip().lower()
            remainder = (m.group(2) or "").strip()

            # If this is a comment label, start capturing.
            if label in {"additional comments", "additional comment", "customer comments", "customer comment",
                         "comments", "comment", "message", "questions", "question"}:
                saw_comment_label = True
                in_comment_block = True
                if remainder:
                    captured.append(remainder)
                continue

        if in_comment_block:
            # Stop if we hit another provider field or obvious template section
            if _PROVIDER_FIELD_LINE_RE.match(s):
                break
            if PROVIDER_BOILERPLATE_LINES_RE.search(s):
                break
            if _PROVIDER_TEMPLATE_HINT_RE.search(s):  # <-- important for Apollo headers
                break
            captured.append(s)

    if captured:
        out = " ".join(captured).strip()
        # Guard: if we somehow captured template header junk, discard
        if _PROVIDER_TEMPLATE_HINT_RE.search(out) and len(out) < 200:
            return ""
        return out

    # --- Pass 1b: If we saw "Comments" label but nothing captured, don't fall back to template text ---
    if saw_comment_label:
        return ""

    # --- Pass 2: fallback ---
    kept = []
    for s in lines:
        if _PROVIDER_FIELD_LINE_RE.match(s):
            continue
        # If the line still looks like provider header/template, drop it
        if _PROVIDER_TEMPLATE_HINT_RE.search(s):
            continue
        kept.append(s)

    return " ".join(kept).strip()

# === Decline detection ==========================================================

_DECLINE_RE = re.compile(
    r'(?i)\b('
    r'not\s+interested|no\s+longer\s+interested|not\s+going\s+to\s+sell|'
    r'going\s+to\s+pass|pass(?:ing)?\s+on(?:\s+the)?\s+offer|'
    r'stop\s+email|do\s+not\s+contact|please\s+stop|unsubscribe|'
    r'take\s+me\s+off|remove\s+me|leave me alone|bought elsewhere|already purchased'
    r')\b'
)
def _is_decline(text: str) -> bool:
    return bool(_DECLINE_RE.search(text or ""))


_OPT_OUT_RE = re.compile(
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
_SCHED_ANY_RE = re.compile(
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
    body_html = re.sub(
        r"(?is)\b(?:best|thanks|thank you|regards|sincerely|warmly|cheers)\b\s*,?\s*patti\s*$",
        "",
        body_html,
    ).strip()

    # 2) Strip HTML-ish sign-offs near the end (<br> Best,<br>Patti ...)
    body_html = re.sub(
        r"(?is)(?:<br\s*/?>\s*){0,3}\b(?:best|thanks|thank you|regards|sincerely|warmly|cheers)\b\s*,?\s*(?:<br\s*/?>\s*){0,3}patti\s*(?:<br\s*/?>\s*)*$",
        "",
        body_html,
    ).strip()

    # 3) Your existing “Patti + Virtual Assistant …” cleanup (keep, but broaden slightly)
    body_html = re.sub(
        r"(?is)(?:\n\s*)?patti\s*(?:<br/?>|\r?\n)+.*?$",
        "",
        body_html,
    ).strip()

    # Collapse multiple blank lines
    body_html = re.sub(r"\n{2,}", "\n", body_html)
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
    if re.search(r'(?is)<p[^>]*>.*?</p>', body_html):
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

def build_patti_footer(rooftop_name: str) -> str:
    rt = (ROOFTOP_INFO.get(rooftop_name) or {})

    img_url      = rt.get("signature_img") or "https://prod.tvmimageservice.com/images/GetLibraryImage?fileNameOrId=664005&Width=0&Height=0&logo=y"
    patti_email  = rt.get("patti_email")   or "patti@pattersonautos.com"
    dealer_site  = (rt.get("website") or "https://www.pattersonautos.com").rstrip("/")
    dealer_addr  = rt.get("address")       or ""
    logo_alt     = f"Patti | {rooftop_name}"

    clean_site = dealer_site.replace("https://", "").replace("http://", "")

    return f"""
<table width="650" border="0" cellspacing="0" cellpadding="0" style="margin-top:18px;border-collapse:collapse;">
  <tr>
    <td style="padding:14px 16px;border:1px solid #e2e2e2;border-radius:4px;background-color:#fafafa;">
      <table width="100%" border="0" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
        <tr>
          <!-- LEFT: logo -->
          <td width="260" valign="top" align="left" style="padding-right:20px;">
            <img src="{img_url}"
                 alt="{logo_alt}"
                 width="240"
                 border="0"
                 style="display:block;height:auto;max-width:240px;">
          </td>

          <!-- RIGHT: Patti + contact details -->
          <td valign="top" align="left"
              style="font-family:Arial, Helvetica, sans-serif;color:#222222;vertical-align:top;">

            <!-- Patti block -->
            <div style="font-size:14px;line-height:18px;margin-bottom:8px;">
              <strong>Patti</strong><br>
              Virtual Assistant | {rooftop_name}
            </div>

            <!-- Contact -->
            <div style="font-size:13px;line-height:20px;margin-bottom:8px;">
              <div>
                <strong>Email:</strong>
                <a href="mailto:{patti_email}" style="color:#0066cc;text-decoration:none;">
                  {patti_email}
                </a>
              </div>
              <div>
                <strong>Website:</strong>
                <a href="{dealer_site}" style="color:#0066cc;text-decoration:none;">
                  {clean_site}
                </a>
              </div>
            </div>

            <!-- Address -->
            <div style="font-size:13px;line-height:20px;color:#333333;">
              <div>{rooftop_name}</div>
              <div>{dealer_addr}</div>
            </div>

          </td>
        </tr>
      </table>
    </td>
  </tr>
</table>
    """.strip()

