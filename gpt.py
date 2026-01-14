import os, time, json, logging, re
from datetime import datetime
from openai import OpenAI
from openai import APIStatusError, NotFoundError  # available in recent SDKs; if import fails, just catch Exception
from rooftops import ROOFTOP_INFO

from dotenv import load_dotenv
load_dotenv()
client = OpenAI(api_key = os.getenv("OPENAI_API_KEY"))

log = logging.getLogger("patti.gpt")

PRIMARY_MODEL = os.getenv("OPENAI_CHAT_MODEL", "gpt-4o-mini")
FALLBACK_MODELS = [m.strip() for m in os.getenv("OPENAI_FALLBACK_MODELS", "gpt-4o,gpt-4o-mini").split(",") if m.strip()]
MODEL_CHAIN = [PRIMARY_MODEL] + [m for m in FALLBACK_MODELS if m and m != PRIMARY_MODEL]

# Dynamically determine current month
CURRENT_MONTH = datetime.now().strftime("%B")

log = logging.getLogger("patti")

CLIENT_TIMEOUT = float(os.getenv("OPENAI_TIMEOUT", "30"))
MAX_RETRIES    = int(os.getenv("OPENAI_MAX_RETRIES", "4"))
ASSISTANT_ID   = os.getenv("OPENAI_ASSISTANT_ID") 

# Allowed site list (persist here so the model never invents off-brand links)
PATTERSON_SITES = [
    "https://www.tustinmazda.com/",
    "https://www.huntingtonbeachmazda.com/",
    "https://www.tustinhyundai.com/",
    "https://www.missionviejokia.com/",
    "https://www.pattersonautos.com/",
]

def _get_rooftop_address(rooftop_name: str) -> str:
    try:
        from rooftops import ROOFTOP_INFO
        return ((ROOFTOP_INFO.get(rooftop_name, {}) or {}).get("address") or "")
    except Exception:
        return ""

def _safe_extract_text(resp):
    try:
        return (resp.choices[0].message.content or "").strip()
    except Exception as e:
        log.warning("OpenAI response missing content: %s", e)
        return ""

def _ensure_reply_dict(text: str, default_subject: str, default_body_leadin: str):
    """
    Try JSON first, then fall back to plain text -> dict, and finally a safe template.
    Always returns {'subject','body'}.
    """
    # JSON path
    try:
        data = json.loads(text)
        subj = (data.get("subject") or "").strip()
        body = (data.get("body") or "").strip()
        if subj and body:
            return {"subject": subj, "body": body}
    except Exception:
        pass

    # Plain text path (use the text as body if it looks like a sentence)
    clean = (text or "").strip()
    if clean:
        # crude subject if not provided
        subj = default_subject
        # if the text accidentally contains JSON braces or HTML noise, trim a bit
        trimmed = re.sub(r"^\s*[{[][\s\S]*$", "", clean).strip() or clean
        return {"subject": subj, "body": trimmed}

    # Final template fallback (should basically never happen)
    return {
        "subject": default_subject,
        "body": default_body_leadin
    }

def chat_complete_with_fallback(messages, want_json: bool = True, temperature: float = 0.6):
    """
    Try models in MODEL_CHAIN until one works.
    If JSON mode isn't supported by a model, retry without strict JSON.
    """
    last_err = None
    for m in MODEL_CHAIN:
        # First try with JSON format (if requested)
        for attempt in (0, 1):  # 0 = with json, 1 = without json
            try:
                kwargs = dict(model=m, messages=messages, temperature=temperature)
                if want_json and attempt == 0:
                    # Some SDKs support response_format={"type":"json_object"}
                    kwargs["response_format"] = {"type": "json_object"}
                resp = client.chat.completions.create(**kwargs)
                return m, resp
            except NotFoundError as e:
                last_err = e
                # Model truly not available; try next model
                break
            except APIStatusError as e:
                last_err = e
                # If the error might be due to response_format not supported, retry once without it
                if attempt == 0:
                    continue
                # otherwise try next model
                break
            except Exception as e:
                last_err = e
                # On unknown errors, try next model (or next attempt without JSON)
                if attempt == 0:
                    continue
                break
    raise last_err or RuntimeError("OpenAI chat completion failed with all models")

def _kbb_ico_rules_system(kbb_ctx: dict | None, rooftop_name: str | None):
    days = (kbb_ctx or {}).get("offer_valid_days", 7)
    excl_sun = (kbb_ctx or {}).get("exclude_sunday", True)
    offer_url = (kbb_ctx or {}).get("offer_url", "")
    rn = rooftop_name or "the dealership"

    parts = [
        f"You are Patti, a friendly virtual acquisition assistant for {rn}. ",
        "You manage Kelley Blue Book® Instant Cash Offer (ICO) leads.\n\n",
        "KBB ICO Conversation Rules:\n",
        "- Acknowledge the customer’s exact question first, then answer directly.\n",
        "- Do NOT propose appointment times; invite them to choose a time (the system appends the standard scheduling sentence).\n",
        f"- ICO offer validity: {days} days" + (" (excluding Sunday)" if excl_sun else "") + ". ",
        "If expired, propose re-issuing politely.\n",
        "- Never include a signature block; the system appends it.\n",
        ("- If an official KBB offer URL is provided to you, include exactly one hyperlink with the text 'View Offer' that points to it when the guest asks about their offer value, details, or expiration. Otherwise avoid URLs.\n"
         if offer_url else
         "- Avoid URLs unless explicitly provided to you.\n"),
        "- Keep 60–130 words unless the customer requested detail.\n",
        "- Stay truthful; if info is missing, ask one precise follow-up question."
    ]
    return "".join(parts)



def _build_system_stack(persona: str, customer_first: str, rooftop_name: str | None, kbb_ctx: dict | None, include_followup_rules: bool = True):
    """
    Returns a list of system messages tailored to persona.
    For convo (prevMessages=True), we still keep JSON/format + convo brain.
    """
    base = []
    if persona == "kbb_ico":
        base = [
            {"role": "system", "content": _patti_persona_system()},
            {"role": "system", "content": _patterson_why_buys_system()},
            {"role": "system", "content": _kbb_ico_rules_system(kbb_ctx, rooftop_name)},
            {"role": "system", "content": _personalization_rules_system()},
            {"role": "system", "content": _appointment_cta_system()},
            {"role": "system", "content": _compliance_system()},
            {"role": "system", "content": f"Current month: {CURRENT_MONTH}. Only reference charity campaigns if this month is listed; otherwise do not mention charity at all."},
            {"role": "system", "content": _links_and_boundaries_system()},
            {"role": "system", "content": _objection_handling_system()},
            {"role": "system", "content": _format_system()},
            {"role": "system", "content": _getCustomerMessagePrompts()},
        ]
        # KBB cadence follow-ups are handled by your template scheduler, so we usually
        # do NOT include the generic follow-up generator here. Keep it optional:
        
        if include_followup_rules:
            base.append({"role": "system", "content": _getFollowUPRules()})
            
        # ---- Inject concrete KBB facts so the model can actually see them ----
        if kbb_ctx:
            amt = kbb_ctx.get("offer_amount_usd") or kbb_ctx.get("amount_usd")
            veh = kbb_ctx.get("vehicle")
            url = kbb_ctx.get("offer_url")
        
            if amt:
                facts_lines = [f"Kelley Blue Book® Instant Cash Offer amount: {amt}."]
                if veh:
                    facts_lines.append(f"Vehicle: {veh}.")
                if url:
                    facts_lines.append(f"Offer details URL: {url}.")
        
                # (1) FACTS message
                base.append({
                    "role": "system",
                    "content": (
                        "Internal KBB facts (authoritative; use to answer customer questions accurately; "
                        "do not volunteer unless asked):\n" + " ".join(facts_lines)
                    )
                })
                log.info("KBB FACTS SYSTEM MSG: %r", base[-1]["content"])
        
                # (2) OVERRIDE rule message
                base.append({
                    "role": "system",
                    "content": (
                        "CRITICAL OVERRIDE: If the customer asks for their KBB/ICO offer/estimate/value/amount "
                        "and an internal KBB facts message contains a dollar amount, you MUST state that exact "
                        "dollar amount in your reply. Do NOT say you 'don't have access' or 'can't see it' when "
                        "the amount is provided internally. Only say you don't have the amount if no dollar amount "
                        "is present internally."
                    )
                })

                log.info("KBB FACTS SYSTEM MSG: %r", base[-1]["content"])

        return base

    # default "sales" persona (your current stack)
    base = [
        {"role": "system", "content": _patti_persona_system()},
        {"role": "system", "content": _patti_rules_system(customer_first)},
        {"role": "system", "content": _patterson_why_buys_system()},
        {"role": "system", "content": _first_message_rules_system()},
        {"role": "system", "content": _personalization_rules_system()},
        {"role": "system", "content": _appointment_cta_system()},
        {"role": "system", "content": _compliance_system()},
        {"role": "system", "content": f"Current month: {CURRENT_MONTH}. Only reference charity campaigns if this month is listed; otherwise do not mention charity at all."},
        {"role": "system", "content": _links_and_boundaries_system()},
        {"role": "system", "content": _objection_handling_system()},
        {"role": "system", "content": _format_system()},
        {"role": "system", "content": _getCustomerMessagePrompts()},
    ]
    if include_followup_rules:
        base.append({"role": "system", "content": _getFollowUPRules()})
    return base


# --- Patti system instruction builders --------------------------------

def _kbb_ico_system():
    return (
        "You are Patti, a friendly virtual acquisition assistant for {rooftop_name}. "
        "You manage Kelley Blue Book® Instant Cash Offer (ICO) leads.\n\n"
        "Rules:\n"
        "- Until the customer replies, follow the day-by-day plan (email/text/phone) exactly as scheduled.\n"
        "- Use the dealer’s provided HTML templates for Day 00, Day 01, Day 02, etc. Insert merge fields.\n"
        "- The KBB ICO offer is valid for 7 days (excluding Sunday); encourage inspection scheduling before expiry.\n"
        "- When the customer replies (any message from the lead after Day 00), STOP the drip and continue as a real person:\n"
        "  • Acknowledge what they said first.\n"
        "  • Answer directly.\n"
        "  • Offer times, options, next steps.\n"
        "  • Keep it warm, succinct, and human.\n\n"
        "Never invent policy; if unsure, ask for a time or offer a call."
    )

def _patti_persona_system():
    return (
        "You are Patti, the virtual assistant for Patterson Auto Group "
        "(Tustin Mazda, Huntington Beach Mazda, Mission Viejo Kia, Tustin Hyundai). "
        "Your tone matches our best team members: warm, professional, helpful, and never pushy."
    )

def _tustin_kia_new_location_system():
    return (
        "New-location flavor for Tustin Kia:\n"
        "- This is a newly opened Kia location with a beautiful, modern showroom.\n"
        "- When it makes sense to invite the guest in, you may mention that this location just opened in mid-November and that you'd love to show them the new showroom.\n"
        "- You can use language like 'we’re tailoring every deal to each customer’s needs' and 'your presence is your leverage' sparingly and naturally, not in every email.\n"
        "- Only use this new-location language when suggesting an in-person visit, test drive, or appraisal; do not mention it in unrelated replies.\n"
        "- Keep the tone confident, warm, and value-focused, similar to a top-performing salesperson at this store.\n"
    )


def _patti_rules_system(customer_first: str):
    return (
        "Objectives:\n"
        "- Start a natural conversation with a strong, value-forward opening.\n"
        "- Always reflect Patterson brand values and Why Buys.\n"
        "- If the customer asks a simple, factual question (e.g., availability, color options, trim, pricing, location, or anything that can be answered directly), skip long introductions, skip repeating 'thank you for your inquiry,' and answer their question immediately in 1–2 sentences before offering any next step.\n"
        "- For simple questions, do not restate the model name unless needed. Keep it natural and focused on their question.\n"
        "- Make helpful vehicle recommendations; encourage action (booking a visit).\n"
        "- Follow through based on the guest’s replies or silence.\n\n"
        "Hard Rules:\n"
        f"- Begin with exactly: Hi {customer_first or 'there'},\n"
        "- Keep 120–180 words unless more detail is clearly requested.\n"
        "- Mention the assigned salesperson by name when provided.\n"
        "- If a model is known, speak to it; otherwise invite specifics (trim, color, timing).\n"
        "- Do NOT include any signature block, phone numbers, or URLs; the system will append them.\n"
        "- Be truthful; never guess. If info is missing, ask one clear follow-up question."
    )

def _patterson_why_buys_system():
    return (
        "Patterson Why Buys (prioritize early):\n"
        "- No Addendums or Dealer MarkUps\n"
        "- Orange County Top Workplace for 20 years running\n"
        "- Community Driven\n"
        "- Master Technicians and Experienced Staff"
    )

def _first_message_rules_system():
    return (
        "First Message Guidance:\n"
        "- Lead with VALUE: include 1–2 Why Buys and/or model benefits up front.\n"
        "- If a valid SRP/VDP URL is supplied in the prompt, you may hyperlink the model name; "
        "otherwise do not add links yourself."
    )

def _personalization_rules_system():
    return (
        "Personalization:\n"
        "- If CRM includes an assigned salesperson, include their name on the first appointment mention."
    )

def _appointment_cta_system():
    return (
        "Scheduling CTA:\n"
        "- Do NOT include any scheduling link or scheduling sentence.\n"
        "- The system will append the correct dealership booking link automatically.\n"

    )

def _compliance_system():
    # Charity months logic: we keep the instruction authoritative; model should avoid outside windows.
    return (
        "Compliance & Brand Values:\n"
        "- Comply with California laws, ComplyAuto standards, and manufacturer ad guidelines.\n"
        "- Only reference charity campaigns during these windows:\n"
        "  March – Autism Speaks; May – Didi Hirsch Mental Health Awareness; July – OC Rescue Mission;\n"
        "  September – HomeAid / Ronald McDonald House; November – Autism Speaks (1st half), "
        "OC Rescue Mission & St. Jude (2nd half); December – OC Rescue Mission / Teen Risk.\n"
        "- If outside those months, do not mention charity at all.\n"
        "Sample charity phrasing when in-window:\n"
        "  “This month, a portion of every vehicle sold or serviced supports [Charity Name]. "
        "Thank you for helping us give back!”"
    )

def _links_and_boundaries_system():
    sites = "\n".join(f"- {u}" for u in PATTERSON_SITES)
    return (
        "Links & Boundaries:\n"
        f"- Only link to these dealership sites:\n{sites}\n"
        "- Hyperlink model names only if a valid SRP/VDP URL is provided in the prompt.\n"
        "- Focus on the dealership the lead originated from; do not promote other Patterson stores.\n"
        "- Do not mention specials/incentives unless clearly visible on the store’s own site.\n"
        "- Do not link to third-party sites (Autotrader, TrueCar, etc.)."
    )

def _objection_handling_system():
    return (
        "Objection Handling (core mindset and examples):\n"
        "- Always work to bring the guest in and overcome any objections or reasons not to follow through with the process.\n"
        "- Your tone stays warm, reassuring, and value-focused - never defensive or pushy.\n"
        "- Reframe concerns into a simple next step: a quick in-person visit/appraisal.\n"
        "- Confident reassurance: 'We’d love to see the car - sometimes we even pay more than the Kelley Blue Book® offer once we inspect it.'\n"
        "- Common playbook:\n"
        "  • 'Just looking': No pressure; invite a quick, no-obligation visit or appraisal.\n"
        "  • 'Better price/offer elsewhere': Congratulate them; explain we often pay more after an in-person look.\n"
        "  • 'Not ready / want to wait': Be patient; remind offers are time-limited but easy to reissue, and a quick visit clarifies value.\n"
        "  • 'Comparing dealers': Respectfully highlight Patterson transparency (no addendums/markups) and top workplace record.\n"
        "  • 'Unsure about value': Encourage a fast inspection — accuracy improves once we see the car.\n"
        "- Every reply should subtly move toward a visit while addressing the exact objection in 1–2 sentences."
    )



def _format_system():
    return (
        "Output JSON with keys exactly: subject, body. "
        "No markdown, no extra text. Example: {\"subject\":\"...\",\"body\":\"...\"}"
    )

def _retryable(status):
    return status in (429, 500, 502, 503, 504)

def _coerce_reply(text: str):
    """
    Try to parse assistant output as JSON with {subject, body}; else build a sane fallback.
    """
    try:
        data = json.loads(text)
        if isinstance(data, dict) and "subject" in data and "body" in data:
            return {"subject": str(data["subject"]).strip(), "body": str(data["body"]).strip()}
    except Exception:
        pass
    # Fallback: keep your downstream behavior (subject is normalized later in main.py)
    return {
        "subject": "Your vehicle inquiry with Patterson Auto Group",
        "body": text.strip() if text.strip() else "Thanks for reaching out — happy to help!"
    }

def _getCustomerMessagePrompts():
    return (
        'These rules will act as the “conversation flow brain” for Patti after the initial message has been sent, guiding how she replies to customers’ follow-ups.\n'
        'Your goal in this stage is to:\n'
        '1- "Understand the customer’s reply to your previous message."\n'
        '2- "Generate a natural, helpful response that continues the conversation appropriately."\n'
        '3- "Decide what action the system should take next (based on the following classification categories)."\n'
        'Classification Categories:\n'
        '{"Class": "availability_inquiry", "Description": "Customer asking if the vehicle is still available or listed.", "Example": "Is this car still available?"}\n'
        '{"Class": "purchase_intent", "Description": "Customer expresses urgency or readiness to buy soon.", "Example": "I can purchase it as soon as today."}\n'
        '{"Class": "specific_vehicle_request", "Description": "Customer mentions or requests a specific model,  color,  trim,  or features.", "Example": "Looking for a silver Sonata Hybrid Limited."}\n'
        '{"Class": "trade_in", "Description": "Customer talks about trading in or appraising their vehicle.", "Example": "Considering trading in my car."}\n'
        '{"Class": "price_quote_request", "Description": "Customer asks for pricing,  a quote,  or out-the-door cost.", "Example": "What’s your best out-the-door price?"}\n'
        '{"Class": "appointment_request", "Description": "Customer requests or mentions a test drive or visit appointment.", "Example": "Would love to test drive tomorrow morning."}\n'
        '{"Class": "contact_request", "Description": "Customer asks for or mentions a specific salesperson or contact person.", "Example": "Please have Joe contact me."}\n'
        '{"Class": "system_or_metadata", "Description": "Text contains only system data,  links,  or IDs (no customer message).", "Example": "8a9e2903-f09b-f011-814f-00505690ec8c.json"}\n'
        '{"Class": "others", "Description": "Message exists but does not fit any of the defined classes.", "Example": "Please send me more info."}\n'
        '4- "Write a short note summarizing what the customer wants (suitable to save in CRM for the sales team)."\n'
        '5- "Extract only the buyer’s intent or question — e.g. availability, test drive, preferences, etc."\n'
        '6- "If multiple buyer messages appear, combine them into one clean text string."\n'
        '7- "Write notes in short, neutral, professional language (one sentence max for each class)"\n'
        '8- "Do not use "others" or "None" class when there is another classes the message belongs to."\n'
        '9- "Always output valid JSON (no markdown or extra text)."\n'
        '10- there may be no response from the customer; there are rules for this situation that I will add; do not deal with it in this stage.\n'
        
        'Input Provided to You (You will always receive these ONLY after the initial message and all follow up messages):\n'
        'first inquery message sent by customer (may None)'
        'messages:\n'
        '{"msgFrom": "patti", "subject": "the subject of the first message sent by Patti", "body": "the body of the first message sent by Patti", "isFollowUp": false }\n'
        '{"msgFrom": "customer", "customerName": "", "subject": "the subject from customer", "body": "the body of the reply to patti message that sent by customer"}\n'
        'then patti replys and the customer replys and back and forth in the same struct.\n'
        'Output You Must Return:\n'
        '{"subject": "same as the customer subject (if not exist use patti ones)", "body": "the message Patti should send as a reply to the customer", "action": "high-level next step derived from classification categories (may multi ones)", "notes": "short CRM-style summary", "isFollowUp": "if it is a follow up true else false"}\n'
    )

def _getFollowUPRules():
    return (
        'These rules defines how you create follow-up messages when the customer has not replied to your previous outreach.\n'
        'DO NOT generate follow-up message unless I tell you to do that.\n'
        'Goal:\n'
        'Encourage the customer to re-engage, confirm interest, or provide an update, while keeping every message polite, natural, and helpful. Your goal is to sound like a real, attentive sales assistant who is genuinely trying to help, not a spam bot.\n'
        'output as the same json format that contains the "subject" and "body" and "isFollowUp" = True . \n'
        'Writing Guidelines:\n'
        '1- "Always personalize when possible (mention car model, previous interest, or name if provided)."\n'
        '2- "Keep it short: 2–4 sentences."\n'
        '3- "Use warm and natural tone."\n'
        '4- "End every message with a simple call to action (e.g., “Would you like me to check availability for you?”)."\n'
        '5- "Never sound robotic or aggressive — no repetition of same message wording."\n'
        '""\n'
        """
        Follow-Up Behavior Rules:
        {
            "stage": "Follow-up #1",
            "condition": "No reply after first message.",
            "goal": "Send gentle reminder and confirm interest",
            "message_style": "Friendly and conversational",
            "example_subject": "Still interested in the [Car Model]?",
            "example_cta": "Ask if they’re still considering or need details"
        },
        {
            "stage": "Follow-up #2",
            "condition": "No reply after first follow-up.",
            "goal": "Encourage engagement and offer help",
            "message_style": "Helpful and reassuring",
            "example_subject": "Happy to help with your car search",
            "example_cta": "Offer assistance or alternate options"
        },
        {
            "stage": "Follow-up #3",
            "condition": "No reply after second follow-up.",
            "goal": "Soft close and confirm if still interested",
            "message_style": "Polite and professional",
            "example_subject": "Should I keep your inquiry open?",
            "example_cta": "Invite them to respond or mark as inactive"
        }
        """
    )

def run_gpt(prompt: str,
            customer_name: str,
            rooftop_name: str = None,
            max_retries: int = MAX_RETRIES,
            prevMessages: bool = False,
            persona: str = "sales",
            kbb_ctx: dict | None = None):

    rooftop_addr = _get_rooftop_address(rooftop_name)

    addr_msg = None
    if rooftop_addr:
        addr_msg = {
            "role": "system",
            "content": f"Dealer address: {rooftop_addr}. If the guest asks where to go or for directions, include this exact address plainly (no brackets)."
        }
           
    # Build system stack (persona-aware)
    # For KBB ICO, we typically exclude generic follow-up rules because cadence uses templates.
                
    log.info("RUN_GPT debug: kbb_ctx keys=%s offer_amount=%r",
         list((kbb_ctx or {}).keys()),
         (kbb_ctx or {}).get("offer_amount_usd"))

    system_msgs = _build_system_stack(
        persona=persona,
        customer_first=customer_name,
        rooftop_name=rooftop_name,
        kbb_ctx=kbb_ctx,
        include_followup_rules=(persona != "kbb_ico")
    )
                
    if persona == "kbb_ico":
        joined = "\n---\n".join([m.get("content","") for m in system_msgs if m.get("role") == "system"])
        log.info("KBB SYSTEM STACK (trunc): %s", joined[:4000])


    # --- Tustin Kia new-location flavor (subscription c27d7f4f...) ---
    # That subscription maps to rooftop_name == "Tustin Kia" in rooftops.py.
    if rooftop_name and rooftop_name.strip().lower() == "tustin kia":
        system_msgs.append({
            "role": "system",
            "content": _tustin_kia_new_location_system(),
        })


    if addr_msg:
        system_msgs.insert(0, addr_msg)   # make sure the address is available to the model

    if prevMessages:
        messages = system_msgs + [
            {"role": "user", "content": prompt}
        ]

        import json, re
        dump = json.dumps(messages, ensure_ascii=False)
        log.info("RUN_GPT debug: kbb_ctx_in_messages=%s", "$27,000" in dump)
        log.info("RUN_GPT debug: messages_preview=%s", dump[:1500])

        
        model_used, resp = chat_complete_with_fallback(messages, want_json=True, temperature=0.6)
        text = _safe_extract_text(resp)
        if not text:
            log.warning("OpenAI returned empty content (model=%s). Using fallback template.", model_used)
        
        dictResult = getDictRes(text) or {"subject": "Re: your offer", "body": "Thanks for the note—happy to help."}

        placeholder_re = re.compile(r"(?i)\bthe subject (of|from)\b.*(patti|customer)")
        subj = (dictResult.get("subject") or "").strip()
        
        if not subj or placeholder_re.search(subj):
            # Use a strong default, especially for KBB persona
            fallback_rooftop = rooftop_name or "Patterson Auto Group"
            if persona == "kbb_ico":
                dictResult["subject"] = f"Kelley Blue Book® Instant Cash Offer | {fallback_rooftop}"
            else:
                dictResult["subject"] = f"Your vehicle inquiry with {fallback_rooftop}"
        
        # If we're replying, keep a single "Re:" prefix
        if not dictResult["subject"].lower().startswith("re:"):
            dictResult["subject"] = "Re: " + dictResult["subject"]

        return dictResult
        
    # --- non-prevMessages path ---

    messages = system_msgs + [
        {"role": "user", "content": prompt}
    ]

    import json, re
    dump = json.dumps(messages, ensure_ascii=False)
    log.info("RUN_GPT debug: kbb_ctx_in_messages=%s", "$27,000" in dump)
    log.info("RUN_GPT debug: messages_preview=%s", dump[:1500])

    model_used, resp = chat_complete_with_fallback(messages, want_json=True, temperature=0.6)
    text = _safe_extract_text(resp)
    if not text:
        log.warning("OpenAI returned empty content (model=%s). Using fallback template.", model_used)
    
    fallback_rooftop = rooftop_name or "Patterson Auto Group"
    default_subject = f"Your vehicle inquiry with {fallback_rooftop}"
    default_body_leadin = (
        f"Hi {customer_name or 'there'},\n\n"
        "Thanks for your inquiry! I’m happy to help with details, availability, and next steps. "
        "Let me know any preferences on trim, color, or timing and I’ll get everything lined up."
    )
    
    reply = _ensure_reply_dict(text, default_subject, default_body_leadin)
    if not reply or not reply.get("body"):
        log.warning("Model text (truncated): %r", (text or '')[:120])
    
    # --- Rooftop substitutions ---
    if rooftop_name:
        if reply.get("subject"):
            reply["subject"] = reply["subject"].replace("Patterson Auto Group", rooftop_name)
        if reply.get("body"):
            reply["body"] = reply["body"].replace("Patterson Auto Group", rooftop_name)
    
    # --- First-name personalization ---
    if customer_name and reply.get("body"):
        reply["body"] = (
            reply["body"]
            .replace("[Guest's Name]", customer_name)
            .replace("[Guest’s Name]", customer_name)
        )
    

    # --- Clean up any stray scheduling tokens/phrases; footer will handle CTA ---
    if rooftop_name and persona != "kbb_ico":
        body = (reply.get("body") or "")
        body = re.sub(r"(?im)^\s*schedule appointment\s*$", "", body)
        body = re.sub(r"(?i)<\{LegacySalesApptSchLink\}>", "", body)
        body = re.sub(r"(?im)^\s*looking forward to[^\n]*\n?", "", body)
        reply["body"] = body.rstrip()
    

    
    reply["messages"] = messages
    log.debug("OpenAI model_used=%s, chars=%d", model_used, len(text or ""))
    return reply



# those for first inquery if exist
def _getCustomerInqueryTextPrompts():
    return (
        'You are Patti, an AI assistant that reads CRM lead or chat transcript text and determines if there is an actual customer message (the buyer’s inquiry) inside.\n'
        'Your task is to:\n'
        '1- "analyze the text and detect if the text contains a real customer message (not system or dealer text)."\n'
        '2- "Extract that message clearly."\n'
        '3- "Classify it into one or more of the predefined categories separated by coma."\n'
        '4- "Write a short note summarizing what the customer wants (suitable to save in CRM for the sales team)."\n'
        '5-"if the text already have a converstion between the customer and the sales team add "salesAlreadyContact" = True"\n'
        'return only JSON output in this exact structure:\n'
        '{"customerMsg": "<the cleaned customer message>", "isCustomerMsg": true, "class": "<one or more of the predefined classes>", "notes": "<short CRM-style summary>", "salesAlreadyContact": False}\n'
        'If there is no real customer message, return:\n'
        '{"customerMsg": "", "isCustomerMsg": false, "class": "", "notes": "", "salesAlreadyContact": false}\n'
        'Classification Categories:\n'
        '{"Class": "availability_inquiry", "Description": "Customer asking if the vehicle is still available or listed.", "Example": "Is this car still available?"}\n'
        '{"Class": "purchase_intent", "Description": "Customer expresses urgency or readiness to buy soon.", "Example": "I can purchase it as soon as today."}\n'
        '{"Class": "specific_vehicle_request", "Description": "Customer mentions or requests a specific model,  color,  trim,  or features.", "Example": "Looking for a silver Sonata Hybrid Limited."}\n'
        '{"Class": "trade_in", "Description": "Customer talks about trading in or appraising their vehicle.", "Example": "Considering trading in my car."}\n'
        '{"Class": "price_quote_request", "Description": "Customer asks for pricing,  a quote,  or out-the-door cost.", "Example": "What’s your best out-the-door price?"}\n'
        '{"Class": "appointment_request", "Description": "Customer requests or mentions a test drive or visit appointment.", "Example": "Would love to test drive tomorrow morning."}\n'
        '{"Class": "contact_request", "Description": "Customer asks for or mentions a specific salesperson or contact person.", "Example": "Please have Joe contact me."}\n'
        '{"Class": "system_or_metadata", "Description": "Text contains only system data,  links,  or IDs (no customer message).", "Example": "8a9e2903-f09b-f011-814f-00505690ec8c.json"}\n'
        '{"Class": "others", "Description": "Message exists but does not fit any of the defined classes.", "Example": "Please send me more info."}\n'
        '{"Class": "None", "Description": "No customer message present (system-only or blank input).", "Example": ""}'
        'Rules:\n'
        '1- "Customer message = any text written by the buyer, not by the dealer, system, or CRM platform"\n'
        '2- Ignore:\n'
        '"CRM logs, links, timestamps, system messages, markup."\n'
        '"Dealer greetings, follow-ups, or signatures."\n'
        '3- "Extract only the buyer’s intent or question — e.g. availability, test drive, preferences, etc."\n'
        '4- "If multiple buyer messages appear, combine them into one clean text string."\n'
        '5- "Write notes in short, neutral, professional language (one sentence max for each class)"\n'
        '6- "If isCustomerMsg is false, leave both customerMsg and notes empty."\n'
        '7- "Do not use "others" or "None" class when there is another classes the message belongs to."\n'
        '8- "Always output valid JSON (no markdown or extra text)."\n'
        'IMPORTANT NOTE: if the text already have a converstion between the customer and the sales team make "salesAlreadyContact" == True \n'
        '\n'
        'Examples:\n'
        'input: Motivated Buyer: increased probability to purchase vehicle. <br /> *** DEALER PORTAL *** <br /> https://dealerportal.truecar.com/...\n'
        'output: {"customerMsg": "", "isCustomerMsg": false, "class": "", "notes": "", "salesAlreadyContact": false}\n'
        'input: Is this car still available?\n'
        'output: {"customerMsg": "Is this car still available?", "isCustomerMsg": true, "class": "availability_inquiry", "notes": "The customer is inquiring about the availability of a specific used vehicle.", "salesAlreadyContact": false}\n'
        'input: [11:16am] Tustin Mazda: Welcome to Tustin Mazda.[11:17am] Tustin Mazda: How may we help you today?[11:17am] Ashley Arias: Hi there, do you have any manual transmission mazda 3[11:17am] Tustin Mazda: Thank you. May I ask who I have the pleasure of speaking with?[11:17am] Ashley Arias: Ashley[11:17am] Tustin Mazda: One moment while I connect you with a member of our team.[11:17am] Fernando R: Hello Ashley, I am Fernando R. I will take just a moment to look into this for you.[11:18am] Ashley Arias: Thank you[11:18am] Fernando R: INVENTORY SENT - 2026 Mazda Mazda3 2.5 S Premium [M851143][11:18am] Fernando R: To confirm, this is the vehicle you are looking for?[11:18am] Ashley Arias: Manual right?[11:18am] Ashley Arias: You got anything a little bit cheaper?[11:19am] Fernando R: Yes, it is listed as a manual vehicle.[11:19am] Fernando R: Customer interested in pricing[11:19am] Fernando R: A member of our Sales Team would be able to answer any questions regarding vehicle pricing. I will forward your question and information over to them. What phone number and time of day would be best for them to reach out to you?[11:20am] Ashley Arias: 714-458-5919, they can call me at 12.[11:21am] Fernando R: Noted! What is the best email address to send that information to?[11:21am] Ashley Arias: ashley.arias07@gmail.com[11:21am] Fernando R: May I ask your last name as well?[11:22am] Ashley Arias: Arias[11:23am] Fernando R: I will forward your information over right away and a member of our team will be contacting you as soon as they are available. May I be of further assistance?[11:23am] Ashley Arias: that’ll be all, thank you![11:23am] Fernando R: Please feel free to reach out to us if we can be of any further assistance. Have a nice day and be safe.[11:33am] Fernando R: Customer is interested in pricing JM1BPAML5T1851143 - personal info provided - please call at 12PM\n'
        'output: {"customerMsg": "Hi there, do you have any manual transmission mazda 3? You got anything a little bit cheaper?", "isCustomerMsg": true, "class": "specific_vehicle_request, price_quote_request", "notes": "The customer is requesting information about manual transmission Mazda 3 options and inquiring about cheaper alternatives.", "salesAlreadyContact": true}\n'

    )

def getDictRes(gptAns):
    try:
        res = json.loads(gptAns)
        return res
    except:
        log.warning("OpenAI returned None JSON format content: %s", gptAns)
        print(gptAns)
        return None

def getCustomerMsgDict(inqueryTextBody):
    if not inqueryTextBody:
        return {"customerMsg": "", "isCustomerMsg": False, "class": "", "notes": ""}
    system_msgs = [
        {"role": "system", "content": _getCustomerInqueryTextPrompts()}
    ]

    messages = system_msgs + [
        {"role": "user", "content": inqueryTextBody}
    ]

    model_used, resp = chat_complete_with_fallback(messages, want_json=True, temperature=0.6)
    text = _safe_extract_text(resp)
    if not text:
        log.warning("OpenAI returned empty content (model=%s). Using fallback template.", model_used)
    
    dictResult = getDictRes(text)

    return dictResult

def extract_appt_time(text: str, tz: str = "America/Los_Angeles") -> dict:
    """
    Use GPT to extract one proposed appointment datetime from customer text.
    Returns: {"iso": "2025-11-06T15:00:00-08:00", "confidence": 0.0-1.0, "window": "exact|morning|afternoon|evening"}
    If nothing found, returns {"iso":"", "confidence":0, "window":""}
    """
    if not (text or "").strip():
        return {"iso": "", "confidence": 0, "window": ""}

    system = {
        "role": "system",
        "content": (
            "You extract one proposed meeting time from natural language. "
            "Assume the user and dealership are in the same time zone. "
            "Return JSON only: {\"iso\":\"<ISO8601 with timezone>\",\"confidence\":0-1,\"window\":\"exact|morning|afternoon|evening\"}. "
            "Use the given timezone. If vague like 'Wednesday morning', choose 10:00 in that timezone and set window accordingly. "
            "If 'next Wednesday', interpret as the next occurrence after today. "
            "If no date is present, return empty values."
        )
    }
    now_local = datetime.now().astimezone()  # used so model has 'today' concept implicitly
    user = {
        "role": "user",
        "content": f"Timezone: {tz}\nNow Local ISO: {now_local.isoformat()}\nText: {text}"
    }
    model_used, resp = chat_complete_with_fallback(
        [system, user],
        want_json=True,
        temperature=0.2
    )
    text_out = _safe_extract_text(resp)
    try:
        data = json.loads(text_out)
        iso = (data.get("iso") or "").strip()
        conf = float(data.get("confidence") or 0)
        window = (data.get("window") or "").strip()
        return {"iso": iso, "confidence": conf, "window": window}
    except Exception:
        return {"iso": "", "confidence": 0, "window": ""}


if __name__ == "__main__":
    # inqueryTextBody = "Finance For $361 Per month for 72 months + tax, $2,358.00 Downpayment , Comments:would love to test drive and hear more about it, IP Address: 75.80.117.116"
    # inqueryTextBody = "Motivated Buyer: increased probability to purchase vehicle. <br /> *** DEALER PORTAL *** <br /> https://dealerportal.truecar.com/dfe/prospects/J5NT6RDXP8?_xt=1&utm_source=crm&utm_medium=deeplink&utm_campaign=5113"
    # inqueryTextBody = "Is your Certified 2022 MAZDA CX-9 Grand Touring listed for $28,987 still available? If so, please give me a call so I can test drive tomorrow morning when you guys open.<br/> --------<br/>My Wallet/Offer Details:<br/>    Deal Type: finance,<br/>    Sale Price: $28987,<br/>    Down Payment: $9701.0,<br/>    Amount Financed: $0.0,<br/>    Credit Range: Very Good,<br/>    Terms: 60 months,<br/>    Payment: $1053,<br/>    Trade-In Value: $40000.0<br/>;<br/>  TCPAOptIn: false;<br/> -------- Copy and paste the following link into your browser to visit this listing: https://www.autotrader.com/cars-for-sale/vehicle/758671792"
    inqueryTextBody = "Preferred contact method: email\n\nThis lead contains customer provided telephone information. Recent changes to the Telephone Consumers Protection Act (TCPA) require a customer's prior express written consent prior to contacting the customer for marketing purposes by phone using automatic telephone dialing systems, SMS texts or artificial/prerecorded voice (each, an \"Unauthorized Contact Method\"). Please note that Mazda has not obtained such consent and you are required to obtain a customer's prior express written consent before contacting the customer through one of the Unauthorized Contact Methods."
    # inqueryTextBody = 
    # inqueryTextBody = 

    ans = getCustomerMsgDict(inqueryTextBody)
    print(ans)
    print(type(ans))

    # customer_name = "waleed"
    # salesperson = "Jim Feinstein"
    # vehicle_str = "2026 Mazda CX-90 3.3 Turbo S Premium Sport"

    # prompt = f"""
    #     Your job is to write personalized, dealership-branded emails from Patti, a friendly virtual assistant.

    #     When writing:
    #     - Begin with exactly `Hi {customer_name},`
    #     - Lead with value (features / Why Buy)
    #     - If a specific vehicle is mentioned, answer directly and link if possible
    #     - If a specific question exists, answer it first
    #     - Include the salesperson’s name
    #     - Keep it warm, clear, and human

    #     Info (may None):
    #     - salesperson’s name: {salesperson}
    #     - vehicle: {vehicle_str}


    #     Guest inquiry:
    #     \"\"\"{inqueryTextBody}\"\"\"

    #     Do not include any signature, dealership contact block, address, phone number, or URL in your reply; I will append it.
    #     """
    # print(run_gpt(prompt, customer_name))
