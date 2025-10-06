import os, time, json, logging
from datetime import datetime
from openai import OpenAI
from openai import APIStatusError  # available in recent SDKs; if import fails, just catch Exception
OPENAI_CHAT_MODEL = os.getenv("OPENAI_CHAT_MODEL", "gpt-5.1-mini")
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

client = OpenAI(timeout=CLIENT_TIMEOUT)

# --- Patti system instruction builders --------------------------------

def _patti_persona_system():
    return (
        "You are Patti, the virtual assistant for Patterson Auto Group "
        "(Tustin Mazda, Huntington Beach Mazda, Mission Viejo Kia, Tustin Hyundai). "
        "Your tone matches our best team members: warm, professional, helpful, and never pushy."
    )

def _patti_rules_system(customer_first: str):
    return (
        "Objectives:\n"
        "- Start a natural conversation with a strong, value-forward opening.\n"
        "- Always reflect Patterson brand values and Why Buys.\n"
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
        "- When inviting a guest to book, include this phrasing verbatim (without a URL):\n"
        "  You can also reserve your time instantly here: Schedule Your Visit\n"
        "- Place it after proposed times or as an alternative. "
        "The system will append the actual scheduling link token later."
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
        "Objection Handling (tone & approach examples; adapt concisely):\n"
        "- “Just looking”: No pressure, offer a casual visit/test drive.\n"
        "- “Found a better price”: Acknowledge; emphasize No Addendums & transparency; invite a review visit.\n"
        "- “Email me numbers”: Offer what you can, suggest a quick visit/call to tailor the quote.\n"
        "- “Not ready to buy”: Supportive; suggest a no-commitment visit to learn more.\n"
        "- “Comparing brands/dealers”: Respectful; highlight Why Buys; offer a specific visit time."
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

def run_gpt(prompt: str,
            customer_name: str,
            rooftop_name: str = None,
            max_retries: int = MAX_RETRIES):

    # Build system stack
    system_msgs = [
        {"role": "system", "content": _patti_persona_system()},
        {"role": "system", "content": _patti_rules_system(customer_name)},
        {"role": "system", "content": _patterson_why_buys_system()},
        {"role": "system", "content": _first_message_rules_system()},
        {"role": "system", "content": _personalization_rules_system()},
        {"role": "system", "content": _appointment_cta_system()},
        {"role": "system", "content": _compliance_system()},
        {"role": "system", "content": f"Current month: {CURRENT_MONTH}. Only reference charity campaigns if this month is listed; otherwise do not mention charity at all."},
        {"role": "system", "content": _links_and_boundaries_system()},
        {"role": "system", "content": _objection_handling_system()},
        {"role": "system", "content": _format_system()},
    ]

    messages = system_msgs + [
        {"role": "user", "content": prompt}
    ]

    # Single chat completion call
    resp = client.chat.completions.create(
        model=OPENAI_CHAT_MODEL,
        messages=messages,
        temperature=0.6,
        response_format={"type": "json_object"}  # if your SDK supports it; remove otherwise
    )
    text = (resp.choices[0].message.content or "").strip()

    # Parse JSON into your canonical reply structure
    try:
        data = json.loads(text)
        reply = {"subject": data.get("subject","").strip(),
                 "body":    data.get("body","").strip()}
    except Exception:
        reply = _coerce_reply(text)  # fallback to your existing parser

        # --- Rooftop substitutions (kept from your original) ---
        if rooftop_name:
            if reply.get("subject"):
                reply["subject"] = reply["subject"].replace("Patterson Auto Group", rooftop_name)
            if reply.get("body"):
                reply["body"] = reply["body"].replace("Patterson Auto Group", rooftop_name)
    
        # --- First-name personalization (kept from your original) ---
        if customer_name and reply.get("body"):
            reply["body"] = (
                reply["body"]
                .replace("[Guest's Name]", customer_name)
                .replace("[Guest’s Name]", customer_name)
            )
    
        # --- Append dynamic schedule link + closing signature ---
        if rooftop_name:
            schedule_line = (
                "Please let us know a convenient time for you, or you can instantly "
                "reserve your time here: <{LegacySalesApptSchLink}>."
            )
            closing_line = "Looking forward to assisting you further."
    
            signature_lines = [
                "", schedule_line, closing_line, "",
                "Patti",
                rooftop_name,
            ]
    
            if rooftop_address:
                signature_lines.append(rooftop_address)
    
            # Remove stray “Schedule Your Visit” text the model might have produced
            body = (reply.get("body") or "")
            body = re.sub(r"Schedule Your Visit\.?", "", body, flags=re.I)
    
            reply["body"] = body.rstrip() + "\n\n" + "\n".join(signature_lines)
    
        return reply
