# ----------------------------------------
# Mazda Loyalty Sprint - SMS Cadence
# ----------------------------------------

from datetime import datetime
from patti_common import build_patti_footer

def build_mazda_loyalty_email(*, day: int, fields: dict) -> dict:
    """
    Deterministic Mazda Loyalty email nudges aligned to SMS cadence days.
    Returns: {"subject": str, "body_text": str, "body_html": str}

    Uses Airtable field 'bucket' to select copy set:
      - "2+ Loyalists ($1,000)" => highest tier copy ($1,000)
      - "Brand Advocates ($500)" => advocate copy ($500)
    """

    # -------------------------
    # Identity + segmentation
    # -------------------------
    first_name = (fields.get("customer_first_name")
                  or fields.get("first_name")
                  or "").strip()

    greet = f"Hi {first_name}," if first_name else "Hi there,"

    bucket_raw = (fields.get("bucket") or "").strip()
    bucket_lower = bucket_raw.lower()

    # Top tier if bucket starts with "2+" (matches your Airtable option),
    # OR contains "1,000" as a safe fallback.
    is_top_tier = bucket_raw.startswith("2+") or ("1,000" in bucket_lower) or ("1000" in bucket_lower)

    # -------------------------
    # Template sets
    # -------------------------
    TOP_TIER = {
        1: {
            "subject": "[Mazda Loyalty] Your highest-tier CX-5 loyalty reward",
            "body": (
                "{greet}\n\n"
                "You’re receiving the highest tier of Mazda’s Loyalty Reward — a $1,000 voucher toward a new CX-5.\n\n"
                "This level is reserved for our 2+ Mazda loyalists, and I just wanted to confirm you’ve received your 16-digit voucher code.\n\n"
                "If you have it handy, reply with the code and I’ll verify eligibility and make sure everything is applied correctly.\n\n"
                "If you haven’t seen it yet, I can help track it down for you.\n\n"
                "Looking forward to helping you take advantage of it,\n"
            ),
        },
        2: {
            "subject": "[Mazda Loyalty] Your $1,000 reward stacks with current CX-5 offers",
            "body": (
                "{greet}\n\n"
                "A quick note — your $1,000 loyalty reward can be stacked with current CX-5 incentives.\n\n"
                "You’re not choosing between offers — your highest-tier reward works alongside current programs.\n\n"
                "If there’s a trim or color you’ve had your eye on, let me know and I’ll send options that qualify.\n\n"
                "Happy to make this seamless for you.\n"
            ),
        },
        3: {
            "subject": "[Mazda Loyalty] You can transfer your $1,000 reward",
            "body": (
                "{greet}\n\n"
                "If you’re not planning to use your $1,000 loyalty reward yourself, it is fully transferable.\n\n"
                "Many of our 2+ loyalists choose to gift it to a family member or close friend.\n\n"
                "If someone comes to mind, just send me their name and best contact info and I’ll personally take care of everything.\n\n"
                "I just don’t want your highest-tier benefit to go unused.\n"
            ),
        },
        4: {
            "subject": "[Mazda Loyalty] The CX-5 is especially strong right now",
            "body": (
                "{greet}\n\n"
                "We’ve seen a strong response to the latest CX-5 — especially from our top-tier loyalty customers.\n\n"
                "Between the upgraded interior, enhanced tech, and your $1,000 loyalty reward, it’s one of the strongest overall value positions in the lineup right now.\n\n"
                "If you'd like, I can send available inventory that qualifies or set up a quick test drive around your schedule.\n\n"
                "Would weekday or weekend work better?\n"
            ),
        },
        5: {
            "subject": "[Mazda Loyalty] Don’t let your $1,000 reward expire",
            "body": (
                "{greet}\n\n"
                "Just a final reminder — your $1,000 loyalty reward won’t remain active indefinitely.\n\n"
                "Whether you use it personally or transfer it to someone close to you, I’d be happy to help you take advantage of it before it expires.\n\n"
                "Reply here and I’ll take care of the details.\n"
            ),
        },
    }

    ADVOCATE = {
        1: {
            "subject": "[Mazda Loyalty] Your CX-5 loyalty reward",
            "body": (
                "{greet}\n\n"
                "As a valued Mazda Brand Advocate, you’ve been issued a $500 loyalty voucher toward a new CX-5.\n\n"
                "I just wanted to confirm you’ve received your 16-digit voucher code.\n\n"
                "If you have it handy, reply with the code and I’ll verify everything for you.\n\n"
                "If you haven’t seen it yet, I can help track it down.\n"
            ),
        },
        2: {
            "subject": "[Mazda Loyalty] Your $500 reward stacks with CX-5 offers",
            "body": (
                "{greet}\n\n"
                "Just a quick reminder — your $500 loyalty reward can be combined with current CX-5 incentives.\n\n"
                "It’s designed to enhance the value you’re already receiving.\n\n"
                "If you’d like, tell me what you’re considering and I’ll send qualifying options.\n"
            ),
        },
        3: {
            "subject": "[Mazda Loyalty] You can gift your $500 reward",
            "body": (
                "{greet}\n\n"
                "If you’re not planning to use your $500 loyalty reward yourself, it’s fully transferable.\n\n"
                "If someone comes to mind, just send their name and contact info and I’ll take care of everything.\n\n"
                "No pressure — just making sure it doesn’t go unused.\n"
            ),
        },
        4: {
            "subject": "[Mazda Loyalty] A quick CX-5 update",
            "body": (
                "{greet}\n\n"
                "The latest CX-5 updates have been getting strong feedback — especially paired with loyalty incentives like yours.\n\n"
                "If you’d like to explore inventory or schedule a test drive, I can coordinate that around your schedule.\n\n"
                "Let me know what works best.\n"
            ),
        },
        5: {
            "subject": "[Mazda Loyalty] Use your $500 reward before it expires",
            "body": (
                "{greet}\n\n"
                "Just a quick reminder — your $500 loyalty reward won’t be available indefinitely.\n\n"
                "If you’d like to use it or transfer it to someone close to you, I’m happy to assist.\n\n"
                "Reply here and I’ll handle the details.\n"
            ),
        },
    }

    templates = TOP_TIER if is_top_tier else ADVOCATE
    # Normalize day (fallback to day 5 copy for anything beyond 5)
    d = int(day or 1)
    if d not in templates:
        d = 5

    subject = templates[d]["subject"]
    body = templates[d]["body"].format(greet=greet)

    body_text = body.strip()

    # -------------------------
    # HTML wrapper + Patti footer (unchanged behavior)
    # -------------------------
    html_main = "<br>".join((body_text or "").split("\n"))
    body_html = (
        "<div style='font-family:Arial, Helvetica, sans-serif; font-size:14px; line-height:20px; color:#222;'>"
        f"{html_main}"
        "</div>"
    )

    rooftop_name = (fields.get("rooftop_name") or fields.get("rooftop") or "").strip()
    footer_html = build_patti_footer(rooftop_name=rooftop_name)
    body_html = body_html + footer_html

    return {
        "subject": subject,
        "body_text": body_text,
        "body_html": body_html,
    }

def build_mazda_loyalty_sms(*, day: int, fields: dict) -> str:
    """
    Deterministic SMS nudges for Mazda Loyalty Sprint.
    This is outbound cadence only (not GPT conversational replies).

    Args:
        day: current sms_day (int)
        fields: Airtable record fields (dict)

    Returns:
        SMS body (str)
    """

    first_name = (fields.get("customer_first_name")
                  or fields.get("first_name")
                  or "").strip()

    bucket = (fields.get("bucket") or "").lower()

    if "1,000" in bucket or "2+" in bucket:
        incentive_text = "$1,000 Mazda Loyalty Reward"
    else:
        incentive_text = "$500 Mazda Loyalty Reward"

    name_prefix = f"{first_name}, " if first_name else ""

    # -------------------------
    # Day-based deterministic sequence
    # -------------------------

    if day == 1:
        return (
            f"{name_prefix}this is Patti from Patterson Autos. Quick question - did you receive your {incentive_text} "
            f"voucher for the new CX-5? If you have your 16-digit code handy, "
            f"I can confirm eligibility for you."
        )

    if day == 2:
        return (
            f"{name_prefix}Patti at Patterson Autos again - just a reminder your {incentive_text} can be used "
            f"on a new CX-5 and is stackable with current specials. "
            f"Want me to check available inventory?"
        )

    if day == 3:
        return (
            f"{name_prefix}this is Patti with Patterson Autos again. I just wanted to let you know if you're not planning to use your "
            f"{incentive_text}, you can gift it to a friend or family member. "
            f"Just send me their name and number and I’ll take care of it."
        )

    if day == 4:
        return (
            f"{name_prefix}we’ve had a strong response to the CX-5 loyalty program. "
            f"If you’d like to test drive one or run numbers, I can set that up quickly."
        )

    if day == 5:
        return (
            f"{name_prefix}final reminder - your {incentive_text} won’t be available forever. "
            f"Let me know if you'd like to use it or gift it before it expires."
        )

    # Fallback after Day 5 (soft close)
    return (
        f"{name_prefix}just checking in one last time regarding your "
        f"{incentive_text}. If you'd like help using it or transferring it, "
        f"I'm here to assist."
    )
