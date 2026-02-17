# ----------------------------------------
# Mazda Loyalty Sprint - SMS Cadence
# ----------------------------------------

from datetime import datetime
from patti_common import build_patti_footer

def build_mazda_loyalty_email(*, day: int, fields: dict) -> dict:
    """
    Deterministic Mazda Loyalty email nudges aligned to SMS cadence days.
    Returns: {"subject": str, "body_text": str, "body_html": str}
    """

    first_name = (fields.get("customer_first_name")
                  or fields.get("first_name")
                  or "").strip()

    bucket = (fields.get("bucket") or "").lower()
    if "1,000" in bucket or "2+" in bucket:
        incentive = "$1,000 Mazda Loyalty Reward"
    else:
        incentive = "$500 Mazda Loyalty Reward"

    greet = f"Hi {first_name}," if first_name else "Hi there,"

    # ---- Day templates ----
    if day == 1:
        subject = f"Quick question about your {incentive} voucher"
        body = (
            f"{greet}\n\n"
            f"Did you receive your {incentive} voucher for the new CX-5?\n"
            f"If you send me your 16-digit code, I can confirm eligibility and help you use it.\n\n"
            f"Reply with the code when you have a moment.\n"
        )

    elif day == 2:
        subject = f"Your {incentive} can be stacked with current CX-5 offers"
        body = (
            f"{greet}\n\n"
            f"Just a reminder: your {incentive} can be used on a new CX-5 and is stackable with current specials.\n"
            f"If you’d like, tell me what color/trim you prefer and I’ll check what’s available.\n"
        )

    elif day == 3:
        subject = f"You can gift your {incentive} to a friend or family member"
        body = (
            f"{greet}\n\n"
            f"If you’re not planning to use your {incentive}, you can gift it to a friend or family member.\n"
            f"Send me their name and best phone number/email and we’ll take care of the rest.\n"
        )

    elif day == 4:
        subject = "Want to take a quick look at CX-5 options?"
        body = (
            f"{greet}\n\n"
            f"We’ve had a strong response to the CX-5 loyalty program.\n"
            f"If you want, I can line up a quick test drive or share a few options that match what you’re looking for.\n"
            f"Would weekday or weekend be better?\n"
        )

    elif day == 5:
        subject = f"Final reminder: don’t miss your {incentive}"
        body = (
            f"{greet}\n\n"
            f"Final reminder — your {incentive} won’t be available forever.\n"
            f"If you’d like help using it (or gifting it), just reply and I’ll make it easy.\n"
        )

    else:
        subject = f"Checking in on your {incentive}"
        body = (
            f"{greet}\n\n"
            f"Just checking in regarding your {incentive}.\n"
            f"If you’d like help using it or transferring it, reply here and we’ll assist.\n"
        )

    body_text = body.strip()

    # basic HTML wrapper
    rooftop_name = (fields.get("rooftop_name") or fields.get("rooftop") or "").strip()

    footer_html = build_patti_footer(rooftop_name=rooftop_name)
    
    body_html = (
        "<div style='font-family:Arial, Helvetica, sans-serif; font-size:14px; line-height:20px; color:#222;'>"
        f"{body_html_core}"
        "</div>"
        f"{footer_html}"
    )


    return {
        "subject": subject,
        "body_text": body_text,
        "body_html": f"<div style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.5'>{body_html}</div>"
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
            f"{name_prefix}quick question — did you receive your {incentive_text} "
            f"voucher for the new CX-5? If you have your 16-digit code handy, "
            f"I can confirm eligibility for you."
        )

    if day == 2:
        return (
            f"{name_prefix}just a reminder your {incentive_text} can be used "
            f"on a new CX-5 and is stackable with current specials. "
            f"Want me to check available inventory?"
        )

    if day == 3:
        return (
            f"{name_prefix}also — if you're not planning to use your "
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
            f"{name_prefix}final reminder — your {incentive_text} won’t be available forever. "
            f"Let me know if you'd like to use it or gift it before it expires."
        )

    # Fallback after Day 5 (soft close)
    return (
        f"{name_prefix}just checking in one last time regarding your "
        f"{incentive_text}. If you'd like help using it or transferring it, "
        f"I'm here to assist."
    )
