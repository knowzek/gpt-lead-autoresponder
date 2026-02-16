# ----------------------------------------
# Mazda Loyalty Sprint - SMS Cadence
# ----------------------------------------

from datetime import datetime


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
