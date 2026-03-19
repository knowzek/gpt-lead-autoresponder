# ----------------------------------------
# Mazda Loyalty Sprint - SAFE Email + SMS Cadence
# ----------------------------------------

from datetime import datetime
from patti_common import build_patti_footer


def build_mazda_loyalty_email(*, day: int, fields: dict) -> dict:
    """
    Deterministic Mazda Loyalty email nudges aligned to SMS cadence days.
    Safer version:
    - no dollar amounts in subject/body except $100 Service & Parts credit
    - no guaranteed eligibility language
    - no "stackable with current specials"
    - no "verify eligibility"
    - frames the Mazda loyalty communication as conditional / review-based
    """

    first_name = (fields.get("customer_first_name") or fields.get("first_name") or "").strip()
    greet = f"Hi {first_name}," if first_name else "Hi there,"

    rooftop_name = (fields.get("rooftop_name") or fields.get("rooftop") or "").strip() or "our dealership"

    EMAIL_TEMPLATES = {
        1: {
            "subject": "[Mazda Loyalty] Quick question about your CX-5 loyalty email",
            "body": (
                "{greet}\n\n"
                "I wanted to reach out in case you received a Mazda Loyalty email regarding the CX-5.\n\n"
                "If you have your 16-digit code, you can reply with it here and I can have our team review it with you.\n\n"
                "If you have not seen the email yet, I can point you in the right direction for where to look.\n"
            ),
        },
        2: {
            "subject": "[Mazda Loyalty] I can help review your CX-5 loyalty options",
            "body": (
                "{greet}\n\n"
                "Just checking in in case you wanted help reviewing your Mazda Loyalty offer for the CX-5.\n\n"
                "If you already have your 16-digit code, send it over and our team can take a look.\n\n"
                "If you are thinking about a specific trim or color, I can also help send available CX-5 options.\n"
            ),
        },
        3: {
            "subject": "[Mazda Loyalty] Another option if you are not planning to use it",
            "body": (
                "{greet}\n\n"
                "If you are not planning to use your Mazda Loyalty offer yourself, there may still be other options available.\n\n"
                "In some cases, it may be transferable to a family member or friend, or it may be usable as a $100 Service & Parts credit at {rooftop_name} in exchange for the loyalty code.\n\n"
                "If you would like help with either option, just reply here and I can help.\n"
            ),
        },
        4: {
            "subject": "[Mazda Loyalty] Want me to send available CX-5 options?",
            "body": (
                "{greet}\n\n"
                "If you are considering a CX-5, I’d be happy to help make the next step easy.\n\n"
                "I can send available inventory, help answer general questions, or have someone from the team follow up with you directly.\n\n"
                "If you want to move forward, just reply with your 16-digit code or let me know what kind of CX-5 you are interested in.\n"
            ),
        },
        5: {
            "subject": "[Mazda Loyalty] Last check-in from Patti",
            "body": (
                "{greet}\n\n"
                "I just wanted to check in one last time regarding the Mazda Loyalty CX-5 email.\n\n"
                "If you would like help reviewing it, transferring it if allowed, or using it toward the $100 Service & Parts credit at {rooftop_name}, I’m happy to help.\n\n"
                "If you’d prefer a team member to reach out directly, reply here and I’ll make sure that happens.\n"
            ),
        },
    }

    d = int(day or 1)
    if d not in EMAIL_TEMPLATES:
        d = 5

    subject = EMAIL_TEMPLATES[d]["subject"]
    body = EMAIL_TEMPLATES[d]["body"].format(greet=greet, rooftop_name=rooftop_name)
    body_text = body.strip()

    html_main = "<br>".join((body_text or "").split("\n"))
    body_html = (
        "<div style='font-family:Arial, Helvetica, sans-serif; font-size:14px; line-height:20px; color:#222;'>"
        f"{html_main}"
        "</div>"
    )

    footer_html = build_patti_footer(rooftop_name=rooftop_name)
    body_html = body_html + footer_html

    return {
        "subject": subject,
        "body_text": body_text,
        "body_html": body_html,
    }


def build_mazda_loyalty_sms(*, day: int, fields: dict) -> str:
    """
    Deterministic outbound SMS nudges for Mazda Loyalty Sprint.
    Safer version:
    - no $500 / $1,000 language
    - no "stackable with specials"
    - no "confirm eligibility"
    - no "reward payout" framing
    - keeps Patti in helper / router mode
    """

    first_name = (fields.get("customer_first_name") or fields.get("first_name") or "").strip()
    rooftop_name = (fields.get("rooftop_name") or fields.get("rooftop") or "").strip() or "Patterson Autos"

    name_prefix = f"{first_name}, " if first_name else ""

    if day == 1:
        return (
            f"{name_prefix}this is Patti from {rooftop_name}. "
            f"Quick question — did you receive a Mazda Loyalty email about the CX-5? "
            f"If you have your 16-digit code, text it here and I can help get it reviewed."
        )

    if day == 2:
        return (
            f"{name_prefix}Patti here from {rooftop_name} again. "
            f"If you received the Mazda Loyalty CX-5 email and want help reviewing your options, "
            f"I’m happy to help. Want me to check available inventory?"
        )

    if day == 3:
        return (
            f"{name_prefix}just a heads-up — if you are not planning to use the Mazda Loyalty offer yourself, "
            f"there may be other options available, including transfer or a $100 Service & Parts credit in exchange for the loyalty code. "
            f"Want help with that?"
        )

    if day == 4:
        return (
            f"{name_prefix}if you’re considering a CX-5, I can help with next steps. "
            f"I can send available inventory or have a team member reach out directly. "
            f"Would you like me to do that?"
        )

    if day == 5:
        return (
            f"{name_prefix}last check-in from Patti on the Mazda Loyalty CX-5 email. "
            f"If you want help reviewing it, using the service credit option, or connecting with someone on our team, "
            f"just reply here."
        )

    return (
        f"{name_prefix}just checking in one last time on the Mazda Loyalty CX-5 email. "
        f"If you’d like help reviewing it or exploring your options, I’m here to help."
    )

def build_event_email(day: int, fields: dict):
    first = fields.get("first_name", "")
    store = fields.get("store", "")
    rsvp = fields.get("rsvp_url", "")

    if day == 1:
        subject = "Be among the first to drive the all-new CX-5 at Patterson Autos"

        body_html = f"""
        <p>Hi {first},</p>

        <p>We’re excited to invite you to our <strong>CX-5 Launch Event</strong> at <strong>{store}</strong>.</p>

        <p><strong>Saturday March 21<br>
        9:00 AM – Noon</strong></p>

        <p>Stop by anytime to:</p>

        <ul>
        <li>See the all-new redesigned CX-5</li>
        <li>Take a drive</li>
        <li>Enjoy Chick-fil-A breakfast and lunch</li>
        <li>Pick up a few surprise gifts</li>
        </ul>

        <p>This is a relaxed drop-in event, but if you plan to attend it helps us to know.</p>

        <p><a href="{rsvp}">RSVP here</a></p>
        """

    elif day == 2:
        subject = "Reminder: CX-5 Launch Event at Patterson Autos"

        body_html = f"""
        <p>Hi {first},</p>

        <p>Just a reminder about our CX-5 Launch Event at {store}.</p>

        <p><strong>Saturday March 21 | 9:00 AM – Noon</strong></p>

        <p>Stop by anytime to see and drive the new CX-5.</p>

        <p><a href="{rsvp}">RSVP if you might attend</a></p>
        """

    else:
        subject = "Tomorrow: CX-5 Launch Event at Patterson Autos"

        body_html = f"""
        <p>Hi {first},</p>

        <p>Our CX-5 Launch Event is tomorrow morning at {store}.</p>

        <p>Stop by anytime between 9:00 AM and Noon.</p>

        <p><a href="{rsvp}">Let us know if you're coming</a></p>
        """

    return {
        "subject": subject,
        "body_html": body_html
    }


def build_event_sms(day: int, fields: dict):
    first = fields.get("first_name", "")
    rsvp = fields.get("rsvp_url", "")
    store = fields.get("store", "")

    if day == 1:
        return f"""
Hi {first}, this is Patti from {store}. You're invited to our CX-5 Launch Event at {store} on March 21st (9-Noon)!

Stop by to see and drive the new CX-5.

RSVP so we know you're coming or let me know if you have any questions:
{rsvp}
Reply STOP to Opt Out
"""

    elif day == 2:
        return f"""
Reminder: CX-5 Launch Event March 21 at {store}.

Food and test drives will be ready.

RSVP if you might stop by:
{rsvp}
-Reply STOP to Opt Out
"""

    elif day == 3:
        return f"""
Tomorrow: CX-5 Launch Event at {store}.

Stop by anytime between 9-Noon.

RSVP here:
{rsvp}
-Reply STOP to Opt Out
"""

    else:
        return f"""
Today's the CX-5 Launch Event at {store}!

We're here from 9-Noon with food and vehicles ready to drive.
"""
