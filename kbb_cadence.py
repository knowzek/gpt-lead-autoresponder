# kbb_cadence.py (new)

CADENCE = {
  0: {"email_template_day": 0, "subject": "We've Received Your Request for an Instant Cash Offer!", "create_phone_task": True, "create_text_task": False},
  1: {"email_template_day": 1, "subject": "It's Time to Finalize Your Instant Cash Offer", "create_phone_task": True, "create_text_task": True},
  2: {"email_template_day": 2, "subject": "We'll Come to You to Finalize Your Instant Cash Offer", "create_phone_task": False, "create_text_task": True},
  # Add: 4,5,6,7,8,11,15,19,29,44,59,89 etc. per the PDF
}

def events_for_day(day):
    # later: include "phone" and "text" tasks if texting is enabled
    return CADENCE.get(day)
