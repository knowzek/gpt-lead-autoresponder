# kbb_cadence.py (new)

CADENCE = {
    1:  {"email_template_day": 1,  "subject": "It's Time to Finalize Your Instant Cash Offer", "create_phone_task": False, "create_text_task": False},
    2:  {"email_template_day": 2,  "subject": "Let's Finalize Your KBB Instant Cash Offer", "create_phone_task": False, "create_text_task": False},
    5:  {"email_template_day": 5,  "subject": "Hurry! Your Instant Cash Offer Expires Soon", "create_phone_task": False, "create_text_task": False},
    6:  {"email_template_day": 6,  "subject": "There's Still Time to Finalize Your Instant Cash Offer", "create_phone_task": False, "create_text_task": False},
    7:  {"email_template_day": 7,  "subject": "Last Day to Finalize Your Instant Cash Offer", "create_phone_task": False, "create_text_task": False},
    8:  {"email_template_day": 8,  "subject": "Your Offer Expired, but We Still Want Your Vehicle!", "create_phone_task": False, "create_text_task": False},
    9:  {"email_template_day": 9,  "subject": "Get an Up-to-Date Instant Cash Offer Today", "create_phone_task": False, "create_text_task": False},
    12: {"email_template_day": 12, "subject": "We'd Love to Have Your Feedback", "create_phone_task": False, "create_text_task": False},
    16: {"email_template_day": 16, "subject": "Increased Demand is Driving Up Trade-In Values", "create_phone_task": False, "create_text_task": False},
    20: {"email_template_day": 20, "subject": "Now is a Great Time to Trade-In or Sell Your Vehicle", "create_phone_task": False, "create_text_task": False},
    30: {"email_template_day": 30, "subject": "Trade-In Values are Up!", "create_phone_task": False, "create_text_task": False},
    44: {"email_template_day": 45, "subject": "We Need Quality Pre-Owned Vehicles Like Yours", "create_phone_task": False, "create_text_task": False},
    60: {"email_template_day": 60, "subject": "You're Invited! Join Us for Our Vehicle BuyBack Event", "create_phone_task": False, "create_text_task": False},
    90: {"email_template_day": 90, "subject": "Still Considering Trading-In or Selling Your Vehicle?", "create_phone_task": False, "create_text_task": False},
}

def events_for_day(day: int):
    """Return the cadence definition (email template, subject, and task flags) for a given day."""
    return CADENCE.get(day)
