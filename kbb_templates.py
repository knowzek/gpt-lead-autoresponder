# kbb_templates.py
import os
from pathlib import Path

# Base directory of this file (so it works in any environment)
BASE_DIR = Path(__file__).resolve().parent

# Folder where your text templates live
TEMPLATE_DIR = BASE_DIR / "kbb_templates"

# Helper function to load text safely
def _load_template(filename: str) -> str:
    path = TEMPLATE_DIR / filename
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        # Fail gracefully and log if needed
        print(f"⚠️ Missing KBB template: {filename}")
        return f"[TEMPLATE {filename} NOT FOUND]"

# Map day numbers to their corresponding files
TEMPLATES = {
    1:  _load_template("ICO CRM Template - Day 01 - TM.txt"),  # It's Time to Finalize Your Instant Cash Offer
    2:  _load_template("ICO CRM Template - Day 02 - TM.txt"),  # We'll Come to You to Finalize Your Instant Cash Offer
    5:  _load_template("ICO CRM Template - Day 05 - TM.txt"),  # Hurry! Your Instant Cash Offer Expires Soon
    6:  _load_template("ICO CRM Template - Day 06 - TM.txt"),  # There's Still Time to Finalize Your Instant Cash Offer
    7:  _load_template("ICO CRM Template - Day 07 - TM.txt"),  # Last Day to Finalize Your Instant Cash Offer
    8:  _load_template("ICO CRM Template - Day 08 - TM.txt"),  # Your Offer Expired, but We Still Want Your Vehicle!
    9:  _load_template("ICO CRM Template - Day 09 - TM.txt"),  # Get an Up-to-Date Instant Cash Offer Today
    12: _load_template("ICO CRM Template - Day 12 - TM.txt"),  # We'd Love to Have Your Feedback
    16: _load_template("ICO CRM Template - Day 16 - TM.txt"),  # Increased Demand is Driving Up Trade-In Values
    20: _load_template("ICO CRM Template - Day 20 - TM.txt"),  # Now is a Great Time to Trade-In or Sell Your Vehicle
    30: _load_template("ICO CRM Template - Day 30 - TM.txt"),  # Trade-In Values are Up!
    45: _load_template("ICO CRM Template - Day 45 - TM.txt"),  # We Need Quality Pre-Owned Vehicles Like Yours
    60: _load_template("ICO CRM Template - Day 60 - TM.txt"),  # You're Invited! Join Us for Our Vehicle BuyBack Event
    90: _load_template("ICO CRM Template - Day 90 - TM.txt"),  # Still Considering Trading-In or Selling Your Vehicle?
}

def fill_merge_fields(html: str, ctx: dict) -> str:
    """Replace <{Field}> tokens in the template with actual values."""
    out = html
    for k, v in ctx.items():
        v = str(v or "")
        out = out.replace(f"<{{{k}}}>", v)
        out = out.replace(f"<{{{k}:S}}>", v)
    return out
