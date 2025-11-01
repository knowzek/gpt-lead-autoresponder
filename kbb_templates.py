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
    0: _load_template("ICO CRM Template - Day 00 - TM.txt"),
    1: _load_template("ICO CRM Template - Day 01 - TM.txt"),
    2: _load_template("ICO CRM Template - Day 02 - TM.txt"),
    # add more days here later
}

def fill_merge_fields(html: str, ctx: dict) -> str:
    """Replace <{Field}> tokens in the template with actual values."""
    out = html
    for k, v in ctx.items():
        v = str(v or "")
        out = out.replace(f"<{{{k}}}>", v)
        out = out.replace(f"<{{{k}:S}}>", v)
    return out
