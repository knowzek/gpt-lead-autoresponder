# kbb_templates.py 
from pathlib import Path

TEMPLATES = {
    0: Path("/mnt/data/ICO CRM Template - Day 00 - TM.txt").read_text(encoding="utf-8"),
    1: Path("/mnt/data/ICO CRM Template - Day 01 - TM.txt").read_text(encoding="utf-8"),
    2: Path("/mnt/data/ICO CRM Template - Day 02 - TM.txt").read_text(encoding="utf-8"),
    # add more days 
}

def fill_merge_fields(html, ctx: dict) -> str:
    # replaces <{Field}> tokens appearing in your files
    out = html
    for k, v in ctx.items():
        out = out.replace(f"<{{{k}}}>", v or "")
        out = out.replace(f"<{{{k}:S}}>", v or "")  # handles :S variants in files
    return out
