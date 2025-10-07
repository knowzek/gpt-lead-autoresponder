# inventory_matcher.py
# Drop-in recommender for Patti: parse OT inventory XML, extract interest from email text,
# score vehicles, and format 1–2 recommendations.

import re
from datetime import datetime
from typing import List, Dict, Any, Optional
import xml.etree.ElementTree as ET

# ---------- 1) Minimal parser (same shape as earlier) ----------
_NS = {"t": "opentrack.dealertrack.com/transitional"}

def _ymd(s: str):
    s = (s or "").strip()
    if not s or s == "0":
        return None
    if len(s) == 8 and s.isdigit():
        try:
            return datetime.strptime(s, "%Y%m%d").date().isoformat()
        except Exception:
            return s
    try:
        return datetime.fromisoformat(s.replace("Z","")).date().isoformat()
    except Exception:
        return s

def parse_vehicle_inventory(xml_text: str) -> List[Dict[str, Any]]:
    root = ET.fromstring(xml_text)
    rows = []
    for r in root.findall(".//t:Result", _NS):
        def g(tag):
            el = r.find(f"t:{tag}", _NS)
            return (el.text or "").strip() if el is not None and el.text is not None else ""
        # optional fields (kept but not used by scorer)
        opt = []
        for of in r.findall("t:OptionalFields/t:VehicleOptionalField", _NS):
            def tx(name): 
                el = of.find(f"t:{name}", _NS); 
                return (el.text or "").strip() if el is not None and el.text else ""
            opt.append({
                "OptionNumber": tx("OptionNumber"),
                "Description": tx("Description"),
                "FieldType": tx("FieldType"),
                "AlphaFieldValue": tx("AlphaFieldValue"),
                "NumericFieldValue": tx("NumericFieldValue"),
                "DateFieldValue": tx("DateFieldValue"),
                "AddToCostFlag": tx("AddToCostFlag"),
            })
        opts = []
        for op in r.findall("t:Options/t:VehicleOption", _NS):
            def tx(name): 
                el = op.find(f"t:{name}", _NS); 
                return (el.text or "").strip() if el is not None and el.text else ""
            opts.append({"OptionCode": tx("OptionCode"), "Description": tx("Description")})
        rows.append({
            "CompanyNumber": g("CompanyNumber"),
            "VIN": g("VIN"),
            "StockNumber": g("StockNumber"),
            "Status": g("Status"),               # 'I' = in inventory
            "TypeNU": g("TypeNU"),               # 'N','U','T' (new/used/trade)
            "Year": g("ModelYear"),
            "Make": g("Make"),
            "Model": g("Model"),
            "Trim": g("Trim"),
            "BodyStyle": g("BodyStyle"),
            "Color": g("Color"),
            "FuelType": g("FuelType"),
            "Cylinders": g("Cylinders"),
            "Odometer": g("Odometer"),
            "DateInInventory": _ymd(g("DateInInventory")),
            "ListPrice": g("ListPrice"),
            "VehicleCost": g("VehicleCost"),
            "PublishToWeb": g("PublishVehicleInfoToWeb") in ("Y","y","1","true"),
            "OptionalFields": opt,
            "Options": opts,
        })
    return rows

# ---------- 2) Interest extraction from email text ----------
_MAKES = [
    # common makes; extend as needed
    "ACURA","ALFA ROMEO","AUDI","BMW","BUICK","CADILLAC","CHEVROLET","CHEVY","CHRYSLER","DODGE","FIAT","FORD",
    "GENESIS","GMC","HONDA","HYUNDAI","INFINITI","JAGUAR","JEEP","KIA","LAND ROVER","LEXUS","LINCOLN","MAZDA",
    "MERCEDES","MERCEDES-BENZ","MITSUBISHI","NISSAN","PORSCHE","RAM","SUBARU","TESLA","TOYOTA","VOLKSWAGEN","VW","VOLVO"
]
# Normalize some aliases
_MAKE_ALIAS = {
    "CHEVY": "CHEVROLET",
    "MERCEDES": "MERCEDES-BENZ",
    "VW": "VOLKSWAGEN",
    "LANDROVER": "LAND ROVER",
}

_BODY_SYNONYMS = {
    "SUV": {"SUV","CROSSOVER","CUV"},
    "TRUCK": {"TRUCK","PICKUP"},
    "SEDAN": {"SEDAN","SALoon"},  # add more if needed
    "COUPE": {"COUPE"},
    "VAN": {"VAN","MINIVAN"},
    "HATCHBACK": {"HATCH","HATCHBACK"},
}

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()

def _upper_clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").upper()).strip()

def _find_years(txt: str) -> List[int]:
    yrs = []
    for m in re.finditer(r"\b(20\d{2}|19\d{2})\b", txt):
        try:
            y = int(m.group(1))
            if 1980 <= y <= 2100:
                yrs.append(y)
        except:
            pass
    return yrs

def _find_price(txt: str) -> Optional[float]:
    # $30k, $30,000, under 25k, budget 18k, etc.
    m = re.search(r"\$\s*([\d,]+(?:\.\d{1,2})?)\s*[kK]?", txt)
    if m:
        raw = m.group(1).replace(",", "")
        try:
            return float(raw)
        except:
            return None
    m2 = re.search(r"\b(?:under|budget|max)\s+(\d{2,3})\s*[kK]\b", txt)
    if m2:
        return float(m2.group(1)) * 1000.0
    return None

def _find_make(txt_up: str) -> Optional[str]:
    for mk in _MAKES:
        key = _upper_clean(mk)
        if key in txt_up:
            return _MAKE_ALIAS.get(key, key)
    # try collapsing spaces for aliases like LANDROVER
    txt_collapsed = txt_up.replace(" ", "")
    for alias, norm in _MAKE_ALIAS.items():
        if alias == "LANDROVER" and "LANDROVER" in txt_collapsed:
            return norm
    return None

def _find_model(candidate: str, after_make_segment: str) -> Optional[str]:
    """
    Heuristic: take first 1–2 tokens after the make that look like a model (letters/numbers).
    """
    toks = [t for t in re.split(r"[^A-Z0-9]+", after_make_segment.upper()) if t]
    if toks:
        # Ignore common non-model words
        BAD = {"NEW","USED","CPO","CERTIFIED","LEASE","LEASED","HYBRID","PLUG","ELECTRIC","EV","SUV","TRUCK","SEDAN","COUPE"}
        for i, t in enumerate(toks[:3]):
            if t not in BAD and not t.isdigit():
                return t
    return None

def _find_body(txt_up: str) -> Optional[str]:
    for canonical, words in _BODY_SYNONYMS.items():
        for w in words:
            if _upper_clean(w) in txt_up:
                return canonical
    return None

def _find_type(txt_up: str) -> Optional[str]:
    # 'N' for new, 'U' for used. Prefer specific words.
    if re.search(r"\b(CPO|CERTIFIED PRE[- ]OWNED|CERTIFIED)\b", txt_up):
        return "U"
    if re.search(r"\bUSED|PRE[- ]OWNED\b", txt_up):
        return "U"
    if re.search(r"\bNEW\b", txt_up):
        return "N"
    return None

def extract_interest(email_text: str) -> Dict[str, Any]:
    txt_up = _upper_clean(email_text)
    yrs = _find_years(txt_up)
    year = yrs[0] if yrs else None
    mk = _find_make(txt_up)
    model = None
    if mk:
        # attempt to grab model after first mention of make
        idx = txt_up.find(mk)
        if idx >= 0:
            model = _find_model(mk, txt_up[idx+len(mk):])
    body = _find_body(txt_up)
    max_price = _find_price(email_text)
    t = _find_type(txt_up)
    return {
        "year": year,           # int or None
        "make": mk,             # "TOYOTA"
        "model": model,         # "RAV4"
        "body": body,           # "SUV"/"SEDAN"/...
        "typeNU": t,            # "N" or "U" or None
        "max_price": max_price  # float dollars or None
    }

# ---------- 3) Scoring against inventory ----------
def _price_to_float(v: str) -> Optional[float]:
    try:
        return float(v.replace(",", "")) if v else None
    except Exception:
        return None

def _year_to_int(v: str) -> Optional[int]:
    try:
        return int(v) if v else None
    except Exception:
        return None

def score_vehicle(v: Dict[str, Any], interest: Dict[str, Any]) -> float:
    score = 0.0

    # must be in stock
    if v.get("Status") != "I":
        return -1.0

    # publishable gets a tiny bump (optional)
    if v.get("PublishToWeb"):
        score += 0.25

    make = _upper_clean(v.get("Make"))
    model = _upper_clean(v.get("Model"))
    body = _upper_clean(v.get("BodyStyle"))
    year = _year_to_int(v.get("Year"))
    price = _price_to_float(v.get("ListPrice"))

    # Hard preferences
    if interest.get("typeNU") in ("N","U"):
        score += 1.0 if v.get("TypeNU") == interest["typeNU"] else 0.0

    if interest.get("make"):
        score += 3.0 if make == interest["make"] else 0.0

    if interest.get("model"):
        # exact match = strong, token contains = medium
        if model == interest["model"]:
            score += 3.0
        elif interest["model"] and interest["model"] in model:
            score += 1.5

    if interest.get("body"):
        score += 1.0 if interest["body"] == body else 0.0

    # Year closeness (±2 years ok)
    if interest.get("year") and year:
        diff = abs(year - interest["year"])
        if diff == 0:
            score += 1.5
        elif diff == 1:
            score += 1.0
        elif diff == 2:
            score += 0.5
        else:
            score -= min(1.0, diff * 0.25)

    # Budget filter (don’t penalize missing price)
    if interest.get("max_price") and price:
        if price <= interest["max_price"]:
            score += 1.0
        else:
            score -= 0.5

    return score

def recommend_inventory(rows: List[Dict[str, Any]], email_text: str, k: int = 2) -> List[Dict[str, Any]]:
    interest = extract_interest(email_text)
    # Prefer in-stock only
    scored = [(score_vehicle(v, interest), v) for v in rows]
    # drop filtered/negative scores
    scored = [x for x in scored if x[0] >= 0.0]
    scored.sort(key=lambda x: x[0], reverse=True)
    recs = [v for _, v in scored[:k]]
    return recs

# ---------- 4) Patti-friendly formatter ----------
def format_recommendations(email_text: str, recs: List[Dict[str, Any]]) -> str:
    if not recs:
        return "I didn’t find a close in-stock match, but I can pull similar options if you share your preferred year, trim, or budget."
    lines = []
    opener = "Here are a couple in-stock options I think you’ll like:"
    if len(recs) == 1:
        opener = "Here’s an in-stock option I think you’ll like:"
    lines.append(opener)
    for r in recs:
        yr, mk, md = (r.get("Year") or "").strip(), (r.get("Make") or "").title(), (r.get("Model") or "").upper()
        trim = (r.get("Trim") or "").title()
        stock = r.get("StockNumber") or ""
        vin = r.get("VIN") or ""
        body = (r.get("BodyStyle") or "").upper()
        price = r.get("ListPrice")
        price_str = f"${price}" if price else "Call for price"
        bits = [b for b in [yr, mk, md, trim] if b]
        headline = " ".join(bits) or f"{mk} {md}".strip()
        details = f"{body}" if body else ""
        lines.append(f"• {headline} — {price_str} (Stock {stock}, VIN {vin}){f' — {details}' if details else ''}")
    lines.append("Would you like photos or a walk-around video?")
    return "\n".join(lines)

# ---------- 5) End-to-end helper ----------
def recommend_from_xml(xml_text: str, customer_email_text: str, k: int = 2) -> str:
    inv = parse_vehicle_inventory(xml_text)
    recs = recommend_inventory(inv, customer_email_text, k=k)
    return format_recommendations(customer_email_text, recs)
