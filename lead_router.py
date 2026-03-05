# lead_router.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any


@dataclass(frozen=True)
class RoutedLead:
    source: str
    lead_type: Optional[str] = None


def _s(val: Any) -> str:
    return (val or "").strip()


def route_inbound_lead(payload: Dict[str, Any]) -> RoutedLead:
    """
    Replicates the Power Automate routing rules in Python so PA can keep
    sending the same payload to the same endpoint.
    """
    from_raw = _s(payload.get("from")).lower()
    subject = _s(payload.get("subject"))
    subject_l = subject.lower()

    # --- Carfax ---
    # PA: equals from == NoReplyLead@carfax.com
    if from_raw == "noreplylead@carfax.com":
        return RoutedLead(source="carfax")

    # --- Cars.com ---
    # PA: subject contains Cars.com New/Used Car Lead for Tustin Kia
    if "cars.com new car lead" in subject_l or "cars.com used car lead" in subject_l:
        return RoutedLead(source="cars.com")

    # --- CarGurus ---
    # PA: from contains dealer-leads@messages.cargurus.com
    if "dealer-leads@messages.cargurus.com" in from_raw or "cargurus" in from_raw:
        return RoutedLead(source="cargurus")

    # --- CarNOW / TrueCar ---
    # PA: from contains adf-no-reply@carnow.com OR truecarmail.com
    if "truecarmail.com" in from_raw:
        return RoutedLead(source="truecar")
    if "adf-no-reply@carnow.com" in from_raw or "carnow.com" in from_raw:
        return RoutedLead(source="carNOW")

    # --- Apollo Special Leads (Team Velocity - Pre-Qual / Value Your Trade) ---
    # PA: subject contains:
    #  - Apollo Website Lead-Pre-Qual VDP
    #  - Apollo Website Lead-Trade - Value Your Trade
    if "apollo website lead-pre-qual vdp".lower() in subject_l:
        return RoutedLead(source="Team Velocity - Pre-Qualification", lead_type="pre_qual")
    if "apollo website lead-trade - value your trade".lower() in subject_l:
        return RoutedLead(source="Team Velocity - Pre-Qualification", lead_type="value_your_trade")

    # --- Apollo (general website lead variants) ---
    # PA: subject contains multiple Apollo Website Lead-* strings
    apollo_markers = [
        "Apollo Website Lead-Contact Dealer",
        "Apollo Website Lead-Contact Us - Vehicle",
        "Apollo Website Lead-Check Availability",
        "Apollo Website Lead-Transact - Contact Us",
        "Apollo Website Lead-Contact Us - Admin",
    ]
    if any(m.lower() in subject_l for m in apollo_markers):
        return RoutedLead(source="Apollo")

    # Fallback: keep whatever PA sent, or unknown
    existing = _s(payload.get("source"))
    if existing:
        return RoutedLead(source=existing, lead_type=_s(payload.get("lead_type")) or None)

    return RoutedLead(source="unknown")
