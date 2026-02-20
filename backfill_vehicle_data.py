"""
backfill_vehicle_data.py
========================
One-time idempotent backfill script that populates Airtable vehicle fields
(Year, Make, Model, Trim, Vin, stockNumber) from the Fortellis Opportunity API
for historical records that are missing vehicle data.

Selection Criteria:
  - opp_id exists on the record
  - Any of {Year, Make, Model} is missing or empty

Behavior:
  - For each matching record, calls Fortellis GET /sales/v2/elead/opportunities/{opp_id}
  - Applies deterministic vehicle selection logic (select_vehicle_from_sought)
  - Populates Airtable vehicle fields via PATCH
  - Records with ALL vehicle fields already populated are skipped (idempotent)
  - Failures are logged and skipped; script continues on partial failures

Usage:
  DRY_RUN=1 python backfill_vehicle_data.py   # preview only (no writes)
  DRY_RUN=0 python backfill_vehicle_data.py   # actual backfill

Environment variables required:
  AIRTABLE_API_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME (or default "Leads"),
  FORTELLIS_CLIENT_ID, FORTELLIS_CLIENT_SECRET
"""

import os
import sys
import time
import logging
from dotenv import load_dotenv

load_dotenv()

LOG_LEVEL = os.getenv("APP_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [backfill] %(message)s",
)
log = logging.getLogger("backfill_vehicle")

DRY_RUN = os.getenv("DRY_RUN", "1").strip().lower() in ("1", "true", "yes")

# ── Airtable config ──────────────────────────────────────────────────
AIRTABLE_API_TOKEN = os.getenv("AIRTABLE_API_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE = os.getenv("AIRTABLE_TABLE_NAME", "Leads")

if not AIRTABLE_API_TOKEN or not AIRTABLE_BASE_ID:
    log.error("Missing AIRTABLE_API_TOKEN or AIRTABLE_BASE_ID")
    sys.exit(1)

AIRTABLE_BASE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"
AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_TOKEN}",
    "Content-Type": "application/json",
}

import requests

from fortellis import (
    get_token,
    fetch_and_select_vehicle,
)


def _airtable_request(method: str, url: str, **kwargs):
    """Thin wrapper around requests for Airtable API calls."""
    r = requests.request(method, url, headers=AIRTABLE_HEADERS, timeout=30, **kwargs)
    if r.status_code == 429:
        # Airtable rate limit: wait and retry once
        retry_after = int(r.headers.get("Retry-After", 30))
        log.warning("Airtable rate limited. Waiting %ds...", retry_after)
        time.sleep(retry_after)
        r = requests.request(method, url, headers=AIRTABLE_HEADERS, timeout=30, **kwargs)
    if r.status_code >= 400:
        raise RuntimeError(
            f"Airtable {method} failed {r.status_code}: {r.text[:800]}"
        )
    return r.json()


def fetch_records_missing_vehicle() -> list[dict]:
    """
    Fetch all Airtable records where:
      - opp_id exists AND
      - any of {Year, Make, Model} is missing/empty
    Uses Airtable's filterByFormula with pagination.
    """
    formula = (
        "AND("
        "  {opp_id} != '',"
        "  OR("
        "    {Year} = '',"
        "    {Make} = '',"
        "    {Model} = '',"
        "    {Year} = BLANK(),"
        "    {Make} = BLANK(),"
        "    {Model} = BLANK()"
        "  )"
        ")"
    )

    all_records = []
    offset = None

    while True:
        params = {
            "filterByFormula": formula,
            "pageSize": 100,
            "fields[]": [
                "opp_id",
                "subscription_id",
                "year",
                "make",
                "model",
                "trim",
                "vin",
                "stockNumber",
            ],
        }
        if offset:
            params["offset"] = offset

        data = _airtable_request("GET", AIRTABLE_BASE_URL, params=params)
        records = data.get("records") or []
        all_records.extend(records)

        offset = data.get("offset")
        if not offset:
            break

        # Airtable rate limit: ~5 requests/second
        time.sleep(0.25)

    return all_records


def _has_all_vehicle_fields(fields: dict) -> bool:
    """Check if a record already has all core vehicle fields populated."""
    return bool(
        (fields.get("year") or "").strip()
        and (fields.get("make") or "").strip()
        and (fields.get("model") or "").strip()
    )


def backfill():
    """Main backfill entry point."""
    log.info("=" * 60)
    log.info("Vehicle data backfill starting (DRY_RUN=%s)", DRY_RUN)
    log.info("=" * 60)

    # Fetch candidate records
    records = fetch_records_missing_vehicle()
    log.info("Found %d records missing vehicle data", len(records))

    if not records:
        log.info("Nothing to backfill. Exiting.")
        return

    # Group by subscription_id to minimize token refreshes
    by_sub: dict[str, list[dict]] = {}
    skipped_no_sub = 0
    for rec in records:
        fields = rec.get("fields") or {}
        sub_id = (fields.get("subscription_id") or "").strip()
        if not sub_id:
            skipped_no_sub += 1
            continue
        by_sub.setdefault(sub_id, []).append(rec)

    if skipped_no_sub:
        log.warning("Skipped %d records with no subscription_id", skipped_no_sub)

    total = sum(len(v) for v in by_sub.values())
    processed = 0
    enriched = 0
    skipped = 0
    failed = 0

    for sub_id, sub_records in by_sub.items():
        log.info(
            "Processing subscription %s (%d records)", sub_id, len(sub_records)
        )

        try:
            token = get_token(sub_id)
        except Exception as e:
            log.error(
                "Failed to get token for subscription %s: %s — skipping %d records",
                sub_id,
                e,
                len(sub_records),
            )
            failed += len(sub_records)
            continue

        for rec in sub_records:
            rec_id = rec.get("id")
            fields = rec.get("fields") or {}
            opp_id = (fields.get("opp_id") or "").strip()
            processed += 1

            if not opp_id:
                log.warning("Record %s has empty opp_id, skipping", rec_id)
                skipped += 1
                continue

            # Idempotency: skip if already populated (double-check)
            if _has_all_vehicle_fields(fields):
                log.info(
                    "Record %s (opp=%s) already has vehicle data, skipping",
                    rec_id,
                    opp_id,
                )
                skipped += 1
                continue

            # Fetch vehicle data from Fortellis
            try:
                vehicle_fields = fetch_and_select_vehicle(opp_id, token, sub_id)
            except Exception as e:
                log.warning(
                    "Fortellis fetch failed opp=%s rec=%s: %s", opp_id, rec_id, e
                )
                failed += 1
                continue

            # Check if we got any useful data
            has_any = any(v for v in vehicle_fields.values())
            if not has_any:
                log.info(
                    "No vehicle data from Fortellis for opp=%s rec=%s", opp_id, rec_id
                )
                skipped += 1
                continue

            # Only write fields that are currently empty (don't overwrite existing)
            patch_fields = {}
            for key in ("year", "make", "model", "trim", "vin", "stockNumber"):
                existing_val = (fields.get(key) or "").strip()
                new_val = (vehicle_fields.get(key) or "").strip()
                if not existing_val and new_val:
                    patch_fields[key] = new_val

            if not patch_fields:
                log.info(
                    "No new fields to write for opp=%s rec=%s (all existing fields preserved)",
                    opp_id,
                    rec_id,
                )
                skipped += 1
                continue

            if DRY_RUN:
                log.info(
                    "[DRY_RUN] Would patch rec=%s opp=%s with %s",
                    rec_id,
                    opp_id,
                    patch_fields,
                )
                enriched += 1
            else:
                try:
                    _airtable_request(
                        "PATCH",
                        f"{AIRTABLE_BASE_URL}/{rec_id}",
                        json={"fields": patch_fields},
                    )
                    log.info(
                        "Enriched rec=%s opp=%s fields=%s", rec_id, opp_id, patch_fields
                    )
                    enriched += 1
                except Exception as e:
                    log.warning(
                        "Airtable PATCH failed rec=%s opp=%s: %s", rec_id, opp_id, e
                    )
                    failed += 1

            # Airtable rate limit: ~5 requests/second
            time.sleep(0.25)

    log.info("=" * 60)
    log.info("Backfill complete.")
    log.info("  Total records found:   %d", len(records))
    log.info("  Processed:             %d", processed)
    log.info("  Enriched:              %d", enriched)
    log.info("  Skipped (idempotent):  %d", skipped)
    log.info("  Failed:                %d", failed)
    log.info("  DRY_RUN:               %s", DRY_RUN)
    log.info("=" * 60)


if __name__ == "__main__":
    backfill()
