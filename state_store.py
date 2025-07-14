# state_store.py
import os
import json
from pathlib import Path

STATE_FILE = Path("processed_leads.json")


def _load_state():
    if STATE_FILE.exists():
        with open(STATE_FILE, "r") as f:
            return set(json.load(f))
    return set()


def _save_state(processed):
    with open(STATE_FILE, "w") as f:
        json.dump(list(processed), f)


def was_processed(activity_id):
    processed = _load_state()
    return activity_id in processed


def mark_processed(activity_id):
    processed = _load_state()
    processed.add(activity_id)
    _save_state(processed)
