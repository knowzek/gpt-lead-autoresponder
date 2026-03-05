# web_app.py (or wherever your Flask routes live)

from flask import request, jsonify
from datetime import datetime as _dt
import logging

log = logging.getLogger(__name__)

@app.route("/lead-notification-inbound", methods=["POST"])
def lead_notification_inbound():
    inbound = request.get_json(force=True, silent=True) or {}

    # --- NEW: apply routing rules in Python ---
    try:
        from lead_router import route_inbound_lead
        routed = route_inbound_lead(inbound)

        # Only override if missing (keeps backward compatibility with any PA branches that already set it)
        if not (inbound.get("source") or "").strip() or inbound.get("source") == "carfax":
            inbound["source"] = routed.source

        if routed.lead_type and not (inbound.get("lead_type") or "").strip():
            inbound["lead_type"] = routed.lead_type

        log.info("ROUTER: from=%r subject=%r -> source=%r lead_type=%r",
                 inbound.get("from"), (inbound.get("subject") or "")[:120],
                 inbound.get("source"), inbound.get("lead_type"))

    except Exception as e:
        log.exception("ROUTER failed (continuing without override): %s", e)

    # Keep your existing debug
    bt = inbound.get("body_text") or ""
    bh = inbound.get("body_html") or ""
    log.info("PA PAYLOAD DEBUG body_text_head=%r", bt[:300])
    log.info("PA PAYLOAD DEBUG body_html_head=%r", bh[:300])

    from email_ingestion import process_lead_notification
    process_lead_notification(inbound)
    return jsonify({"ok": True}), 200
