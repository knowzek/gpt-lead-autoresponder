# web_app.py
import logging
from datetime import datetime as _dt
from flask import Flask, request, jsonify

from email_ingestion import process_inbound_email  # the handler we wrote

log = logging.getLogger("patti.web")
app = Flask(__name__)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/email-inbound", methods=["POST"])
def email_inbound():
    """
    Webhook endpoint called by Power Automate whenever a new email arrives.
    """
    try:
        payload = request.get_json(force=True) or {}

        inbound = {
            "from": payload.get("from"),
            "to": payload.get("to"),
            "cc": payload.get("cc"),
            "subject": (payload.get("subject") or "").strip(),
            "body_html": payload.get("body_html") or "",
            "body_text": payload.get("body_text") or "",
            "timestamp": payload.get("timestamp") or _dt.utcnow().isoformat(),
            "headers": payload.get("headers") or {},
        }

        log.info("📥 Incoming email: from=%s subject=%s", inbound["from"], inbound["subject"])
        process_inbound_email(inbound)

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        log.exception("Email ingestion failed: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    # For local testing only; Render will use gunicorn
    app.run(host="0.0.0.0", port=5000, debug=True)
