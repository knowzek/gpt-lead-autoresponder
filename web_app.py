# web_app.py
import logging
import json
from datetime import datetime as _dt
from flask import Flask, request, jsonify

from email_ingestion import process_inbound_email
from kbb_adf_ingestion import process_kbb_adf_notification
from sms_ingestion import process_inbound_sms

log = logging.getLogger("patti.web")
app = Flask(__name__)

def _looks_like_kbb(inbound: dict) -> bool:
    """
    Conservative KBB detector. If this returns True, we DO NOT want this
    internet-leads service touching Airtable at all.
    """
    subj = (inbound.get("subject") or "").lower()
    frm = (inbound.get("from") or "").lower()
    body = ((inbound.get("body_text") or "") + " " + (inbound.get("body_html") or "")).lower()

    # Subject/body keywords commonly found in KBB ICO / offer emails
    kbb_keywords = [
        "kbb", "kelley blue book", "instant cash offer", "offer alert",
        "autotrader-tradein", "tradein@",
    ]

    # If any strong signal hits, treat as KBB.
    for kw in kbb_keywords:
        if kw in subj or kw in frm or kw in body:
            return True

    return False


# -----------------------------
#   KBB ADF Inbound Endpoint
# -----------------------------
@app.route("/kbb-adf-inbound", methods=["POST"])
def kbb_adf_inbound():
    """
    Entry point for 'Offer Created from Kelley Blue Book' emails
    hitting Patti's inbox via Power Automate.
    """
    try:
        payload = request.get_json(force=True) or {}

        inbound = {
            "from": payload.get("from"),
            "subject": payload.get("subject"),
            "body_html": payload.get("body_html") or "",
            "body_text": payload.get("body_text") or "",
            "timestamp": payload.get("timestamp") or _dt.utcnow().isoformat(),
            "headers": payload.get("headers") or {},
        }

        log.info("📩 KBB ADF inbound: from=%s subject=%s", inbound["from"], inbound["subject"])

        process_kbb_adf_notification(inbound)

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        log.exception("KBB ADF ingestion failed: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/kbb-email-inbound", methods=["POST"])
def kbb_email_inbound():
    try:
        payload = request.get_json(force=True) or {}

        inbound = {
            # ✅ pass-through fields your downstream expects
            "subscription_id": payload.get("subscription_id") or payload.get("subscriptionId"),
            "source": payload.get("source") or "reply",

            "conversation_id": payload.get("conversation_id") or payload.get("conversationId"),
            "message_id": payload.get("message_id") or payload.get("messageId"),

            "test_mode": payload.get("test_mode"),
            "test_email": payload.get("test_email"),

            # existing fields
            "from": payload.get("from"),
            "to": payload.get("to"),
            "cc": payload.get("cc"),
            "subject": (payload.get("subject") or "").strip(),
            "body_html": payload.get("body_html") or "",
            "body_text": payload.get("body_text") or "",
            "timestamp": payload.get("timestamp") or _dt.utcnow().isoformat(),
            "headers": payload.get("headers") or {},
        }

        log.info(
            "📥 KBB email inbound: from=%s subject=%s sub_id=%s source=%s",
            inbound["from"], inbound["subject"], inbound.get("subscription_id"), inbound.get("source")
        )

        process_inbound_email(inbound)

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        log.exception("KBB email ingestion failed: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500

# -----------------------------
#   Health Check
# -----------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

@app.route("/lead-notification-inbound", methods=["POST"])
def lead_notification_inbound():
    inbound = request.get_json(force=True) or {}
    from email_ingestion import process_lead_notification
    process_lead_notification(inbound)
    return jsonify({"ok": True}), 200


# -----------------------------
#   Standard Email Inbound
# -----------------------------
@app.route("/email-inbound", methods=["POST"])
def email_inbound():
    try:
        payload = request.get_json(force=True) or {}

        inbound = {
            # pass-through keys that help routing + reply threading
            "source": payload.get("source"),
            "subscription_id": payload.get("subscription_id") or payload.get("subscriptionId"),
            "conversation_id": payload.get("conversation_id"),
            "message_id": payload.get("message_id"),
            "test_mode": payload.get("test_mode"),
            "test_email": payload.get("test_email"),

            # email content
            "from": payload.get("from"),
            "to": payload.get("to"),
            "cc": payload.get("cc"),
            "subject": (payload.get("subject") or "").strip(),
            "body_html": payload.get("body_html") or "",
            "body_text": payload.get("body_text") or "",
            "timestamp": payload.get("timestamp") or _dt.utcnow().isoformat(),

            # combine any payload headers with actual HTTP headers
            "headers": {
                **(payload.get("headers") or {}),
                **{k: v for k, v in request.headers.items()},
            },
        }

        log.info("📥 Incoming email: from=%s subject=%s", inbound["from"], inbound["subject"])

        if _looks_like_kbb(inbound):
            log.warning("🛑 KBB detected on /email-inbound. Ignoring. from=%s subject=%s",
                        inbound["from"], inbound["subject"])
            return jsonify({"status": "ignored", "reason": "kbb_routed_elsewhere"}), 200

        log.info(
            "📦 inbound keys=%s resolved_sub=%s from=%s subject=%s body_text_len=%s to=%s",
            list(payload.keys()),
            inbound.get("subscription_id"),
            inbound.get("from"),
            inbound.get("subject"),
            len(inbound.get("body_text") or ""),
            inbound.get("to"),
        )

        process_inbound_email(inbound)
        return jsonify({"status": "ok"}), 200

    except Exception as e:
        log.exception("Email ingestion failed: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/sms-inbound", methods=["POST"])
def sms_inbound():
    """
    Webhook endpoint called by GoTo for inbound SMS.
    For now: log raw payload, apply simple rules, reply immediately.
    """
    try:
        payload_json = request.get_json(silent=True) or {}
        raw_text = ""
        try:
            raw_text = (request.data or b"").decode("utf-8", errors="ignore")
        except Exception:
            raw_text = ""

        log.info("📥 Incoming SMS webhook")
        out = process_inbound_sms(payload_json, raw_text=raw_text)
        return jsonify(out), 200

    except Exception as e:
        log.exception("SMS ingestion failed: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500



# -----------------------------
#   Local Run
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
