# web_app.py
import logging
import json
import re
from datetime import datetime as _dt
from flask import Flask, request, jsonify
import os
import threading

from email_ingestion import process_inbound_email, process_lead_notification
from kbb_adf_ingestion import process_kbb_adf_notification
from sms_ingestion import process_inbound_sms
from sms_poller import send_sms_cadence_once
from lead_router import detect_lead_source

log = logging.getLogger("patti.web")
app = Flask(__name__)


KBB_RULES = [
    # strong phrases (safe anywhere)
    ("kelley_blue_book", re.compile(r"(?i)kelley\s+blue\s+book")),
    ("instant_cash_offer", re.compile(r"(?i)instant\s+cash\s+offer")),
    ("offer_alert", re.compile(r"(?i)\boffer\s+alert\b")),

    # if you insist on "kbb", require word boundaries
    ("kbb_word", re.compile(r"(?i)\bkbb\b")),
]

def _looks_like_kbb(inbound: dict) -> bool:
    subj = (inbound.get("subject") or "")
    frm  = (inbound.get("from") or "")
    # ⚠️ Do NOT scan full HTML. If you must, strip URLs first (see Fix #2).
    body_text = (inbound.get("body_text") or "")

    haystacks = {
        "subject": subj,
        "from": frm,
        "body_text": body_text,
    }

    for name, rx in KBB_RULES:
        for where, txt in haystacks.items():
            m = rx.search(txt)
            if m:
                snip = txt[max(0, m.start()-60): m.end()+60].replace("\n","\\n").replace("\r","\\r")
                log.warning("🛑 KBB detect hit rule=%s in=%s snip=%r", name, where, snip)
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
    """
    Central inbound endpoint for ALL rooftop mailbox flows.
    Power Automate will POST *every* inbound email here (no vendor filtering in PA).
    This endpoint will classify vendor/lead_source and:
      - call process_lead_notification() if it’s a recognized lead
      - otherwise return handled=false (ignored)
    """
    try:
        payload = request.get_json(force=True, silent=False) or {}

        inbound = {
            # Required for Fortellis routing
            "subscription_id": (payload.get("subscription_id") or payload.get("subscriptionId") or "").strip(),

            # Optional, but keep for logging/analytics if you want it
            "rooftop_code": (payload.get("rooftop_code") or payload.get("rooftopCode") or "").strip(),

            # You already send this today from PA (sometimes hardcoded like "cars.com", "apollo", etc.)
            "source": (payload.get("source") or "").strip(),

            # Core email fields
            "timestamp": payload.get("timestamp"),
            "from": payload.get("from"),
            "to": payload.get("to"),
            "cc": payload.get("cc"),
            "subject": (payload.get("subject") or "").strip(),
            "body_html": payload.get("body_html") or "",
            "body_text": payload.get("body_text") or "",

            # Message identifiers (you already send these in your Compose)
            "conversation_id": payload.get("conversation_id") or payload.get("conversationId") or "",
            "message_id": payload.get("message_id") or payload.get("messageId") or "",

            # Optional: some flows send lead_type
            "lead_type": payload.get("lead_type") or payload.get("leadType") or "",

            # Optional toggles
            "test_mode": bool(payload.get("test_mode")),

            # Keep headers if present (useful for debugging/dedupe later)
            "headers": payload.get("headers") or {},
        }

        if not inbound["subscription_id"]:
            return jsonify({
                "status": "error",
                "message": "subscription_id is required",
            }), 400

        # Central vendor detection (based on your PA rules)
        detected = detect_lead_source(inbound)
        inbound["lead_source"] = detected or ""

        # Central vendor detection (based on your PA rules)
        detected = detect_lead_source(inbound)
        inbound["lead_source"] = detected or ""
        
        # ✅ NEW: overwrite inbound["source"] so your downstream uses the normalized value
        if detected:
            inbound["source"] = detected
        
        # ✅ NEW: if Apollo special, set lead_type (matches your PA behavior)
        if detected == "Team Velocity - Pre-Qualification" and not inbound.get("lead_type"):
            from lead_router import detect_lead_type
            inbound["lead_type"] = detect_lead_type(inbound)

        log.info(
            "📥 lead-notification-inbound: sub_id=%s rooftop=%s detected=%r pa_source=%r from=%r subject=%r msg_id=%r",
            inbound.get("subscription_id"),
            inbound.get("rooftop_code"),
            detected,
            inbound.get("source"),
            (inbound.get("from") or ""),
            (inbound.get("subject") or "")[:160],
            inbound.get("message_id"),
        )

        # If it doesn’t match your rules, ignore (non-lead email)
        if not detected:
            return jsonify({
                "status": "ok",
                "handled": False,
                "ignored": True,
                "reason": "no_rule_match",
            }), 200

        # If it matched, route to your existing lead handler
        # (This preserves your current system behavior.)
        process_lead_notification(inbound)

        return jsonify({
            "status": "ok",
            "handled": True,
            "lead_source": detected,
        }), 200

    except Exception as e:
        log.exception("lead-notification-inbound failed: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500


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

        # Keep your existing KBB guard exactly as-is
        if _looks_like_kbb(inbound):
            log.warning(
                "🛑 KBB detected on /email-inbound. Ignoring. from=%s subject=%s",
                inbound["from"],
                inbound["subject"],
            )
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

        # --- ASYNC: respond fast to Power Automate, process in background ---
        import threading

        def _worker(snapshot: dict):
            try:
                process_inbound_email(snapshot)
            except Exception:
                log.exception("email_inbound worker failed")

        threading.Thread(target=_worker, args=(inbound,), daemon=True).start()

        # Return immediately so PA never hits the ~120s timeout
        return jsonify({"status": "accepted"}), 200

    except Exception as e:
        log.exception("Email ingestion failed: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/sms-inbound", methods=["POST"])
def sms_inbound():
    """
    Webhook endpoint called by GoTo for inbound SMS.
    Mazda service: route directly to Mazda Loyalty handler (Airtable + GPT + handoff).
    """
    try:
        payload_json = request.get_json(silent=True) or {}
        log.info("📥 Incoming SMS webhook")

        from sms_poller import handle_mazda_loyalty_inbound_sms_webhook
        out = handle_mazda_loyalty_inbound_sms_webhook(payload_json=payload_json)

        return jsonify(out), 200

    except Exception as e:
        log.exception("SMS ingestion failed: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500


from sms_poller import poll_once

@app.route("/sms-poll", methods=["POST"])
def sms_poll():
    # simple guard so nobody hits it publicly
    key = request.headers.get("X-Admin-Key", "")
    if key != os.getenv("ADMIN_KEY", ""):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    poll_once()
    return jsonify({"ok": True}), 200


@app.route("/sms-cadence", methods=["POST"])
def sms_cadence():
    key = request.headers.get("X-Admin-Key", "")
    if key != os.getenv("ADMIN_KEY", ""):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    send_sms_cadence_once()
    return jsonify({"ok": True}), 200

@app.route("/email-router-inbound", methods=["POST"])
def email_router_inbound():
    """
    Universal inbound email router.
    Power Automate rooftop flows will POST raw email payload here,
    with subscription_id (required) + optional rooftop_code.
    This endpoint classifies lead source and forwards to the correct processor,
    or ignores non-lead mail.
    """
    try:
        payload = request.get_json(force=True, silent=False) or {}

        inbound = {
            "subscription_id": (payload.get("subscription_id") or payload.get("subscriptionId") or "").strip(),
            "rooftop_code": (payload.get("rooftop_code") or payload.get("rooftopCode") or "").strip(),
            "source": (payload.get("source") or "email_router").strip(),

            "conversation_id": (payload.get("conversation_id") or payload.get("conversationId") or "").strip(),
            "message_id": (payload.get("message_id") or payload.get("messageId") or "").strip(),

            "from": payload.get("from"),
            "to": payload.get("to"),
            "cc": payload.get("cc"),
            "subject": (payload.get("subject") or "").strip(),
            "body_html": payload.get("body_html") or "",
            "body_text": payload.get("body_text") or "",
            "timestamp": payload.get("timestamp") or _dt.utcnow().isoformat(),
            "headers": payload.get("headers") or {},
        }

        # Require subscription_id (critical for Fortellis routing)
        if not inbound["subscription_id"]:
            return jsonify({
                "status": "error",
                "message": "subscription_id is required on /email-router-inbound",
            }), 400

        lead_source = detect_lead_source(inbound)

        log.info(
            "📥 EMAIL ROUTER inbound: sub_id=%s rooftop=%s lead_source=%r from=%r subject=%r",
            inbound.get("subscription_id"),
            inbound.get("rooftop_code"),
            lead_source,
            (inbound.get("from") or ""),
            (inbound.get("subject") or "")[:160],
        )

        # -----------------------------------------
        # ROUTING TABLE (we’ll finalize next)
        # -----------------------------------------
        # For now:
        # - If it matches one of your lead-source rules, treat as a lead notification
        # - Otherwise ignore (non-lead mail)
        #
        # Once you paste your PA rules, we can route certain sources to different
        # processors if needed.
        if lead_source:
            inbound["lead_source"] = lead_source
            process_lead_notification(inbound)
            return jsonify({"status": "ok", "handled": True, "lead_source": lead_source}), 200

        return jsonify({"status": "ok", "handled": False, "ignored": True, "reason": "no_rule_match"}), 200

    except Exception as e:
        log.exception("email router inbound failed: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500


# -----------------------------
#   Local Run
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
