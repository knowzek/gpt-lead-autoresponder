import os, time, json, logging
from openai import OpenAI
from openai import APIStatusError  # available in recent SDKs; if import fails, just catch Exception

log = logging.getLogger("patti")

CLIENT_TIMEOUT = float(os.getenv("OPENAI_TIMEOUT", "30"))
MAX_RETRIES    = int(os.getenv("OPENAI_MAX_RETRIES", "4"))
ASSISTANT_ID   = os.getenv("OPENAI_ASSISTANT_ID")  # must be set

client = OpenAI(timeout=CLIENT_TIMEOUT)

def _retryable(status):
    return status in (429, 500, 502, 503, 504)

def _coerce_reply(text: str):
    """
    Try to parse assistant output as JSON with {subject, body}; else build a sane fallback.
    """
    try:
        data = json.loads(text)
        if isinstance(data, dict) and "subject" in data and "body" in data:
            return {"subject": str(data["subject"]).strip(), "body": str(data["body"]).strip()}
    except Exception:
        pass
    # Fallback: keep your downstream behavior (subject is normalized later in main.py)
    return {
        "subject": "Your vehicle inquiry with Patterson Auto Group",
        "body": text.strip() if text.strip() else "Thanks for reaching out — happy to help!"
    }

def run_gpt(prompt: str, customer_name: str, rooftop_name: str = None, max_retries: int = MAX_RETRIES):
    """
    Compose a reply using the Assistants API. Retries on 5xx/429 and degrades gracefully
    so the job never crashes on transient OpenAI errors.
    """
    backoff = 1.0
    last_err = None

    # Optional: lazy import so the function doesn't crash if the mapping isn't present
    rooftop_address = ""
    try:
        from rooftops import ROOFTOP_INFO  # expected shape: {"Mission Viejo Kia": {"address": "...", "email": "..."}}
        if rooftop_name:
            rooftop_address = (ROOFTOP_INFO.get(rooftop_name, {}) or {}).get("address", "")
    except Exception:
        # No mapping available; we’ll still include the rooftop name without an address.
        pass

    for attempt in range(max_retries):
        try:
            thread = client.beta.threads.create()
            client.beta.threads.messages.create(
                thread_id=thread.id,
                role="user",
                content=prompt
            )
            run = client.beta.threads.runs.create(
                thread_id=thread.id,
                assistant_id=ASSISTANT_ID,
            )

            # poll briefly for completion
            for _ in range(60):  # ~30s @0.5s
                run = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
                if run.status == "completed":
                    break
                if run.status in ("failed", "cancelled", "expired"):
                    raise RuntimeError(f"Assistant run status={run.status}")
                time.sleep(0.5)

            msgs = client.beta.threads.messages.list(thread_id=thread.id, order="desc", limit=1)
            if not msgs.data:
                raise RuntimeError("Assistant returned no messages")
            parts = msgs.data[0].content
            text = ""
            for p in parts:
                if hasattr(p, "text") and getattr(p.text, "value", None):
                    text += p.text.value
            if not text.strip():
                raise RuntimeError("Assistant message had no text content")

            # Parse into {"subject": ..., "body": ...}
            reply = _coerce_reply(text)

            # 1) Swap out "Patterson Auto Group" with the specific rooftop, if provided
            if rooftop_name:
                if "subject" in reply and reply["subject"]:
                    reply["subject"] = reply["subject"].replace("Patterson Auto Group", rooftop_name)
                if "body" in reply and reply["body"]:
                    reply["body"] = reply["body"].replace("Patterson Auto Group", rooftop_name)

            # 2) Personalize guest name placeholders if present (preserving your existing behavior)
            if customer_name and "body" in reply and reply["body"]:
                reply["body"] = (
                    reply["body"]
                    .replace("[Guest's Name]", customer_name)
                    .replace("[Guest’s Name]", customer_name)
                )

            # 3) Append rooftop signature (name + address) when available
            if rooftop_name:
                signature_lines = ["", "Patti", "Virtual Assistant", rooftop_name]
                if rooftop_address:
                    signature_lines.append(rooftop_address)
                signature = "\n".join(signature_lines)
                reply["body"] = (reply.get("body") or "").rstrip() + "\n\n" + signature

            return reply

        except APIStatusError as e:
            status = getattr(e, "status_code", None)
            last_err = e
            if status is not None and _retryable(status) and attempt < max_retries - 1:
                log.warning("OpenAI %s on attempt %d; retrying in %.1fs", status, attempt + 1, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 8.0)
                continue
            log.error("OpenAI APIStatusError (status=%s): %s", status, str(e)[:200])
            break
        except Exception as e:
            last_err = e
            # Retry unknown transient errors once or twice, otherwise fall back
            if attempt < max_retries - 1:
                log.warning("OpenAI call failed (%s); retrying in %.1fs", type(e).__name__, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 8.0)
                continue
            log.error("OpenAI call failed: %s", str(e)[:200])
            break

    # graceful fallback so the cron keeps going
    fallback_rooftop = rooftop_name or "Patterson Auto Group"
    subject = f"Your vehicle inquiry with {fallback_rooftop}"

    body_lines = [
        f"Hi {customer_name or 'there'},",
        "",
        "Thanks for your inquiry! I’m happy to help with details, availability, and next steps. "
        "Let me know any preferences on trim, color, or timing and I’ll get everything lined up.",
        "",
        "Patti",
        "Virtual Assistant",
        fallback_rooftop
    ]
    if rooftop_name and rooftop_address:
        body_lines.append(rooftop_address)

    return {
        "subject": subject,
        "body": "\n".join(body_lines)
    }
