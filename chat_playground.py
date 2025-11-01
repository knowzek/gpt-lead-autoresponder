import os, time, uuid, json
from flask import Flask, request, redirect, url_for, render_template_string
from helpers import rJson, wJson
from processNewData import processHit
import traceback, sys
from datetime import datetime, timedelta
from uuid import uuid4


app = Flask(__name__)
TEST_PATH = os.environ.get("TEST_HIT_PATH", "jsons/process/TEST-LEAD.json")

HTML = """
<!doctype html>
<title>Patti Test Chat</title>
<style>
 body{font:16px/1.5 system-ui,Arial;padding:24px;max-width:900px;margin:auto}
 h1{margin:0 0 8px}
 .row{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
 .msg{margin:8px 0;padding:12px 14px;border-radius:12px;white-space:pre-wrap}
 .user{background:#eef}
 .patti{background:#efe}
 .sys{background:#f4f4f4}
 form{margin-top:12px}
 .meta{font-size:12px;color:#666;margin-bottom:4px}
 input[type=text], textarea{width:100%;max-width:100%;padding:10px;border:1px solid #ccc;border-radius:8px}
 button{padding:10px 14px;border:0;background:#222;color:#fff;border-radius:8px;cursor:pointer}
 .grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
</style>

<h1>Patti Test Chat</h1>
<div class=meta>File: {{path}}</div>

{% if messages|length == 0 %}
  <p>No messages yet. Click <b>Kick Off</b> to generate Patti's opener, or seed a lead + first customer message below.</p>
{% endif %}

{% for m in messages %}
  <div class="msg {{'patti' if m.role=='assistant' else ('user' if m.role=='user' else 'sys')}}">
    <div class=meta>{{m.role}}</div>
    <div>{{m.content}}</div>
  </div>
{% endfor %}

<form method="post" action="{{ url_for('send') }}">
  <div class=row>
    <input type="text" name="text" placeholder="Type your message to Patti..." autofocus />
    <button type="submit">Send</button>
  </div>
</form>

<form method="post" action="{{ url_for('kickoff') }}">
  <button type="submit">Kick Off (create Patti opener)</button>
</form>

<hr>

<h2>Seed a Realistic Lead + First Message</h2>
<form method="post" action="{{ url_for('seed') }}">
  <div class="grid">
    <div>
      <label>Customer first name</label>
      <input type="text" name="firstName" value="Jason">
    </div>
    <div>
      <label>Customer last name</label>
      <input type="text" name="lastName" value="Miller">
    </div>
    <div>
      <label>Email</label>
      <input type="text" name="email" value="jason.miller@example.com">
    </div>
    <div>
      <label>Phone</label>
      <input type="text" name="phone" value="555-123-9876">
    </div>
    <div>
      <label>Make</label>
      <input type="text" name="make" value="Mazda">
    </div>
    <div>
      <label>Model</label>
      <input type="text" name="model" value="CX-5">
    </div>
    <div>
      <label>Year</label>
      <input type="text" name="year" value="2024">
    </div>
    <div>
      <label>Source</label>
      <input type="text" name="source" value="Website">
    </div>
  </div>
  <div style="margin-top:8px">
    <label>First customer message (what the lead actually wrote)</label>
    <textarea name="notes" rows="3">Hi, I’m interested in the 2024 Mazda CX-5 you have listed. Is it still available?</textarea>
  </div>
  <div class=row style="margin-top:8px">
    <button type="submit">Seed Lead + Message</button>
  </div>
</form>
"""

def _playground_force_assistant_reply(state: dict, text: str) -> dict:
    msgs = state.get("messages") or []
    if not isinstance(msgs, list):
        msgs = []
    msgs.append({
        "role": "assistant",
        "content": text,
        "msgFrom": "patti",
        "subject": "Re: your inquiry",
        "body": text,
        "date": datetime.utcnow().isoformat() + "Z",
    })
    state["messages"] = msgs
    state["conversation"] = msgs
    state["thread"] = msgs
    cd = state.get("checkedDict") or {}
    cd["patti_already_contacted"] = True
    cd["last_msg_by"] = "patti"
    state["checkedDict"] = cd
    return state

from datetime import datetime, timedelta

def playground_inject_patti_reply(state: dict) -> dict:
    try:
        if os.getenv("OFFLINE_MODE", "1") != "1":
            return state  # playground only

        msgs = state.get("messages") or state.get("conversation") or state.get("thread") or []
        if not isinstance(msgs, list):
            msgs = []

        # look at last message only
        last = msgs[-1] if msgs else None
        last_text = ""
        last_role = ""
        if isinstance(last, dict):
            last_role = (last.get("role") or last.get("msgFrom") or "").lower()
            last_text = (last.get("content") or last.get("body") or last.get("text") or "").strip()

        # only reply if the last message is from the user/customer
        if last_role not in ("user", "customer") or not last_text:
            print("[PLAY] injector: last msg not from user; skip")
            return state

        customer_name = ((state.get("customer") or {}).get("firstName") or "there")
        rooftop_name  = ((state.get("rooftop")  or {}).get("name")      or "Patterson Auto Group")

        prompt = f"""
Generate Patti's next reply to this customer message:

Customer: "{last_text}"

Rules:
- Start exactly with: Hi {customer_name},
- Be helpful and human. One short paragraph is fine.
- No signatures/phone/address/URLs.
- Do not mention scheduling, booking, or test-drive links; I will add that line automatically.
"""
        from gpt import run_gpt
        resp = run_gpt(prompt, customer_name, rooftop_name)
        subject = resp.get("subject", "Re: your inquiry")
        body    = resp.get("body", "Happy to help!")

        now = datetime.utcnow()
        assistant_time = (now + timedelta(seconds=1)).isoformat() + "Z"

        patti_msg = {
            "msgFrom": "patti",
            "subject": subject,
            "body": body,
            "date": assistant_time,
            "role": "assistant",
            "content": body,
        }
        msgs.append(patti_msg)
        state["messages"] = msgs
        state["conversation"] = msgs
        state["thread"] = msgs

        # mark contact + push follow-up into the future (avoid nudges)
        cd = state.get("checkedDict") or {}
        cd["patti_already_contacted"] = True
        cd["last_msg_by"] = "patti"
        state["checkedDict"] = cd
        state["followUP_date"] = (now + timedelta(days=2)).isoformat() + "Z"
        state["followUP_count"] = 0

        return state
    except Exception as e:
        print(f"[PLAY] injector error: {e}")
        return state



def coalesce_messages(state: dict | None) -> dict:
    # Be defensive: accept None or non-dict and normalize
    if not isinstance(state, dict):
        state = {}
    pool = []
    for key in ("messages", "conversation", "thread"):
        arr = state.get(key) or []
        if isinstance(arr, list):
            pool.extend(arr)

    # de-dupe by (id, text/body/content)
    seen = set()
    deduped = []
    for m in pool:
        if not isinstance(m, dict):
            continue
        t = (m.get("content") or m.get("body") or m.get("text") or "")
        k = (m.get("id"), t)
        if k not in seen:
            seen.add(k)
            deduped.append(m)

    state["messages"] = deduped
    return state



def ensure_dir():
    os.makedirs("jsons/process", exist_ok=True)

def ensure_min_schema(state: dict | None) -> dict:
    """Make the JSON look like what processNewData.processHit expects."""
    from datetime import datetime

    state = state or {}
    if not isinstance(state, dict):
        state = {}

    # Required identifiers
    state.setdefault("opportunityId", f"TEST-{uuid.uuid4().hex[:8]}")
    state.setdefault("_subscription_id", os.environ.get("TEST_SUB_ID", "bb4a4f18-1693-4450-a08e-40d8df30c139"))

    # Core arrays/flags used by processHit
    for k, default in (
        ("messages", []),
        ("conversation", []),     # ← add
        ("thread", []),           # ← add
        ("completedActivitiesTesting", []),
        ("alreadyProcessedActivities", []),
    ):
        v = state.get(k)
        state[k] = v if isinstance(v, list) else (list(v) if isinstance(v, tuple) else default)

    state.setdefault("patti_already_contacted", False)
    state.setdefault("last_msg_by", None)

    # Normalize followUP_count (int)
    fu_count = state.get("followUP_count")
    try:
        fu_count = int(fu_count) if fu_count is not None else 0
    except Exception:
        fu_count = 0
    state["followUP_count"] = fu_count

    # Normalize followUP_date (str | None)
    fu_date = state.get("followUP_date")
    if fu_date is None:
        fu_date_str = None
    elif isinstance(fu_date, str):
        fu_date_str = fu_date
    else:
        fu_date_str = fu_date.isoformat() if hasattr(fu_date, "isoformat") else None
    state["followUP_date"] = fu_date_str

    # Mirror into checkedDict (processHit writes here)
    cd = state.get("checkedDict") or {}
    cd.setdefault("patti_already_contacted", state["patti_already_contacted"])
    cd.setdefault("last_msg_by", state["last_msg_by"])
    cd.setdefault("followUP_count", state["followUP_count"])
    cd.setdefault("followUP_date", state["followUP_date"])
    cd.setdefault("alreadyProcessedActivities", state["alreadyProcessedActivities"])
    state["checkedDict"] = cd

    # --- NEW: coalesce/align message arrays for the UI ---
    pool = []
    for key in ("messages", "conversation", "thread"):
        arr = state.get(key) or []
        if isinstance(arr, list):
            pool.extend(arr)

    # de-dupe by (id, text/body/content)
    seen, deduped = set(), []
    for m in pool:
        if not isinstance(m, dict):
            continue
        t = (m.get("content") or m.get("body") or m.get("text") or "")
        k = (m.get("id"), t)
        if k not in seen:
            seen.add(k)
            deduped.append(m)

    state["messages"] = deduped
    state["conversation"] = deduped
    state["thread"] = deduped
    # --- end NEW ---

    return state



def safe_process(state):
    """Run processHit without crashing the web UI; log any error."""
    try:
        return processHit(state), None
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        return state, str(e)
     

from datetime import datetime, timezone

def _parse_dt(s: str | None):
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    # Normalize trailing Z to +00:00
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        # Make naive datetimes UTC-aware
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None

def norm_msgs(state):
    raw = state.get("messages") or []
    if not isinstance(raw, list):
        raw = []

    enriched = []
    for idx, m in enumerate(raw):
        if not isinstance(m, dict):
            continue

        raw_role = (m.get("role") or m.get("msgFrom") or m.get("from") or m.get("author") or "system")
        role_l = str(raw_role).lower()
        if role_l in ("assistant", "patti"):
            role = "assistant"
        elif role_l in ("user", "customer"):
            role = "user"
        else:
            role = "system"

        raw_text = (m.get("content") or m.get("body") or m.get("text") or m.get("message"))
        if not raw_text:
            import json as _json
            raw_text = _json.dumps(
                {k: v for k, v in m.items() if k in ("subject","body","notes","date","action")},
                ensure_ascii=False
            )

        ts = _parse_dt(m.get("date") or m.get("createdAt"))
        enriched.append({
            "role": role,
            "content": str(raw_text),
            "_ts": ts,
            "_idx": idx,
        })

    # Oldest → newest; ensure the fallback is also UTC-aware
    enriched.sort(key=lambda x: (x["_ts"] or datetime.min.replace(tzinfo=timezone.utc), x["_idx"]))

    return [type("M", (), {"role": e["role"], "content": e["content"]}) for e in enriched]




def add_customer_activity(state, text: str):
    acts = state.get("completedActivitiesTesting", [])
    acts.append({
        "id": f"web-{uuid.uuid4().hex[:8]}",
        "typeId": 20,
        "title": "Customer Email",
        "notes": text,
        "completedDate": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    })
    state["completedActivitiesTesting"] = acts
    return state

def seed_state(firstName, lastName, email, phone, make, model, year, source, notes):
    return {
        "opportunityId": f"TEST-{uuid.uuid4().hex[:8]}",
        "_subscription_id": os.environ.get("TEST_SUB_ID", "bb4a4f18-1693-4450-a08e-40d8df30c139"),
        "source": source or "Website",
        "upType": "Internet",
        "soughtVehicles": [{
            "isPrimary": True,
            "isNew": True,
            "yearFrom": int(year or 2025),
            "yearTo": int(year or 2025),
            "make": make or "Mazda",
            "model": model or "CX-5"
        }],
        "salesTeam": [{
            "firstName": "Veronica",
            "lastName": "Paco",
            "isPrimary": True,
            "positionName": "Salesperson",
            "positionCode": "S"
        }],
        "customer": {
            "id": f"CUST-{uuid.uuid4().hex[:6]}",
            "firstName": firstName or "Alex",
            "lastName": lastName or "Rivera",
            "emails": [{"address": email or "alex@example.com"}],
            "phones": [{"number": phone or ""}]
        },
        "messages": [],
        "patti_already_contacted": False,
        "last_msg_by": None,
        "followUP_count": 0,
        "followUP_date": None,
        "alreadyProcessedActivities": [],
        "completedActivitiesTesting": [{
            "id": f"lead-{uuid.uuid4().hex[:8]}",
            "typeId": 20,
            "title": "Customer Inquiry",
            "notes": notes or "Is this available?",
            "completedDate": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }]
    }

@app.route("/", methods=["GET"])
def home():
    ensure_dir()
    state = rJson(TEST_PATH) if os.path.exists(TEST_PATH) else {}
    state = ensure_min_schema(state)
    state = coalesce_messages(state)  
    msgs = norm_msgs(state) 
    # Fallback: if still empty, synthesize a visible customer message from the latest activity
    if not state["messages"] and state.get("completedActivitiesTesting"):
        last = state["completedActivitiesTesting"][-1]
        state["messages"] = [{
            "id": f"cust-{last.get('id')}",
            "role": "user",
            "content": last.get("notes") or "Hi, I’m interested…"
        }]
    msgs = norm_msgs(state)
    return render_template_string(HTML, messages=msgs, path=TEST_PATH)

@app.route("/kickoff", methods=["POST"])
def kickoff():
    ensure_dir()
    state = rJson(TEST_PATH) if os.path.exists(TEST_PATH) else seed_state("Alex","Rivera","alex@example.com","","Mazda","MX-5 Miata","2025","Website","")
    state = ensure_min_schema(state)
    state, err = safe_process(state)
    state = coalesce_messages(state)  # ← add
    wJson(state, TEST_PATH)
    return redirect(url_for("home"))

@app.route("/send", methods=["POST"])
def send():
    ensure_dir()
    text = (request.form.get("text") or "").strip()
    if not text:
        return redirect(url_for("home"))

    # Load or seed baseline state
    state = rJson(TEST_PATH) if os.path.exists(TEST_PATH) else seed_state(
        "Alex", "Rivera", "alex@example.com", "", "Mazda", "MX-5 Miata", "2025", "Website", ""
    )
    state = ensure_min_schema(state)

    # Append a synthetic *customer email* activity that processHit/checkActivities understand
    acts = state.get("completedActivitiesTesting", [])
    acts.append({
        "activityId": f"web-{uuid.uuid4().hex[:8]}",
        "activityType": 20,
        "activityName": "Read Email",
        "completedDate": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "message": {
            "subject": "Customer reply via playground",
            "body": text
        },
        "comments": ""
    })
    state["completedActivitiesTesting"] = acts
    wJson(state, TEST_PATH)

    # Run processor (may return None)
    processed, err = safe_process(state)
    if not isinstance(processed, dict) or processed is None:
        try:
            processed = rJson(TEST_PATH)  # recover last written
        except Exception:
            processed = {}

    # Normalize, inject Patti (OFFLINE playground only), persist
    processed = ensure_min_schema(processed)
    # 💬 Mirror the user’s typed message into the visible chat
    if text:
        msgs = processed.get("messages") or []
        if not isinstance(msgs, list):
            msgs = []
        user_msg = {
            "role": "user",
            "content": text,
            "msgFrom": "customer",
            "body": text,
            "date": datetime.utcnow().isoformat() + "Z",
        }
        msgs.append(user_msg)
        processed["messages"] = msgs
        processed["conversation"] = msgs
        processed["thread"] = msgs
    
    # 🤖 Playground-only: generate Patti’s reply
    processed = playground_inject_patti_reply(processed)
    wJson(processed, TEST_PATH)
    return redirect(url_for("home"))


@app.route("/seed", methods=["POST"])
def seed():
    ensure_dir()
    # 1) Build baseline state from form
    first  = request.form.get("firstName")
    last   = request.form.get("lastName")
    email  = request.form.get("email")
    phone  = request.form.get("phone")
    make   = request.form.get("make")
    model  = request.form.get("model")
    year   = request.form.get("year")
    source = request.form.get("source")
    notes  = (request.form.get("notes") or "").strip()

    state = seed_state(first, last, email, phone, make, model, year, source, notes)
    state = ensure_min_schema(state)

    # ✅ make sure this lead is processable
    state["isActive"] = True
    state["patti_already_contacted"] = False
    state["checkedDict"] = {
        "patti_already_contacted": False,
        "last_msg_by": "customer",
        "followUP_count": 0,
        "followUP_date": None,
        "alreadyProcessedActivities": {}
    }

    # 2) Seed a synthetic *customer activity* so processHit will respond
    #    (processHit triggers on activityType==20 or activityName=="Read Email")
    act_id = f"lead-{uuid4().hex[:8]}"  # if you used: from uuid import uuid4
    subject_bits = [year or "", make or "", model or ""]
    subject_text = " ".join([b for b in subject_bits if b]).strip() or "your vehicle"
    synthetic_activity = {
        "activityId": act_id,
        "activityType": 20,               # Customer Email
        "activityName": "Read Email",
        "completedDate": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "comments": "",                   # no sentinel -> allows first reply
        "message": {
            "subject": f"New inquiry about {subject_text}",
            "body": notes or "Hi! I'm interested and have a few questions."
        }
    }
    acts = state.get("completedActivitiesTesting", [])
    acts.append(synthetic_activity)
    state["completedActivitiesTesting"] = acts

    wJson(state, TEST_PATH)

    # 3) Run your processor
    processed, err = safe_process(state)

    # 4) Defensive recovery if processor returned None/invalid
    if not isinstance(processed, dict):
        try:
            processed = rJson(TEST_PATH)
        except Exception:
            processed = {}

    # 5) Normalize, inject reply (playground only), and persist
    processed = ensure_min_schema(processed)
    processed = playground_inject_patti_reply(processed)  # OFFLINE-only guard is inside
    wJson(processed, TEST_PATH)

    return redirect(url_for("home"))

if __name__ == "__main__":
    # safety defaults
    os.environ.setdefault("PATTI_SAFE_MODE", "1")
    os.environ.setdefault("EMAIL_MODE", "0")
    os.environ.setdefault("DEBUGMODE", "1")
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
