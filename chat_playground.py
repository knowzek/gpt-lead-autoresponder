import os, time, uuid, json
from flask import Flask, request, redirect, url_for, render_template_string
from helpers import rJson, wJson
from processNewData import processHit
import traceback, sys

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

def ensure_dir():
    os.makedirs("jsons/process", exist_ok=True)

def ensure_min_schema(state: dict | None) -> dict:
    """Make the JSON look like what processNewData.processHit expects."""
    state = state or {}
    state.setdefault("messages", [])
    state.setdefault("completedActivitiesTesting", [])
    state.setdefault("alreadyProcessedActivities", [])
    state.setdefault("patti_already_contacted", False)
    state.setdefault("last_msg_by", None)
    state.setdefault("followUP_count", 0)
    state.setdefault("followUP_date", None)

    cd = state.get("checkedDict") or {}
    cd.setdefault("patti_already_contacted", state.get("patti_already_contacted", False))
    cd.setdefault("last_msg_by", state.get("last_msg_by"))
    cd.setdefault("followUP_count", state.get("followUP_count", 0))
    cd.setdefault("followUP_date", state.get("followUP_date"))
    cd.setdefault("alreadyProcessedActivities", state.get("alreadyProcessedActivities", []))
    state["checkedDict"] = cd
    return state

def safe_process(state):
    """Run processHit without crashing the web UI; log any error."""
    try:
        return processHit(state), None
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        return state, str(e)
     

def norm_msgs(state):
    out = []
    for m in state.get("messages", []):
        # accept multiple schema variants
        raw_role = (
            m.get("role") or
            m.get("msgFrom") or
            m.get("from") or
            m.get("author") or
            "system"
        )
        raw_text = (
            m.get("content") or
            m.get("body") or
            m.get("text") or
            m.get("message")
        )

        role_l = str(raw_role).lower()
        if role_l in ("assistant","patti"):
            role = "assistant"
        elif role_l in ("user","customer"):
            role = "user"
        else:
            role = "system"

        if not raw_text:
            # last resort: show a compact JSON preview instead of the whole dict
            import json as _json
            raw_text = _json.dumps({k: v for k, v in m.items() if k in ("subject","body","notes","date","action")}, ensure_ascii=False)

        out.append(type("M", (), {"role": role, "content": raw_text}))
    return out


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
    msgs = norm_msgs(state)
    return render_template_string(HTML, messages=msgs, path=TEST_PATH)

@app.route("/kickoff", methods=["POST"])
def kickoff():
    ensure_dir()
    state = rJson(TEST_PATH) if os.path.exists(TEST_PATH) else seed_state("Alex","Rivera","alex@example.com","","Mazda","MX-5 Miata","2025","Website","")
    state = ensure_min_schema(state)
    state, err = safe_process(state)
    wJson(state, TEST_PATH)
    return redirect(url_for("home"))

@app.route("/send", methods=["POST"])
def send():
    ensure_dir()
    text = (request.form.get("text") or "").strip()
    if text:
        state = rJson(TEST_PATH) if os.path.exists(TEST_PATH) else seed_state("Alex","Rivera","alex@example.com","","Mazda","MX-5 Miata","2025","Website","")
        state = ensure_min_schema(state)
        # append the customer message
        acts = state.get("completedActivitiesTesting", [])
        acts.append({
            "id": f"web-{uuid.uuid4().hex[:8]}",
            "typeId": 20,
            "title": "Customer Email",
            "notes": text,
            "completedDate": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        })
        state["completedActivitiesTesting"] = acts
        state, err = safe_process(state)
        wJson(state, TEST_PATH)
    return redirect(url_for("home"))

@app.route("/seed", methods=["POST"])
def seed():
    ensure_dir()
    state = seed_state(
        request.form.get("firstName"),
        request.form.get("lastName"),
        request.form.get("email"),
        request.form.get("phone"),
        request.form.get("make"),
        request.form.get("model"),
        request.form.get("year"),
        request.form.get("source"),
        request.form.get("notes"),
    )
    state = ensure_min_schema(state)
    wJson(state, TEST_PATH)
    state, err = safe_process(state)  # immediate first run
    wJson(state, TEST_PATH)
    return redirect(url_for("home"))


if __name__ == "__main__":
    # safety defaults
    os.environ.setdefault("PATTI_SAFE_MODE", "1")
    os.environ.setdefault("EMAIL_MODE", "0")
    os.environ.setdefault("DEBUGMODE", "1")
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
