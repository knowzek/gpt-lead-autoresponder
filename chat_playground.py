# chat_playground.py
import os, time, uuid, json
from flask import Flask, request, redirect, url_for, render_template_string
from helpers import rJson, wJson
from processNewData import processHit

app = Flask(__name__)
TEST_PATH = os.environ.get("TEST_HIT_PATH", "jsons/process/TEST-LEAD.json")

HTML = """
<!doctype html>
<title>Patti Test Chat</title>
<style>
 body{font:16px/1.4 system-ui,Arial;padding:20px;max-width:800px;margin:auto}
 .msg{margin:8px 0;padding:10px;border-radius:10px}
 .user{background:#eef}
 .patti{background:#efe}
 .sys{background:#eee}
 form{margin-top:16px;display:flex;gap:8px}
 input[type=text]{flex:1;padding:10px;border:1px solid #ccc;border-radius:8px}
 button{padding:10px 14px;border:0;background:#222;color:#fff;border-radius:8px}
 .meta{font-size:12px;color:#666}
</style>
<h1>Patti Test Chat</h1>
<div class=meta>File: {{path}}</div>
{% if messages|length == 0 %}
  <p>No messages yet. Click "Kick Off" to generate Patti's opener.</p>
{% endif %}
{% for m in messages %}
  <div class="msg {{'patti' if m.role=='assistant' else ('user' if m.role=='user' else 'sys')}}">
    <div class=meta>{{m.role}}</div>
    <div>{{m.content}}</div>
  </div>
{% endfor %}

<form method="post" action="{{ url_for('send') }}">
  <input type="text" name="text" placeholder="Type your message to Patti..." autofocus />
  <button type="submit">Send</button>
</form>

<form method="post" action="{{ url_for('kickoff') }}">
  <button type="submit">Kick Off (create Patti opener)</button>
</form>
"""

def ensure_seed():
    os.makedirs("jsons/process", exist_ok=True)
    if not os.path.exists(TEST_PATH):
        seed = {
            "opportunityId": "TEST-LEAD-001",
            "_subscription_id": "bb4a4f18-1693-4450-a08e-40d8df30c139",
            "source": "Mazda",
            "upType": "Internet",
            "soughtVehicles": [{"isPrimary": True, "isNew": True, "yearFrom": 2025, "yearTo": 2025, "make": "Mazda", "model": "MX-5 Miata"}],
            "salesTeam": [{"firstName": "Veronica", "lastName": "Paco", "isPrimary": True, "positionName": "Salesperson", "positionCode": "S"}],
            "customer": {"id": "CUST-TEST", "firstName": "Alex", "lastName": "Rivera", "emails": [{"address":"alex@example.com"}]},
            "messages": [],
            "patti_already_contacted": False,
            "last_msg_by": None,
            "followUP_count": 0,
            "followUP_date": None,
            "alreadyProcessedActivities": [],
            "completedActivitiesTesting": []
        }
        wJson(seed, TEST_PATH)

def normalize_messages(state):
    out = []
    for m in state.get("messages", []):
        role = m.get("role") or m.get("from") or m.get("author") or "system"
        text = m.get("content") or m.get("text") or m.get("message") or json.dumps(m, ensure_ascii=False)
        out.append(type("M", (), {"role": "assistant" if role in ("assistant","patti") else ("user" if role=="user" else "system"),
                                  "content": text}))
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

@app.route("/", methods=["GET"])
def home():
    ensure_seed()
    state = rJson(TEST_PATH)
    msgs = normalize_messages(state)
    return render_template_string(HTML, messages=msgs, path=TEST_PATH)

@app.route("/kickoff", methods=["POST"])
def kickoff():
    ensure_seed()
    state = rJson(TEST_PATH)
    state = processHit(state)
    wJson(state, TEST_PATH)
    return redirect(url_for("home"))

@app.route("/send", methods=["POST"])
def send():
    ensure_seed()
    text = (request.form.get("text") or "").strip()
    if text:
        state = rJson(TEST_PATH)
        state = add_customer_activity(state, text)
        state = processHit(state)
        wJson(state, TEST_PATH)
    return redirect(url_for("home"))

if __name__ == "__main__":
    # safety defaults
    os.environ.setdefault("PATTI_SAFE_MODE", "1")
    os.environ.setdefault("EMAIL_MODE", "0")
    os.environ.setdefault("DEBUGMODE", "1")
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
