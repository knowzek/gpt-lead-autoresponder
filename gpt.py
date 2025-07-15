import os
import openai

openai.api_key = os.getenv("OPENAI_API_KEY")
ASSISTANT_ID = "asst_..."  # Replace with your Assistants API ID

def run_gpt(user_prompt, customer_name):
    thread = openai.beta.threads.create()

    openai.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content=user_prompt
    )

    run = openai.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=ASSISTANT_ID
    )

    while True:
        run = openai.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        if run.status == "completed":
            break
        elif run.status == "failed":
            raise Exception("❌ GPT run failed")

    messages = openai.beta.threads.messages.list(thread_id=thread.id)
    raw = messages.data[0].content[0].text.value.strip()

    # Optional: Strip fluff like "Certainly!" if followed by a subject line
    lines = raw.splitlines()
    if len(lines) > 1 and "subject:" in lines[1].lower():
        lines = lines[1:]

    subject_line = ""
    body_lines = []

    for line in lines:
        if line.lower().startswith("subject:"):
            subject_line = line.replace("Subject:", "").strip()
        else:
            body_lines.append(line)

    email_body = "\n".join(body_lines).strip()

    # Replace placeholder with real name
    if customer_name:
        email_body = email_body.replace("[Guest's Name]", customer_name)

    return {
        "subject": subject_line,
        "body": email_body
    }
