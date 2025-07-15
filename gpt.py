import os
import openai

openai.api_key = os.getenv("OPENAI_API_KEY")
ASSISTANT_ID = "asst_5wPtENezPqy78Pu2ZA7Dr2qk"  

def run_gpt(user_prompt, customer_name):
    thread = openai.beta.threads.create()

    # Send user message (no system message, keeps Patti's tone)
    openai.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content=user_prompt
    )

    # Trigger GPT run
    run = openai.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=ASSISTANT_ID
    )

    # Wait for completion
    while True:
        run = openai.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        if run.status == "completed":
            break
        elif run.status == "failed":
            raise Exception("❌ GPT run failed")

    # Get GPT response
    messages = openai.beta.threads.messages.list(thread_id=thread.id)
    raw = messages.data[0].content[0].text.value.strip()

    # Optional: Strip fluff like "Certainly!" if followed by Subject line
    lines = raw.splitlines()
    if len(lines) > 1 and "subject:" in lines[1].lower():
        lines = lines[1:]

    # Extract subject + body
    subject_line = ""
    body_lines = []
    for line in lines:
        if line.lower().startswith("subject:") or line.lower().startswith("### subject:"):
            subject_line = line.split(":", 1)[1].strip()
        else:
            body_lines.append(line)

    email_body = "\n".join(body_lines).strip()

    # Replace placeholder with actual name
    if customer_name:
        email_body = email_body.replace("[Guest's Name]", customer_name)

    return {
        "subject": subject_line,
        "body": email_body
    }
