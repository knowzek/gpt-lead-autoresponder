# gpt.py
import os
import openai

openai.api_key = os.getenv("OPENAI_API_KEY")
ASSISTANT_ID = "g-67f7499803088191a8014bbcd3db4930"  # Patti assistant

def run_gpt(user_prompt):
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
        run = openai.beta.threads.runs.retrieve(
            thread_id=thread.id,
            run_id=run.id
        )
        if run.status == "completed":
            break
        elif run.status == "failed":
            raise Exception("GPT run failed")

    messages = openai.beta.threads.messages.list(thread_id=thread.id)
    response = messages.data[0].content[0].text.value
    return response.strip()
