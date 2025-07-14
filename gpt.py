import os
import openai

openai.api_key = os.getenv("OPENAI_API_KEY")

def generate_response(lead):
    lead_type = lead.get("leadType", "Unknown")
    source = lead.get("source", "Unknown")
    vehicle = lead.get("soughtVehicles", [{}])[0]
    vehicle_desc = f"{vehicle.get('yearFrom', '')} {vehicle.get('make', '')} {vehicle.get('model', '')}".strip()

    prompt = f"""You are a helpful virtual assistant for a car dealership. A new {lead_type} lead just came in from {source}.
The customer is interested in: {vehicle_desc}.

Write a friendly, professional email response that:
- thanks them for their interest
- acknowledges their vehicle interest
- offers to assist with next steps or scheduling

Respond in the dealership’s tone: courteous, knowledgeable, not overly pushy.
"""

    print("🧠 Sending prompt to OpenAI...")
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}]
    )

    return response["choices"][0]["message"]["content"]
