# main.py
import os
from fortellis import get_token, get_recent_leads, get_opportunity
from gpt import run_gpt
from emailer import send_email
from state_store import was_processed, mark_processed

MICKEY_EMAIL = os.getenv("MICKEY_EMAIL")

def format_prompt(opportunity):
    vehicle = opportunity.get("soughtVehicles", [{}])[0]
    trade = opportunity.get("tradeIns", [{}])[0]
    make = vehicle.get("make", "a vehicle")
    model = vehicle.get("model", "")
    year = vehicle.get("yearFrom", "")
    trim = vehicle.get("trim", "")
    source = opportunity.get("source", "Internet")
    salesperson = opportunity.get("salesTeam", [{}])[0].get("firstName", "our team")

    return f"""
A new lead came in from {source}. They're interested in a {year} {make} {model} {trim}.
They may also trade in a {trade.get('year', '')} {trade.get('make', '')} {trade.get('model', '')}.
Write a friendly follow-up email introducing {salesperson} from our dealership.
    """

def main():
    token = get_token()
    leads = get_recent_leads(token)

    for lead in leads:
        aid = lead["activityId"]
        if was_processed(aid):
            continue

        opp_id = lead["opportunityId"]
        opportunity = get_opportunity(opp_id, token)
        prompt = format_prompt(opportunity)
        response = run_gpt(prompt)
        send_email(to=MICKEY_EMAIL, subject="[GPT Demo] New Lead Response", body=response)
        mark_processed(aid)

if __name__ == "__main__":
    main()
