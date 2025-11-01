from fortellis import get_token, get_opportunity, get_activities, get_activity_by_id_v1, send_opportunity_email_activity
from helpers import rJson, wJson

if __name__ == "__main__":
    subscription_id = "a4efeb74-2289-43d5-9814-1049fd35e894"
    opportunity_id = "923e9fe1-b3b6-f011-814f-00505690ec8c"
    token = get_token(subscription_id)
    # rooftop_sender = "sales@missionviejokia.edealerhub.com"
    # PROOF_RECIPIENTS = "dev.almousa@gmail.com"
    # proof_subject = "Explore the 2026 KIA Sportage Hybrid!"
    # proof_body = "Hi Cam,<br><br>Thank you for your interest in the 2026 KIA Sportage Hybrid! With its impressive fuel efficiency and advanced safety features, it’s an excellent choice for both city driving and weekend adventures. Plus, at Mission Viejo Kia, we pride ourselves on offering No Addendums or Dealer Markups, ensuring a transparent and stress-free experience.<br><br>We’re here to help you every step of the way! If you have any specific questions about the Sportage Hybrid, or if you have preferences regarding trim, color, or timing, please let us know. Ashley Madrigal is ready to assist you with any information you need.<br><br>Please let us know a convenient time for you, or you can instantly reserve your time here: <{LegacySalesApptSchLink}><br><br><br>Patti<br>Mission Viejo Kia<br>24041 El Toro Rd, Lake Forest, CA 92630<br><br><hr><p><em>Note: QA-only email sent to Mickey via CRM sendEmail; customer was NOT emailed.</em></p>"
    # rooftop_name = "Mission Viejo Kia"
    # send_opportunity_email_activity(
    #     token=token,
    #     dealer_key=subscription_id, 
    #     opportunity_id=opportunity_id,
    #     sender=rooftop_sender,            # from rooftops.py mapping
    #     recipients=PROOF_RECIPIENTS,       # 👈 proof only
    #     carbon_copies=[],                 # or keep empty in production
    #     subject=proof_subject,
    #     body_html=proof_body,
    #     rooftop_name=rooftop_name,
    # )

    opp = rJson(f"jsons/{opportunity_id}.json")
    # opp = get_opportunity(opportunity_id, token, subscription_id)
    # acts = get_activities(opportunity_id, opp.get('customer', {}).get("id"), token, subscription_id)
    # opp['acts'] = acts
    act_id = "991cef46-37b7-f011-814f-00505690ec8c"
    act = get_activity_by_id_v1(act_id, token, subscription_id)
    opp['act'] = act
    wJson(opp, f"jsons/{opportunity_id}.json")
