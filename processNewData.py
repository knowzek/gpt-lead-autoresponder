from helpers import (
    rJson,
    wJson,
    getFirstActivity,
    adf_to_dict,
    getInqueryUsingAdf,
    get_names_in_dir,
    sortActivities
)
from esQuerys import getNewData, esClient, getNewDataByDate
from rooftops import get_rooftop_info
from constants import *
from gpt import run_gpt, getCustomerMsgDict
import re

from fortellis import get_activities, get_token, get_activity_by_id_v1
from fortellis import get_vehicle_inventory_xml  # we’ll add this helper next
from inventory_matcher import recommend_from_xml

from datetime import datetime

# TODO:
# add all actions to crm

already_processed = get_names_in_dir("jsons/process")
DEBUGMODE = True

def checkActivities(opportunity, currDate, rooftop_name):
    # TODO: change this later to online one
    activities = opportunity.get('completedActivities', [])
    # activities = opportunity.get('completedActivitiesTesting', [])
    activities = sortActivities(activities)
    
    alreadyProcessedActivities =opportunity.get('alreadyProcessedActivities', {})
    checkedDict = opportunity.get('checkedDict', {})
    subscription_id = opportunity.get('_subscription_id')
    messages = opportunity.get("messages", [])
    customerInfo = opportunity.get('customer', {})

    token = None

    for act in activities:
        activityId = act.get("activityId")
        if activityId in alreadyProcessedActivities:
            continue

        if not token and not DEBUGMODE:
            token = get_token(subscription_id)
        
        comments = (act.get("comments") or "")
        activityName = (act.get("activityName") or "").strip().lower()
        activityType   = act.get("activityType")
        

        # 1) Our sentinel in any prior comment?
        if PATTI_FIRST_REPLY_SENTINEL in comments:
            checkedDict["patti_already_contacted"] = True
            continue
            # break

        if activityName == "Read Email" or activityType == 20:
            if not DEBUGMODE:
                fullAct = get_activity_by_id_v1(activityId, token, subscription_id)
                customerMsg = fullAct.get('message', {})
                customerMsgDict = {
                    "msgFrom": "customer",
                    "customerName": customerInfo.get('firstName'),
                    "subject": customerMsg.get('subject'),
                    "body": customerMsg.get('body'),
                    "date": fullAct.get('completedDate')
                }

                opportunity['messages'].append(customerMsgDict)
                messages = opportunity['messages']

                prompt = f"""
                generate next patti reply, here is the current messages between patti and the customer (python list of dicts):
                {messages}
                """
                response = run_gpt(
                            prompt,
                            customerInfo.get('firstName'),
                            rooftop_name,
                            prevMessages= True)

                subject   = response["subject"]
                body_html = response["body"]

                body_html = re.sub(
                    r"(?is)(?:\n\s*)?patti\s*(?:\r?\n)+virtual assistant.*?$",
                    "",
                    body_html
                )

                opportunity['messages'].append(
                    {
                        "msgFrom": "patti",
                        "subject": subject,
                        "body": body_html,
                        "date": currDate,
                        "action": response.get("action"),
                        "notes": response.get("notes")

                    }
                )

                # TODO: fix in which line
                opportunity['checkedDict']['last_msg_by'] = "patti"

                opportunity['followUP_date'] = currDate
                opportunity['followUP_count'] = 0
                opportunity['alreadyProcessedActivities'][activityId] = fullAct

            else:
                # testing...
                fullAct = act
                customerMsg = fullAct.get('message', {})
                customerMsgDict = {
                    "msgFrom": "customer",
                    "customerName": customerInfo.get('firstName'),
                    "subject": customerMsg.get('subject'),
                    "body": customerMsg.get('body'),
                    "date": fullAct.get('completedDate')
                }

                opportunity['messages'].append(customerMsgDict)
                messages = opportunity['messages']

                prompt = f"""
                generate next patti reply, here is the current messages between patti and the customer (python list of dicts):
                {messages}
                """
                response = run_gpt(
                            prompt,
                            customerInfo.get('firstName'),
                            rooftop_name,
                            prevMessages= True)

                subject   = response["subject"]
                body_html = response["body"]

                body_html = re.sub(
                    r"(?is)(?:\n\s*)?patti\s*(?:\r?\n)+virtual assistant.*?$",
                    "",
                    body_html
                )

                opportunity['messages'].append(
                    {
                        "msgFrom": "patti",
                        "subject": subject,
                        "body": body_html,
                        "date": currDate,
                        "action": response.get("action"),
                        "notes": response.get("notes")
                    }
                )

                # TODO: fix in which line
                opportunity['checkedDict']['last_msg_by'] = "patti"

                opportunity['followUP_date'] = currDate
                opportunity['followUP_count'] = 0
                opportunity['alreadyProcessedActivities'][activityId] = fullAct
   




def processHit(hit):
    currDate = datetime.now()

    # remove later
    global already_processed

    inquiry_text = None  # ensure defined

    try:
        opportunity : dict = hit['_source']
    except:
        opportunity : dict = hit

    if not opportunity.get('isActive', True):
        print("pass...")
        return

    subscription_id = opportunity['_subscription_id']
    opportunityId = opportunity['opportunityId']

    checkedDict = opportunity.get("checkedDict", {})

    # remove it later
    # if f"{opportunityId}.json" in already_processed:
    #     return

    customer = opportunity['customer']
    customerId = customer['id']

    print("opportunityId:", opportunityId)

    

    # ========= Getting new activites from fortellis =====

    # print("opportunityId:", opportunityId)

    token = get_token(subscription_id)
    activities = get_activities(opportunityId, customerId, token, subscription_id)
    currDate = datetime.now()
    docToUpdate = {
        "scheduledActivities": activities.get("scheduledActivities", []),
        "completedActivities": activities.get("completedActivities", []),
        "updated_at": currDate
    }
    opportunity.update(docToUpdate)
    esClient.update(index="opportunities", id=opportunityId, doc=opportunity)
    # continue

    # ====================================================


    # getting customer email & info
    customer_emails = customer.get('emails', [])
    customer_email = None
    for email in customer_emails:
        if email.get('doNotEmail') or not email.get('isPreferred'):
            continue
        customer_email = email['address']
        break

    customer_name = customer.get("firstName") or "there"

    # getting primary sales person
    salesTeam = opportunity.get('salesTeam', [])
    salesPersonObj = None
    for sales in salesTeam:
        if not sales.get('isPrimary'):
            continue
        salesPersonObj = sales
        break

    first_name = (salesPersonObj.get("firstName") or "").strip()
    last_name  = (salesPersonObj.get("lastName") or "").strip()
    full_name  = (first_name + " " + last_name).strip()

    salesperson = (
        SALES_PERSON_MAP.get(first_name)
        or SALES_PERSON_MAP.get(full_name)
        or full_name
        or "our team"
    )

    source = opportunity.get("source", "")
    sub_source = opportunity.get("subSource", "")

    # --- Rooftop resolution (from Subscription-Id) ---
    rt = get_rooftop_info(subscription_id)
    rooftop_name   = rt.get("name")   or "Patterson Auto Group"
    rooftop_sender = rt.get("sender") or TEST_FROM
    rooftop_addr   = rt.get("address") or ""

    dealership = (
        DEALERSHIP_MAP.get(first_name)
        or DEALERSHIP_MAP.get(full_name)
        or DEALERSHIP_MAP.get(source)
        or DEALERSHIP_MAP.get(sub_source)
        or rooftop_name
    )



    # === Vehicle & SRP link =============================================
    soughtVehicles = opportunity.get('soughtVehicles', [])
    vehicleObj = None
    for vehicle in soughtVehicles:
        if not vehicle.get('isPrimary'):
            continue
        vehicleObj = vehicle
        break

    make  = vehicleObj.get("make", "")
    model = vehicleObj.get("model", "")
    year  = vehicleObj.get("yearFrom", "")
    trim  = vehicleObj.get("trim", "")
    stock = vehicleObj.get("stockNumber", "")

    vehicle_str = f"{year} {make} {model} {trim}".strip() or "one of our vehicles"
    base_url = DEALERSHIP_URL_MAP.get(dealership)
    if base_url and (make and model):
        vehicle_str = f'<a href="{base_url}?make={make}&model={model}">{vehicle_str}</a>'

    

    completedActivities = opportunity.get('completedActivities', [])
    # scheduledActivities = opportunity.get('scheduledActivities', [])

    patti_already_contacted = checkedDict.get('patti_already_contacted', False)

    if not patti_already_contacted:

        firstActivity = getFirstActivity(completedActivities)
        opportunity['firstActivity'] = firstActivity
        if firstActivity:
            token = get_token(subscription_id)
            firstActivityFull = get_activity_by_id_v1(firstActivity['activityId'], token, subscription_id)
            opportunity['firstActivityFull'] = firstActivityFull
            firstActivityMessageBody = firstActivityFull.get('message', {}).get('body', '')
            firstActivityAdfDict = adf_to_dict(firstActivityMessageBody)
            opportunity['firstActivityAdfDict'] = firstActivityAdfDict
            inquiry_text_body = getInqueryUsingAdf(firstActivityAdfDict)
            opportunity['inquiry_text_body'] = inquiry_text_body
            customerFirstMsgDict: dict = getCustomerMsgDict(inquiry_text_body)
            opportunity['customerFirstMsgDict'] = customerFirstMsgDict
            if "alreadyProcessedActivities" in opportunity:
                opportunity["alreadyProcessedActivities"][firstActivity['activityId']] = firstActivityFull
            else:
                opportunity["alreadyProcessedActivities"] = {}
                opportunity["alreadyProcessedActivities"][firstActivity['activityId']] = firstActivityFull
            


            try:
                inquiry_text = customerFirstMsgDict.get('customerMsg', None)
            except:
                pass

            # TODO: check with kristin if need to add activity logic to crm that patti will not used here
            if customerFirstMsgDict.get('salesAlreadyContact', False):
                opportunity['isActive'] = False
                opportunity['checkedDict']['is_sales_contacted'] = True
                esClient.update(index="opportunities", id=opportunityId, doc=opportunity)

                wJson(opportunity, f"jsons/process/{opportunityId}.json")

                return

        # === Compose with GPT ===============================================
        fallback_mode = not inquiry_text or inquiry_text.strip().lower() in ["", "request a quote", "interested", "info", "information", "looking"]

        if fallback_mode:
            prompt = f"""
        Your job is to write personalized, dealership-branded emails from Patti, a friendly virtual assistant.
        The guest submitted a lead through {source}. They’re interested in: {vehicle_str}. Salesperson: {salesperson}
        They didn’t leave a detailed message.

        Please write a warm, professional email reply that:
        - Begin with exactly `Hi {customer_name},`
        - Start with 1–2 appealing vehicle features or dealership Why Buys
        - Welcome the guest and highlight our helpfulness
        - Invite specific questions or preferences
        - Mention the salesperson by name

        Do not include any signature, dealership contact block, address, phone number, or URL in your reply; I will append it.

        """
        else:
            prompt = f"""
        Your job is to write personalized, dealership-branded emails from Patti, a friendly virtual assistant.

        When writing:
        - Begin with exactly `Hi {customer_name},`
        - Lead with value (features / Why Buy)
        - If a specific vehicle is mentioned, answer directly and link if possible
        - If a specific question exists, answer it first
        - Include the salesperson’s name
        - Keep it warm, clear, and human

        Info (may None):
        - salesperson’s name: {salesperson}
        - vehicle: {vehicle_str}


        Guest inquiry:
        \"\"\"{inquiry_text}\"\"\"

        Do not include any signature, dealership contact block, address, phone number, or URL in your reply; I will append it.
        """
            
        # === Inventory recommendations =====================================

        # Get live inventory XML
        try:
            inventory_xml = get_vehicle_inventory_xml("Patterson2", "FjX^PGwk63", "ZE", "ZE7")
        except Exception as e:
            # log.warning(f"❌ Could not retrieve inventory XML: {e}")
            inventory_xml = None

        # 🔁 Use the same inquiry text you already computed.
        # If it's empty (fallback mode), feed a lightweight hint from the parsed vehicle fields.
        if inquiry_text and inquiry_text.strip():
            customer_email_text = inquiry_text
        else:
            # minimal hint so the matcher can still try (e.g., "Honda Pilot 2021 SUV")
            hint_bits = [str(year or "").strip(), (make or "").strip(), (model or "").strip(), (trim or "").strip()]
            customer_email_text = " ".join([b for b in hint_bits if b]) or "SUV car"
        
        recommendation_text = ""
        if inventory_xml:
            try:
                recommendation_text = recommend_from_xml(inventory_xml, customer_email_text).strip()
                if recommendation_text:
                    prompt += f"\n\nInventory suggestions to include:\n{recommendation_text}\n"
                    # log.info("✅ Added inventory suggestions to prompt.")
            except Exception as e:
                pass
                # log.warning(f"Recommendation failed: {e}")
            
        response  = run_gpt(prompt, customer_name, rooftop_name)
        subject   = response["subject"]
        body_html = response["body"]

        body_html = re.sub(
            r"(?is)(?:\n\s*)?patti\s*(?:\r?\n)+virtual assistant.*?$",
            "",
            body_html
        )

        opportunity["body_html"] = body_html

        if "messages" in opportunity:
            opportunity['messages'].append(
                {
                    "msgFrom": "patti",
                    "subject": subject,
                    "body": body_html,
                    "date": currDate
                }
            )
        else:
            opportunity['messages'] = [
                {
                    "msgFrom": "patti",
                    "subject": subject,
                    "body": body_html,
                    "date": currDate
                }
            ]
        opportunity['checkedDict']['patti_already_contacted'] = True
        # TODO: fix in which line
        opportunity['checkedDict']['last_msg_by'] = "patti"
        opportunity['followUP_date'] = currDate
        opportunity['followUP_count'] = 0
        esClient.update(index="opportunities", id=opportunityId, doc=opportunity)
    else:
        # handle follow-ups messages
        checkActivities(opportunity, currDate, rooftop_name)

    followUP_date = opportunity['followUP_date']
    followUP_date = datetime.fromisoformat(followUP_date)
    followUP_count = opportunity['followUP_count']

    if followUP_date <= currDate and followUP_count > 3:
        opportunity['isActive'] = False
        esClient.update(index="opportunities", id=opportunityId, doc=opportunity)        
        wJson(opportunity, f"jsons/process/{opportunityId}.json")
        return
    elif followUP_date <= currDate:
        messages = opportunity['messages']
        prompt = f"""
        generate next patti reply which is a follow-up message, here is the current messages between patti and the customer (python list of dicts):
        {messages}
        """
        response = run_gpt(
        prompt,
        customer_name,
        rooftop_name,
        prevMessages= True)

        subject   = response["subject"]
        body_html = response["body"]

        body_html = re.sub(
            r"(?is)(?:\n\s*)?patti\s*(?:\r?\n)+virtual assistant.*?$",
            "",
            body_html
        )

        opportunity['messages'].append(
            {
                "msgFrom": "patti",
                "subject": subject,
                "body": body_html,
                "date": currDate,
                "action": response.get("action"),
                "notes": response.get("notes")
            }
        )

        # TODO: fix in which line
        opportunity['checkedDict']['last_msg_by'] = "patti"

        opportunity['followUP_date'] = currDate
        opportunity['followUP_count'] += 1
        esClient.update(index="opportunities", id=opportunityId, doc=opportunity)

    
    
    wJson(opportunity, f"jsons/process/{opportunityId}.json")




if __name__ == "__main__":

    # data = getNewData()
    data = getNewDataByDate("2025-10-31")

    for i,hit in enumerate(data):
        processHit(hit)
        if i == 0:
            exit()

    # hit = rJson("jsons/process/d43f2231-1ab5-f011-814f-00505690ec8c.json")
    # processHit(hit)
    

    
