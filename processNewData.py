from helpers import (
    rJson,
    wJson,
    getFirstActivity,
    adf_to_dict,
    getInqueryUsingAdf,
    get_names_in_dir,
    sortActivities
)
from kbb_ico import process_kbb_ico_lead  # ADD

from esQuerys import getNewData, esClient, getNewDataByDate
from rooftops import get_rooftop_info
from constants import *
from gpt import run_gpt, getCustomerMsgDict
import re
import logging

from uuid import uuid4

from fortellis import get_activities, get_token, get_activity_by_id_v1, get_opportunity
from fortellis import get_activities, get_token, get_activity_by_id_v1, get_opportunity, add_opportunity_comment


#from fortellis import get_vehicle_inventory_xml  
from inventory_matcher import recommend_from_xml

from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv
load_dotenv()

log = logging.getLogger(__name__)
OFFLINE_MODE = os.getenv("OFFLINE_MODE", "0").lower() in ("1", "true", "yes")

EXIT_KEYWORDS = [
    "not interested", "no longer interested", "bought elsewhere",
    "already purchased", "stop emailing", "unsubscribe",
    "please stop", "no thanks", "do not contact",
    "leave me alone", "sold my car", "found another dealer"
]

def is_exit_message(msg: str) -> bool:
    if not msg:
        return False
    msg_low = msg.lower()
    return any(k in msg_low for k in EXIT_KEYWORDS)


# TODO:
# add all actions to crm

already_processed = get_names_in_dir("jsons/process")
DEBUGMODE = os.getenv("DEBUGMODE", "1") == "1"

def is_active_opp(opportunity: dict) -> bool:
    # Fortellis opp payloads typically include a status or flags you can check.
    status = (opportunity.get("status") or "").strip().lower()
    # Some payloads have booleans like "isActive" or "isClosed"
    is_active_flag = opportunity.get("isActive")
    is_closed_flag = opportunity.get("isClosed")

    if isinstance(is_active_flag, bool):
        return is_active_flag
    if isinstance(is_closed_flag, bool):
        return not is_closed_flag

    # Fallback on status text
    return status in {"open", "active", "in progress"}

def _is_assigned_to_kristin(doc: dict) -> bool:
    """
    Return True if Kristin Nowzek appears on the sales team by name or email.
    """
    sales = (doc.get("salesTeam") or [])
    log.debug(
        "Assign check: %s",
        [{"fn": (m.get("firstName") or ""), "ln": (m.get("lastName") or ""), "em": (m.get("email") or "")}
         for m in sales]
    )
    for m in sales:
        fn = (m.get("firstName") or "").strip().lower()
        ln = (m.get("lastName") or "").strip().lower()
        em = (m.get("email") or "").strip().lower()
        if (fn == "kristin" and ln == "nowzek") or em in {
            "knowzek@pattersonautos.com", "knowzek@gmail.com"
        }:
            return True
    return False



def checkActivities(opportunity, currDate, rooftop_name):
    if OFFLINE_MODE:
        activities = opportunity.get('completedActivitiesTesting', [])
    else:
        activities = opportunity.get('completedActivities', [])
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
    
        if not token and not DEBUGMODE and not OFFLINE_MODE:
            token = get_token(subscription_id)
        
        comments = (act.get("comments") or "")
        activityName = (act.get("activityName") or "").strip().lower()
        activityType = act.get("activityType")
    
        # 1) Our sentinel in any prior comment?
        if PATTI_FIRST_REPLY_SENTINEL in comments:
            checkedDict["patti_already_contacted"] = True
            continue
    
        if activityName == "read email" or activityType == 20:
            fullAct = act
            if not DEBUGMODE and not OFFLINE_MODE:
                fullAct = get_activity_by_id_v1(activityId, token, subscription_id)
    
            customerMsg = (fullAct.get('message') or {})
            customerMsgDict = {
                "msgFrom": "customer",
                "customerName": customerInfo.get('firstName'),
                "subject": customerMsg.get('subject'),
                "body": customerMsg.get('body'),
                "date": fullAct.get('completedDate')
            }
            
            # append the customer's message to the thread
            opportunity.setdefault('messages', []).append(customerMsgDict)
            messages = opportunity['messages']
            checkedDict["last_msg_by"] = "customer"
            opportunity['checkedDict'] = checkedDict  # ensure persisted even if it was missing
            
            # 🚫 Early-exit check — stop if customer declined
            customer_body = (customerMsg.get('body') or '').strip()
            if is_exit_message(customer_body):
                log.info("Customer opted out or declined interest. Marking opportunity inactive.")
                opportunity['isActive'] = False
                checkedDict['exit_reason'] = customer_body[:120]
                checkedDict['exit_type'] = "customer_declined"
                opportunity['checkedDict'] = checkedDict
            
                # mark this activity as processed so we don't re-handle it next run
                opportunity.setdefault('alreadyProcessedActivities', {})[activityId] = fullAct
            
                if not OFFLINE_MODE:
                    add_opportunity_comment(
                        token, subscription_id, opportunity['opportunityId'],
                        f"[Patti Exit] Customer indicated no interest: “{customer_body[:200]}”"
                    )

                    try:
                        from fortellis import set_opportunity_inactive
                        set_opportunity_inactive(
                            token,
                            subscription_id,
                            opportunity['opportunityId'],
                            sub_status="Not In Market",
                            comments="Customer declined — set inactive by Patti"
                        )
                    except Exception as e:
                        log.warning("set_opportunity_inactive failed: %s", e)
        
                    # Optional: notify salesperson
                    sales_team = opportunity.get('salesTeam', [])
                    if sales_team:
                        first_sales = sales_team[0]
                        salesperson_email = (first_sales.get('email') or "").strip()
                        if salesperson_email:
                            try:
                                from emailer import send_email
                                send_email(
                                    to=salesperson_email,
                                    subject="Patti stopped follow-up — customer declined",
                                    body=f"The customer replied:\n\n{customer_body}\n\nPatti stopped follow-ups for this lead."
                                )
                            except Exception as e:
                                log.warning("Emailer failed: %s", e)
            
                    esClient.update(index="opportunities", id=opportunity['opportunityId'], doc=opportunity)
            
                wJson(opportunity, f"jsons/process/{opportunity['opportunityId']}.json")
                return
            
            # ✅ continue with GPT reply generation
            prompt = f"""
            generate next patti reply, here is the current messages between patti and the customer (python list of dicts):
            {messages}
            """
            response = run_gpt(
                prompt,
                customerInfo.get('firstName'),
                rooftop_name,
                prevMessages=True
            )
            
            subject = response["subject"]
            body_html = response["body"]
            
            body_html = re.sub(
                r"(?is)(?:\n\s*)?patti\s*(?:\r?\n)+virtual assistant.*?$",
                "",
                body_html
            )
            
            opportunity['messages'].append({
                "msgFrom": "patti",
                "subject": subject,
                "body": body_html,
                "date": currDate,
                "action": response.get("action"),
                "notes": response.get("notes"),
            })
            
            checkedDict['last_msg_by'] = "patti"
            opportunity['checkedDict'] = checkedDict
            
            # mark processed to avoid repeat handling
            opportunity.setdefault('alreadyProcessedActivities', {})[activityId] = fullAct
            
            nextDate = currDate + timedelta(hours=24)   # or use a constant
            opportunity['followUP_date'] = nextDate.isoformat()
            opportunity['followUP_count'] = 0




def processHit(hit):
    currDate = datetime.now()

    # remove later
    global already_processed

    inquiry_text = None  # ensure defined

    try:
        opportunity : dict = hit['_source']
    except:
        opportunity : dict = hit
        
    # ✅ Only run on Kristin-assigned opps
    if not _is_assigned_to_kristin(opportunity):
        # optional log to verify the filter is working
        log.info("Skip opp %s (not assigned to Kristin)",
                 opportunity.get('opportunityId'))
        return

    if not opportunity.get('isActive', True):
        print("pass...")
        return

    subscription_id = opportunity['_subscription_id']
    opportunityId = opportunity['opportunityId']

    # --- Normalize testing arrays so live runs never use them for logic ---
    if OFFLINE_MODE:
        opp_messages = (opportunity.get("completedActivitiesTesting")
                        or opportunity.get("messages") or [])
    else:
        opportunity.pop("completedActivitiesTesting", None)
        # keep messages only for display/logs; don't base behavior on it
        opp_messages = []


    checkedDict = opportunity.get("checkedDict", {})

    # remove it later
    # if f"{opportunityId}.json" in already_processed:
    #     return

    customer = opportunity['customer']
    customerId = customer['id']

    print("opportunityId:", opportunityId)

    

    # ========= Getting new activites from fortellis =====

    # print("opportunityId:", opportunityId)
    
    if OFFLINE_MODE:
        local_completed = opportunity.get("completedActivitiesTesting", []) or []
        activities = {"scheduledActivities": [], "completedActivities": local_completed}
    else:
        token = get_token(subscription_id)
        activities = get_activities(opportunityId, customerId, token, subscription_id)


    # Safety: if anything upstream handed us a list, coerce to the dict shape we expect
    if isinstance(activities, list):
        activities = {"scheduledActivities": [], "completedActivities": activities}

    currDate = datetime.now()
    docToUpdate = {
        "scheduledActivities": activities.get("scheduledActivities", []),
        "completedActivities": activities.get("completedActivities", []),
        "updated_at": currDate
    }
    opportunity.update(docToUpdate)

    # Ensure test arrays never land in ES in live mode
    if not OFFLINE_MODE:
        opportunity.pop("completedActivitiesTesting", None)
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
    
    # 🔒 Fresh active-check from Fortellis (ES can be stale)
    try:
        tok_for_check = get_token(subscription_id) if not OFFLINE_MODE else None
        fresh_opp = get_opportunity(opportunityId, tok_for_check, subscription_id) if not OFFLINE_MODE else opportunity
    except Exception as e:
        # If we can’t fetch the opp, skip gracefully and don’t risk writes
        log.warning("Skipping opp %s (get_opportunity failed): %s", opportunityId, str(e)[:200])
        # Optional: mark in ES so we don’t keep retrying
        if not OFFLINE_MODE:
            esClient.update(index="opportunities", id=opportunityId, doc={
                "patti": {"skip": True, "skip_reason": "get_opportunity_failed"}
            })
        return

    if not is_active_opp(fresh_opp):
        log.info("Skipping opp %s (inactive from Fortellis).", opportunityId)
        # Mark in ES so future runs don’t retry
        if not OFFLINE_MODE:
            esClient.update(index="opportunities", id=opportunityId, doc={
                "patti": {"skip": True, "skip_reason": "inactive_opportunity"}
            })
        return

    # === Persona routing: treat Kristin's opps as KBB ICO =======================
    # TEMP test gate: only flip to KBB mode for opps assigned to Kristin
    if _is_assigned_to_kristin(opportunity):
        # Lead age (7-day window logic can use this inside kbb_ico)
        lead_age_days = 0
        created_raw = (
            opportunity.get("createdDate")
            or opportunity.get("created_at")               # ES-stamped when ingested
            or (opportunity.get("firstActivity", {}) or {}).get("completedDate")
        )
        try:
            if created_raw:
                created_dt = datetime.fromisoformat(str(created_raw).replace("Z", "+00:00"))
                lead_age_days = (datetime.now(timezone.utc) - created_dt).days
        except Exception:
            pass

        # For logs/visibility
        src_join = f"{source} {sub_source}".strip().lower()
        log.info("Persona route: mode=%s lead_age_days=%s src=%s",
                 "kbb_ico", lead_age_days, (src_join[:120] if src_join else "<none>"))

        # Try to surface any inquiry text we may already have; safe default to ""
        inquiry_text_safe = (opportunity.get("inquiry_text_body") or "").strip()

        # Hand off to the KBB ICO flow (templates + stop-on-reply convo)
        try:
            tok = None
            if not OFFLINE_MODE:
                tok = get_token(subscription_id)
            
            process_kbb_ico_lead(
                opportunity=opportunity,
                lead_age_days=lead_age_days,
                rooftop_name=rooftop_name,
                inquiry_text=inquiry_text_safe,
                token=tok,                               # ✅ pass a live token
                subscription_id=subscription_id,
                SAFE_MODE=os.getenv("SAFE_MODE", "1") in ("1","true","True"),
                rooftop_sender=rooftop_sender,
            )
        except Exception as e:
            log.error("KBB ICO handler failed for opp %s: %s", opportunityId, e)

        # Persist any updates (messages/state) and exit this hit
        if not OFFLINE_MODE:
            esClient.update(index="opportunities", id=opportunityId, doc=opportunity)
        wJson(opportunity, f"jsons/process/{opportunityId}.json")
        return
    # ===========================================================================



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

    
    completedActivities = activities.get('completedActivities', [])
    # completedActivities = opportunity.get('completedActivities', [])
    # scheduledActivities = opportunity.get('scheduledActivities', [])

    patti_already_contacted = checkedDict.get('patti_already_contacted', False)

    if not patti_already_contacted:

        firstActivity = getFirstActivity(completedActivities)
        opportunity['firstActivity'] = firstActivity
    
        if firstActivity:
            firstActivityFull = None  # define up front for both branches
    
            if not OFFLINE_MODE:
                token = get_token(subscription_id)
                firstActivityFull = get_activity_by_id_v1(firstActivity['activityId'], token, subscription_id)
                firstActivityMessageBody = (firstActivityFull.get('message') or {}).get('body', '') or ''
            else:
                # OFFLINE: derive a body from newest local activity
                # Prefer the last completed activity we just built above
                newest = (completedActivities[-1] if completedActivities else {}) or {}
                msg = newest.get("message") or {}
    
                # Some offline items only have 'notes'
                firstActivityMessageBody = (msg.get("body") or newest.get("notes") or "").strip()
    
                # Create an offline "full" act so the rest of the code can store it
                import uuid, datetime as _dt
                firstActivityFull = {
                    "activityId": newest.get("activityId") or newest.get("id") or f"offline-{uuid.uuid4().hex[:8]}",
                    "completedDate": newest.get("completedDate") or _dt.datetime.utcnow().isoformat() + "Z",
                    "message": {"subject": newest.get("subject", ""), "body": firstActivityMessageBody},
                    "activityType": newest.get("activityType", 20),
                    "activityName": newest.get("activityName", "Read Email"),
                }
    
                # Keep firstActivity in sync with the id we will store
                firstActivity['activityId'] = firstActivityFull['activityId']
    
            # Parse the first message into ADF → plain text inquiry
            #firstActivityAdfDict = adf_to_dict(firstActivityMessageBody or "")
            #opportunity['firstActivityAdfDict'] = firstActivityAdfDict
            #inquiry_text_body = getInqueryUsingAdf(firstActivityAdfDict) or ""
            #opportunity['inquiry_text_body'] = inquiry_text_body

            raw_body = (firstActivityMessageBody or "").strip()
            
            def _looks_like_adf(s: str) -> bool:
                s0 = s.lstrip()
                return s0.startswith("<") and ("<adf" in s0.lower() or "<customer" in s0.lower())
            
            inquiry_text_body = ""
            firstActivityAdfDict = {}
            
            if raw_body and _looks_like_adf(raw_body):
                try:
                    firstActivityAdfDict = adf_to_dict(raw_body)
                    inquiry_text_body = getInqueryUsingAdf(firstActivityAdfDict) or ""
                except Exception:
                    # Not valid ADF—fallback to plaintext by stripping tags
                    inquiry_text_body = re.sub(r"<[^>]+>", "", raw_body)
            else:
                # Not XML/ADF—use plaintext (strip any HTML tags)
                inquiry_text_body = re.sub(r"<[^>]+>", "", raw_body)
            
            opportunity['firstActivityAdfDict'] = firstActivityAdfDict
            opportunity['inquiry_text_body'] = inquiry_text_body


    
            customerFirstMsgDict: dict = getCustomerMsgDict(inquiry_text_body)
            opportunity['customerFirstMsgDict'] = customerFirstMsgDict

            
            # Record this activity as processed (safe handling for list/dict types)
            apa = opportunity.get("alreadyProcessedActivities")
            if isinstance(apa, list):
                # Convert list of activities to dict keyed by activityId/id/index
                apa = {
                    str((a or {}).get("activityId") or (a or {}).get("id") or i): (a or {})
                    for i, a in enumerate(apa)
                    if isinstance(a, dict)
                }
            elif not isinstance(apa, dict) or apa is None:
                apa = {}
            opportunity["alreadyProcessedActivities"] = apa
            
            # Build a reliable key for this activity
            act_id = str(
                (firstActivity or {}).get("activityId")
                or (firstActivityFull or {}).get("activityId")
                or (firstActivity or {}).get("id")
                or (firstActivityFull or {}).get("id")
                or f"unknown-{uuid4().hex}"
            )
            
            # Save this activity under that key
            opportunity["alreadyProcessedActivities"][act_id] = firstActivityFull or firstActivity or {}

            # --- ensure the seeded customer message exists and is visible to the UI ---

            raw_inquiry = (opportunity.get('inquiry_text_body') or "").strip()
            if not raw_inquiry:
                raw_inquiry = (
                    ((firstActivityFull or {}).get('message', {}) or {}).get('subject', '') or
                    (firstActivityFull or {}).get('notes') or
                    (firstActivity or {}).get('title') or
                    "Hi! I'm interested in this vehicle and had a few questions."
                ).strip()
            
            # Find or initialize conversation array
            conv = (opportunity.get('messages')
                    or opportunity.get('conversation')
                    or opportunity.get('thread')
                    or [])
            if not isinstance(conv, list):
                conv = []
            
            # Append seed message if missing
            already = any(
                isinstance(m, dict) and m.get('role') == 'customer' and m.get('source') == 'seed'
                for m in conv
            )
            if not already:
                conv.append({
                    "id": f"cust-{act_id}",
                    "role": "customer",
                    "text": raw_inquiry,
                    "source": "seed",
                    "createdAt": currDate.isoformat()
                })
            
            # Write back to all likely keys so the UI sees it
            opportunity['messages'] = conv
            opportunity['conversation'] = conv
            opportunity['thread'] = conv
            
            # Optional: log for debugging
            print(f"[SEED] Added seed customer message. len={len(conv)} act_id={act_id}")

   

            try:
                inquiry_text = customerFirstMsgDict.get('customerMsg', None)
            except:
                pass

            # TODO: check with kristin if need to add activity logic to crm that patti will not used here
            if customerFirstMsgDict.get('salesAlreadyContact', False):
                opportunity['isActive'] = False
                opportunity['checkedDict']['is_sales_contacted'] = True
                if not OFFLINE_MODE:
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
        # NOTE: when you need to use just uncomment and uncomment in import section also
        # try:
        #     inventory_xml = get_vehicle_inventory_xml("Patterson2", "FjX^PGwk63", "ZE", "ZE7")
        # except Exception as e:
        #     # log.warning(f"❌ Could not retrieve inventory XML: {e}")
        #     inventory_xml = None

        # 🔁 Use the same inquiry text you already computed.
        # If it's empty (fallback mode), feed a lightweight hint from the parsed vehicle fields.
        if inquiry_text and inquiry_text.strip():
            customer_email_text = inquiry_text
        else:
            # minimal hint so the matcher can still try (e.g., "Honda Pilot 2021 SUV")
            hint_bits = [str(year or "").strip(), (make or "").strip(), (model or "").strip(), (trim or "").strip()]
            customer_email_text = " ".join([b for b in hint_bits if b]) or "SUV car"
        
        recommendation_text = ""

        # NOTE: (cont with line: 523)when you need to use just uncomment and uncomment in import section also
        # if inventory_xml:
        #     try:
        #         recommendation_text = recommend_from_xml(inventory_xml, customer_email_text).strip()
        #         if recommendation_text:
        #             prompt += f"\n\nInventory suggestions to include:\n{recommendation_text}\n"
        #             # log.info("✅ Added inventory suggestions to prompt.")
        #     except Exception as e:
        #         pass
        #         # log.warning(f"Recommendation failed: {e}")
            
        response  = run_gpt(prompt, customer_name, rooftop_name)
        subject   = response["subject"]
        body_html = response["body"]

        body_html = re.sub(
            r"(?is)(?:\n\s*)?patti\s*(?:\r?\n)+virtual assistant.*?$",
            "",
            body_html
        )
        from kbb_ico import _PREFS_RE
        body_html = _PREFS_RE.sub("", body_html).strip()
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
        opportunity['checkedDict']['last_msg_by'] = "patti"
        nextDate = currDate + timedelta(hours=24)   # or use your cadence
        opportunity['followUP_date'] = nextDate.isoformat()
        opportunity['followUP_count'] = 0
        if not OFFLINE_MODE:
            esClient.update(index="opportunities", id=opportunityId, doc=opportunity)
    else:
        # handle follow-ups messages
        checkActivities(opportunity, currDate, rooftop_name)

    fud = opportunity.get('followUP_date')
    followUP_date = datetime.fromisoformat(fud) if isinstance(fud, str) else (fud if isinstance(fud, datetime) else currDate)
    followUP_count = opportunity['followUP_count']

    last_by = (opportunity.get('checkedDict') or {}).get('last_msg_by', '')
    if followUP_date <= currDate and followUP_count > 3:
        opportunity['isActive'] = False
        if not OFFLINE_MODE:
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

        nextDate = currDate + timedelta(days=1)
        opportunity['followUP_date'] = nextDate.isoformat()
        opportunity['followUP_count'] += 1
        if not OFFLINE_MODE:
            esClient.update(index="opportunities", id=opportunityId, doc=opportunity)

    
    
    wJson(opportunity, f"jsons/process/{opportunityId}.json")


# ---- Rolling ES lookback window (default 6 days) ----
LOOKBACK_DAYS = int(os.getenv("ES_LOOKBACK_DAYS", "6"))

if __name__ == "__main__":
    if not OFFLINE_MODE:
        # Start date = now - LOOKBACK_DAYS (UTC), formatted YYYY-MM-DD
        
        start_date = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).date().isoformat()
        log.info("ES lookback start_date=%s (last %s days)", start_date, LOOKBACK_DAYS)

        data = getNewDataByDate(start_date)

        # Process ALL hits (no early exit)
        for hit in data:
            processHit(hit)
    # playground drives processHit() via Flask; nothing to run here when OFFLINE_MODE=1

    

    
