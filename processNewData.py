from helpers import (
    rJson,
    wJson,
    getFirstActivity,
    adf_to_dict,
    getInqueryUsingAdf,
    get_names_in_dir,
    sortActivities
)
from kbb_ico import process_kbb_ico_lead 
from es_resilient import es_update_with_retry
from esQuerys import getNewData, esClient, getNewDataByDate
from rooftops import get_rooftop_info
from constants import *
from gpt import run_gpt, getCustomerMsgDict, extract_appt_time
import re
import logging
import hashlib, json, time
from uuid import uuid4
import uuid

from fortellis import (
    get_activities,
    get_token,
    get_activity_by_id_v1,
    get_opportunity,
    add_opportunity_comment,
    schedule_activity,
)

from patti_common import fmt_local_human
#from fortellis import get_vehicle_inventory_xml  
from inventory_matcher import recommend_from_xml

# from datetime import datetime, timedelta, timezone
from datetime import datetime as _dt, timedelta as _td, timezone as _tz
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


already_processed = get_names_in_dir("jsons/process")
DEBUGMODE = os.getenv("DEBUGMODE", "1") == "1"

def _lc(x): return str(x).strip().lower() if x is not None else ""
def _first_present_lc(doc, *keys):
    for k in keys:
        if doc and k in doc and doc[k] is not None:
            return _lc(doc[k])
    return ""
def _kbb_flags_from(opportunity_doc: dict, fresh_opp: dict | None) -> dict:
    src  = _first_present_lc(fresh_opp, "source") or _first_present_lc(opportunity_doc, "source")
    st   = _first_present_lc(fresh_opp, "status") or _first_present_lc(opportunity_doc, "status")
    sub  = (_first_present_lc(fresh_opp, "subStatus", "substatus")
            or _first_present_lc(opportunity_doc, "subStatus", "substatus"))
    upt  = (_first_present_lc(fresh_opp, "upType", "uptype")
            or _first_present_lc(opportunity_doc, "upType", "uptype"))
    return {"source": src, "status": st, "substatus": sub, "uptype": upt}

_KBB_SOURCES = {
    "kbb instant cash offer",
    "kbb servicedrive",
    "kbb service drive",
}

def _is_exact_kbb_source(val) -> bool:
    return (val or "").strip().lower() in _KBB_SOURCES

def _is_exact_kbb_ico_flags(flags: dict | None, es_doc: dict | None = None) -> bool:
    src_fortellis = ((flags or {}).get("source") or "").strip()
    src_es        = ((es_doc or {}).get("source") or "").strip()
    return _is_exact_kbb_source(src_fortellis) or _is_exact_kbb_source(src_es)


STATE_KEYS = ("mode", "last_template_day_sent", "nudge_count",
              "last_customer_msg_at", "last_agent_msg_at")

def _state_signature(state: dict) -> str:
    base = {k: state.get(k) for k in STATE_KEYS}
    blob = json.dumps(base, sort_keys=True, separators=(',', ':'))
    return hashlib.md5(blob.encode("utf-8")).hexdigest()


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


def _lc(x):
    return str(x).strip().lower() if x is not None else ""

def _first_present_lc(doc, *keys):
    for k in keys:
        if doc and k in doc and doc[k] is not None:
            return _lc(doc[k])
    return ""

def _kbb_flags_from(opportunity_doc: dict, fresh_opp: dict | None) -> dict:
    # prefer fresh_opp fields, fall back to opportunity doc
    src  = _first_present_lc(fresh_opp, "source")    or _first_present_lc(opportunity_doc, "source")
    st   = _first_present_lc(fresh_opp, "status")    or _first_present_lc(opportunity_doc, "status")
    sub  = (_first_present_lc(fresh_opp, "subStatus", "substatus")
            or _first_present_lc(opportunity_doc, "subStatus", "substatus"))
    upt  = (_first_present_lc(fresh_opp, "upType", "uptype")
            or _first_present_lc(opportunity_doc, "upType", "uptype"))
    return {"source": src, "status": st, "substatus": sub, "uptype": upt}

def _is_kbb_ico(doc_flags: dict) -> bool:
    return (
        doc_flags["source"] == "kbb instant cash offer" and
        doc_flags["status"] == "active" and
        doc_flags["substatus"] == "new" and
        doc_flags["uptype"] == "campaign"
    )


def _is_kbb_ico_new_active(doc: dict) -> bool:
    source    = _get_lc(doc, "source")
    status    = _get_lc(doc, "status")
    substatus = _get_lc(doc, "subStatus", "substatus")  # ← read both
    uptype    = _get_lc(doc, "upType", "uptype")        # ← read both
    
    print("KBB detect →", {"source": source, "status": status, "substatus": substatus, "uptype": uptype})

    return (
        source == "kbb instant cash offer" and
        status == "active" and
        substatus == "new" and
        uptype == "campaign"
    )


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
    
    alreadyProcessedActivities = opportunity.get('alreadyProcessedActivities', {})
    checkedDict = opportunity.get('checkedDict', {})
    subscription_id = opportunity.get('_subscription_id')
    messages = opportunity.get("messages", [])
    customerInfo = opportunity.get('customer', {})

    # Get a single token for this function, if needed
    if OFFLINE_MODE or DEBUGMODE:
        token = None
    else:
        token = get_token(subscription_id)

    for act in activities:
        activityId = act.get("activityId")
        if activityId in alreadyProcessedActivities:
            continue

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
            
            # 🚫 Unified opt-out / decline check — stop if customer declined
            from patti_common import _is_optout_text, _is_decline

            customer_body = (customerMsg.get('body') or '').strip()
            if _is_optout_text(customer_body) or _is_decline(customer_body):
                log.info("Customer opted out or declined interest. Marking opportunity inactive.")
                opportunity['isActive'] = False
                checkedDict['exit_reason'] = customer_body[:250]
                checkedDict['exit_type'] = "customer_declined"
                opportunity['checkedDict'] = checkedDict

                # mark this activity as processed so we don't re-handle it next run
                opportunity.setdefault('alreadyProcessedActivities', {})[activityId] = fullAct

                if not OFFLINE_MODE:
                    # Add a clear exit comment in CRM
                    add_opportunity_comment(
                        token,
                        subscription_id,
                        opportunity['opportunityId'],
                        f"[Patti Exit] Customer indicated no interest / opt-out: “{customer_body[:200]}”"
                    )

                    try:
                        from fortellis import (
                            set_opportunity_inactive,
                            set_customer_do_not_email,
                        )

                        # Set opp to Not In Market
                        set_opportunity_inactive(
                            token,
                            subscription_id,
                            opportunity['opportunityId'],
                            sub_status="Not In Market",
                            comments="Customer declined — set inactive by Patti",
                        )

                        # Mirror KBB behavior: mark customer as Do Not Email
                        cust = (opportunity.get("customer") or {})
                        customer_id = cust.get("id")
                        emails = cust.get("emails") or []
                        email_address = (
                            next((e for e in emails if e.get("isPreferred")), emails[0])
                            if emails else {}
                        ).get("address")

                        if customer_id and email_address:
                            set_customer_do_not_email(
                                token,
                                subscription_id,
                                customer_id,
                                email_address,
                                do_not=True,
                            )

                    except Exception as e:
                        log.warning("CRM inactive/DoNotEmail failed (general decline): %s", e)

                    # Optional: notify salesperson (keep existing behavior)
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
                                    body=(
                                        "The customer replied:\n\n"
                                        f"{customer_body}\n\n"
                                        "Patti stopped follow-ups for this lead."
                                    ),
                                )
                            except Exception as e:
                                log.warning("Emailer failed: %s", e)

                    # Persist to ES
                    es_update_with_retry(
                        esClient,
                        index="opportunities",
                        id=opportunity['opportunityId'],
                        doc=opportunity,
                    )

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
            
            nextDate = currDate + _td(hours=24)   # or use a constant
            opportunity['followUP_date'] = nextDate.isoformat()
            opportunity['followUP_count'] = 0




def processHit(hit):
    currDate = _dt.now()

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

    # Reuse a single token for this whole processHit run
    if OFFLINE_MODE:
        token = None
    else:
        token = get_token(subscription_id)

    # EARLY SKIP: avoid API calls for opps already marked inactive
    patti_meta = opportunity.get("patti") or {}
    if patti_meta.get("skip") and patti_meta.get("skip_reason") == "inactive_opportunity":
        log.info("Skipping opp %s (inactive_opportunity in ES).", opportunityId)
        return


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


    # getting customer email & info
    customer_emails = customer.get('emails', [])
    customer_email = None
    for email in customer_emails:
        if email.get('doNotEmail') or not email.get('isPreferred'):
            continue
        customer_email = email['address']
        break

    customer_name = customer.get("firstName") or "there"

    # --- Getting primary salesperson (robust) ---
    salesTeam = opportunity.get("salesTeam") or []
    if not isinstance(salesTeam, list):
        salesTeam = []
    
    salesPersonObj = None
    for sales in salesTeam:
        if not isinstance(sales, dict):
            continue
        if str(sales.get("isPrimary")).lower() in ("true", "1", "yes"):
            salesPersonObj = sales
            break
    
    # fallback if nothing found
    if not isinstance(salesPersonObj, dict):
        log.warning("No valid primary salesperson found for opp_id=%s", opportunity.get("id"))
        salesPersonObj = (salesTeam[0] if salesTeam and isinstance(salesTeam[0], dict) else {})
    
    first_name = (salesPersonObj.get("firstName") or "").strip()
    last_name  = (salesPersonObj.get("lastName") or "").strip()
    full_name  = (f"{first_name} {last_name}").strip()
    
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
        # token was fetched once at the top of processHit
        fresh_opp = get_opportunity(opportunityId, token, subscription_id) if not OFFLINE_MODE else opportunity

        # Clear any prior transient_error now that the fetch succeeded
        if not OFFLINE_MODE:
            es_update_with_retry(
                esClient,
                index="opportunities",
                id=opportunityId,
                doc={"patti": {"transient_error": None}}
            )


    except Exception as e:
        # Downgrade to a transient error so we retry next run (no hard skip)
        log.warning("Transient get_opportunity failure for %s: %s", opportunityId, str(e)[:200])
        if not OFFLINE_MODE:
            # increment a lightweight failure counter using what we already have in memory
            prev = (opportunity.get("patti") or {}).get("transient_error") or {}
            fail_count = (prev.get("count") or 0) + 1
            es_update_with_retry(
                esClient,
                index="opportunities",
                id=opportunityId,
                doc={
                    "patti": {
                        "transient_error": {
                            "code": "get_opportunity_failed",
                            "message": str(e)[:200],
                            "at": _dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "count": fail_count
                        },
                        # make sure we are NOT marking as a permanent skip
                        "skip": False,
                        "skip_reason": None
                    }
                }
            )
        # We can’t proceed without fresh_opp; exit gracefully and let the next run retry.
        return
    
    # keep this — we still skip inactive opps
    if not is_active_opp(fresh_opp):
        log.info("Skipping opp %s (inactive from Fortellis).", opportunityId)
        if not OFFLINE_MODE:
            es_update_with_retry(
                esClient,
                index="opportunities",
                id=opportunityId,
                doc={
                    "patti": {
                        "skip": True,
                        "skip_reason": "inactive_opportunity",
                        # clear any transient flag since this is a definitive state
                        "transient_error": None,
                        "inactive_at": _dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "inactive_snapshot": {
                            "status": fresh_opp.get("status"),
                            "subStatus": fresh_opp.get("subStatus"),
                            "isActive": fresh_opp.get("isActive")
                        }
                    }
                }
            )
        return

    
    # === KBB routing ===
    flags = _kbb_flags_from(opportunity, fresh_opp)
    log.info("KBB detect → %s", flags)
    
    # Early eligibility gate: pass if Kristin-assigned OR exact KBB (ICO/ServiceDrive)
    if not (_is_assigned_to_kristin(opportunity) or _is_exact_kbb_ico_flags(flags, opportunity)):
        log.info("Skip opp %s (neither Kristin-assigned nor exact KBB source)", opportunityId)
        return

    # --- BEGIN: ensure dateIn is present for cadence math (no external vars required) ---
    def _parse_iso_safe(s):
        try:
            return _dt.fromisoformat(str(s).replace("Z", "+00:00"))
        except Exception:
            return None
    
    def _to_iso_utc(dt):
        return dt.astimezone(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    if not opportunity.get("dateIn"):
        # Try to use a locally available activity history dict if present; otherwise fall back to fields on the opp.
        acts = None
        try:
            acts = activity_history  # optional; only if you actually have this in scope
        except NameError:
            acts = None
    
        # Pull completed/scheduled from the best available source
        if isinstance(acts, dict):
            comp  = acts.get("completedActivities")  or []
            sched = acts.get("scheduledActivities")  or []
        else:
            comp  = opportunity.get("completedActivities")  or []
            sched = opportunity.get("scheduledActivities")  or []
    
        candidates = []
    
        # 1) firstActivity.completedDate if present on the opp
        fa_dt = _parse_iso_safe(((opportunity.get("firstActivity") or {}).get("completedDate")))
        if fa_dt:
            candidates.append(fa_dt)
    
        # 2) earliest completed activity timestamp
        for a in comp:
            adt = _parse_iso_safe(a.get("completedDate") or a.get("activityDate"))
            if adt:
                candidates.append(adt)
    
        # 3) earliest scheduled dueDate that's already in the past (as a last resort)
        now_utc = _dt.now(_tz.utc)
        for a in sched:
            due = _parse_iso_safe(a.get("dueDate") or a.get("dueDateTime"))
            if due and due <= now_utc:
                candidates.append(due)
    
        if candidates:
            derived_datein_dt = min(candidates)
            opportunity["dateIn"] = _to_iso_utc(derived_datein_dt)
            log.info(
                "KBB dateIn derived → %s (opp=%s)",
                opportunity["dateIn"],
                opportunity.get("opportunityId") or opportunity.get("id")
            )
    # --- END: ensure dateIn is present for cadence math ---

    
    # Persona routing for exact KBB (ICO/ServiceDrive)
    if _is_exact_kbb_ico_flags(flags, opportunity):
        # Lead age (safe default)
        lead_age_days = 0
        created_raw = (
            opportunity.get("dateIn")  
            or opportunity.get("createdDate")
            or opportunity.get("created_at")
            or (opportunity.get("firstActivity") or {}).get("completedDate")
        )
        try:
            if created_raw:
                created_dt = _dt.fromisoformat(str(created_raw).replace("Z", "+00:00"))
                lead_age_days = (_dt.now(_tz.utc) - created_dt).days
        except Exception:
            pass

        # --- DEBUG: lead-age source + math ---
        try:
            # Identify which field actually supplied created_raw
            if opportunity.get("dateIn"):
                _age_src = "dateIn"
            elif opportunity.get("createdDate"):
                _age_src = "createdDate"
            elif opportunity.get("created_at"):
                _age_src = "created_at"
            elif (opportunity.get("firstActivity") or {}).get("completedDate"):
                _age_src = "firstActivity.completedDate"
            else:
                _age_src = "None"
        
            log.info(
                "KBB age calc → src=%s created_raw=%r lead_age_days=%s opp=%s",
                _age_src,
                created_raw,
                lead_age_days,
                opportunity.get("opportunityId") or opportunity.get("id")
            )
        except Exception as _e:
            log.warning("KBB age calc debug failed: %s", _e)
        # --- /DEBUG ---

        # Try to surface any inquiry text we may already have; safe default to ""
        inquiry_text_safe = (opportunity.get("inquiry_text_body") or "").strip()
    
        # Hand off to the KBB ICO flow (templates + stop-on-reply convo)
        try:
            tok = None
            if not OFFLINE_MODE:
                tok = token
    
            state, action_taken = process_kbb_ico_lead(
                opportunity=opportunity,
                lead_age_days=lead_age_days,
                rooftop_name=rooftop_name,
                inquiry_text=inquiry_text_safe,
                token=tok,
                subscription_id=subscription_id,
                SAFE_MODE=os.getenv("SAFE_MODE", "1") in ("1","true","True"),
                rooftop_sender=rooftop_sender,
            )
    
            # Persist updates
            if not OFFLINE_MODE:
                es_update_with_retry(esClient, index="opportunities", id=opportunityId, doc=opportunity)
    
            # Optional: write compact state note if we acted
            if action_taken:
                compact = {
                    "mode": state.get("mode"),
                    "last_template_day_sent": state.get("last_template_day_sent"),
                    "nudge_count": state.get("nudge_count"),
                    "last_customer_msg_at": state.get("last_customer_msg_at"),
                    "last_agent_msg_at": state.get("last_agent_msg_at"),
                    "last_inbound_activity_id": state.get("last_inbound_activity_id"),
                    "last_appt_activity_id": state.get("last_appt_activity_id"),
                    "appt_due_utc": state.get("appt_due_utc"),
                    "appt_due_local": state.get("appt_due_local"),
                }
                note_txt = f"[PATTI_KBB_STATE] {json.dumps(compact, separators=(',',':'))}"
                if not OFFLINE_MODE:
                    add_opportunity_comment(tok, subscription_id, opportunityId, note_txt)
    
        except Exception as e:
            log.exception("KBB ICO handler failed for opp %s: %s", opportunityId, e)
    
        # Do not fall through to general flow
        wJson(opportunity, f"jsons/process/{opportunityId}.json")
        return
    
    # === if we got here, proceed with the normal (non-KBB) flow ===

    # ========= Getting new activities from Fortellis (NON-KBB only) =====

    if OFFLINE_MODE:
        local_completed = opportunity.get("completedActivitiesTesting", []) or []
        activities = {"scheduledActivities": [], "completedActivities": local_completed}
    else:
        activities = get_activities(opportunityId, customerId, token, subscription_id)
    
    # Safety: if anything upstream handed us a list, coerce to the dict shape we expect
    if isinstance(activities, list):
        activities = {"scheduledActivities": [], "completedActivities": activities}
    
    currDate = _dt.now()
    docToUpdate = {
        "scheduledActivities": activities.get("scheduledActivities", []),
        "completedActivities": activities.get("completedActivities", []),
        "updated_at": currDate
    }
    opportunity.update(docToUpdate)
    
    if not OFFLINE_MODE:
        opportunity.pop("completedActivitiesTesting", None)
        es_update_with_retry(esClient, index="opportunities", id=opportunityId, doc=opportunity)


    # ====================================================


    # === Vehicle & SRP link =============================================
    soughtVehicles = opportunity.get('soughtVehicles') or []
    if not isinstance(soughtVehicles, list):
        soughtVehicles = []
    vehicleObj = None
    for vehicle in soughtVehicles:
        if not vehicle.get('isPrimary'):
            continue
        vehicleObj = vehicle
        break

    if not vehicleObj:
        vehicleObj = (soughtVehicles[0] if soughtVehicles and isinstance(soughtVehicles[0], dict) else {})

    make  = str(vehicleObj.get("make") or "")
    model = str(vehicleObj.get("model") or "")
    year  = str(vehicleObj.get("yearFrom") or vehicleObj.get("year") or "")
    trim  = str(vehicleObj.get("trim") or "")
    stock = str(vehicleObj.get("stockNumber") or "")

    vehicle_str = f"{year} {make} {model} {trim}".strip() or "one of our vehicles"
    base_url = DEALERSHIP_URL_MAP.get(dealership)
    if base_url and (make and model):
        vehicle_str = f'<a href="{base_url}?make={make}&model={model}">{vehicle_str}</a>'

    
    completedActivities = activities.get('completedActivities', [])

    patti_already_contacted = checkedDict.get('patti_already_contacted', False)

    if not patti_already_contacted:

        firstActivity = getFirstActivity(completedActivities)
        opportunity['firstActivity'] = firstActivity
    
        if firstActivity:
            firstActivityFull = None  # define up front for both branches
    
            if not OFFLINE_MODE:
                firstActivityFull = get_activity_by_id_v1(firstActivity['activityId'], token, subscription_id)
                firstActivityMessageBody = (firstActivityFull.get('message') or {}).get('body', '') or ''
            else:
                # OFFLINE: derive a body from newest local activity
                newest = (completedActivities[-1] if completedActivities else {}) or {}
                msg = newest.get("message") or {}
    
                firstActivityMessageBody = (msg.get("body") or newest.get("notes") or "").strip()
    
                # Create an offline "full" act so the rest of the code can store it
                
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

            # --- unified opt-out check on the very first inbound ---
            from patti_common import _is_optout_text, _is_decline

            if inquiry_text and (_is_optout_text(inquiry_text) or _is_decline(inquiry_text)):
                log.info("❌ Customer opted out on first message. Marking inactive.")

                # make sure checkedDict exists
                checkedDict = opportunity.get("checkedDict") or {}
                checkedDict["exit_type"] = "customer_declined"
                checkedDict["exit_reason"] = (inquiry_text or "")[:250]
                opportunity["checkedDict"] = checkedDict
                opportunity["isActive"] = False

                # mark Patti as do-not-email for this opp
                patti_meta = opportunity.get("patti") or {}
                patti_meta["email_blocked_do_not_email"] = True
                opportunity["patti"] = patti_meta

                if not OFFLINE_MODE:
                    # update ES
                    es_update_with_retry(
                        esClient,
                        index="opportunities",
                        id=opportunityId,
                        doc=opportunity
                    )

                    # update CRM (best-effort)
                    try:
                        from fortellis import set_opportunity_inactive, set_customer_do_not_email
                        set_opportunity_inactive(
                            token,
                            subscription_id,
                            opportunityId,
                            sub_status="Not In Market",
                            comment="Customer opted out of communication."
                        )
                        set_customer_do_not_email(token, subscription_id, opportunityId)
                    except Exception as e:
                        log.error(f"Failed to set CRM inactive / do-not-email: {e}")

                # persist local JSON + stop processing
                wJson(opportunity, f"jsons/process/{opportunityId}.json")
                return

            # TODO: check with kristin if need to add activity logic to crm that patti will not used here
            if customerFirstMsgDict.get('salesAlreadyContact', False):
                opportunity['isActive'] = False
                opportunity['checkedDict']['is_sales_contacted'] = True
                if not OFFLINE_MODE:
                    es_update_with_retry(esClient, index="opportunities", id=opportunityId, doc=opportunity)

                wJson(opportunity, f"jsons/process/{opportunityId}.json")

                return

            # --- Step 3: try to auto-schedule an appointment from the inquiry text ---
            proposed = extract_appt_time(inquiry_text or "", tz="America/Los_Angeles")
            appt_iso = (proposed.get("iso") or "").strip()
            conf = float(proposed.get("confidence") or 0)

            created_appt_ok = False
            appt_human = None
            due_dt_iso_utc = None

            if appt_iso and conf >= 0.60:
                try:
                    # parse the local time and convert to UTC ISO
                    dt_local = _dt.fromisoformat(appt_iso.replace("Z", "+00:00"))
                    due_dt_iso_utc = dt_local.astimezone(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                    schedule_activity(
                        token,
                        subscription_id,
                        opportunityId,
                        due_dt_iso_utc=due_dt_iso_utc,
                        activity_name="Sales Appointment",
                        activity_type="Appointment",
                        comments=f"Auto-scheduled from customer email: {inquiry_text[:180]}",
                    )
                    created_appt_ok = True
                    appt_human = fmt_local_human(dt_local)

                    # 🔐 Store appointment state so future runs know this opp is scheduled
                    patti_meta = opportunity.get("patti") or {}
                    patti_meta["mode"] = "scheduled"
                    patti_meta["appt_due_utc"] = due_dt_iso_utc
                    opportunity["patti"] = patti_meta

                    log.info(
                        "✅ Auto-scheduled appointment for %s at %s (conf=%.2f)",
                        opportunityId,
                        appt_human,
                        conf,
                    )
                except Exception as e:
                    log.error(
                        "Failed to auto-schedule appointment for %s (appt_iso=%r): %s",
                        opportunityId,
                        appt_iso,
                        e,
                    )



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
            
        # --- NEW: if Patti auto-scheduled an appointment, tell GPT to confirm it ---
        if created_appt_ok and appt_human:
            prompt += f"""

    IMPORTANT APPOINTMENT CONTEXT (do not skip):
    - The guest proposed a time and Patti already scheduled a dealership appointment for {appt_human}.
    
    In your email:
    - Clearly confirm that date and time in plain language.
    - Thank them for scheduling.
    - Invite them to reply if they need to adjust the time or have any questions.
    - Do NOT ask them to pick a time; the appointment is already scheduled. Focus on confirming it.
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
        nextDate = currDate + _td(hours=24)   # or use your cadence
        opportunity['followUP_date'] = nextDate.isoformat()
        opportunity['followUP_count'] = 0
        if not OFFLINE_MODE:
            es_update_with_retry(esClient, index="opportunities", id=opportunityId, doc=opportunity)
    else:
        # handle follow-ups messages
        checkActivities(opportunity, currDate, rooftop_name)

        fud = opportunity.get('followUP_date')
        followUP_date = _dt.fromisoformat(fud) if isinstance(fud, str) else (fud if isinstance(fud, _dt) else currDate)
        followUP_count = opportunity['followUP_count']
    
        # --- NEW: Step 4 — pause cadence if there is an upcoming appointment ---
        patti_meta = opportunity.get("patti") or {}
        appt_due_utc = patti_meta.get("appt_due_utc")
        if appt_due_utc:
            try:
                appt_dt = _dt.fromisoformat(str(appt_due_utc).replace("Z", "+00:00"))
                now_utc = _dt.now(_tz.utc)
                if appt_dt > now_utc:
                    log.info(
                        "⏸ Skipping cadence follow-up for %s — appointment already scheduled at %s",
                        opportunityId,
                        appt_dt.isoformat(),
                    )
                    wJson(opportunity, f"jsons/process/{opportunityId}.json")
                    return
            except Exception as e:
                log.warning(
                    "Failed to parse appt_due_utc %r for %s: %s",
                    appt_due_utc,
                    opportunityId,
                    e,
                )
    
        last_by = (opportunity.get('checkedDict') or {}).get('last_msg_by', '')
        if followUP_date <= currDate and followUP_count > 3:
            opportunity['isActive'] = False
            if not OFFLINE_MODE:
                es_update_with_retry(esClient, index="opportunities", id=opportunityId, doc=opportunity)
            wJson(opportunity, f"jsons/process/{opportunityId}.json")
            return
        elif followUP_date <= currDate:
            messages = opportunity['messages']
            prompt = f"""
            generate next patti reply which is a follow-up message, ... messages between patti and the customer (python list of dicts):
            {messages}
            """
            response = run_gpt(
                prompt,
                customer_name,
                rooftop_name,
                prevMessages=True,
            )

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

            nextDate = currDate + _td(days=1)
            opportunity['followUP_date'] = nextDate.isoformat()
            opportunity['followUP_count'] += 1
            if not OFFLINE_MODE:
                es_update_with_retry(esClient, index="opportunities", id=opportunityId, doc=opportunity)
    
    wJson(opportunity, f"jsons/process/{opportunityId}.json")


# ---- Rolling ES lookback window (default 6 days) ----
LOOKBACK_DAYS = int(os.getenv("ES_LOOKBACK_DAYS", "6"))

if __name__ == "__main__":
    if not OFFLINE_MODE:
        # Start date = now - LOOKBACK_DAYS (UTC), formatted YYYY-MM-DD
        
        start_date = (_dt.now(_tz.utc) - _td(days=LOOKBACK_DAYS)).date().isoformat()
        log.info("ES lookback start_date=%s (last %s days)", start_date, LOOKBACK_DAYS)

        data = getNewDataByDate(start_date)

        # Process ALL hits (no early exit)
        for hit in data:
            processHit(hit)
    # playground drives processHit() via Flask; nothing to run here when OFFLINE_MODE=1

    

    
