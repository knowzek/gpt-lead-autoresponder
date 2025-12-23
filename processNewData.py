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
from kbb_ico import _top_reply_only, _is_optout_text as _kbb_is_optout_text, _is_decline as _kbb_is_decline
from rooftops import get_rooftop_info
from constants import *
from gpt import run_gpt, getCustomerMsgDict, extract_appt_time
import re
import logging
import hashlib, json, time
from uuid import uuid4
from zoneinfo import ZoneInfo
import uuid
from airtable_store import (
    find_by_opp_id, query_view, acquire_lock, release_lock,
    opp_from_record, save_opp
)

from fortellis import (
    get_activities,
    get_token,
    get_activity_by_id_v1,
    get_opportunity,
    add_opportunity_comment,
    schedule_activity,
    send_opportunity_email_activity,
    set_opportunity_substatus,
)


from patti_common import fmt_local_human, normalize_patti_body, append_soft_schedule_sentence, rewrite_sched_cta_for_booked

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

def airtable_save(opportunity: dict, extra_fields: dict | None = None):
    """
    Persist the full opp back to Airtable (opp_json + follow_up_at + is_active).
    """
    if OFFLINE_MODE:
        return
    return save_opp(opportunity, extra_fields=extra_fields or {})

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
    substatus = _get_lc(doc, "subStatus", "substatus")
    uptype    = _get_lc(doc, "upType", "uptype")

    print("KBB detect →", {
        "source": source,
        "status": status,
        "substatus": substatus,
        "uptype": uptype,
    })

    return (
        source in _KBB_SOURCES and
        status == "active" and
        uptype == "campaign" and
        substatus in {"new", "working"}
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



def checkActivities(opportunity, currDate, rooftop_name, activities_override=None):
    if activities_override is not None:
        activities = activities_override
    elif OFFLINE_MODE:
        activities = opportunity.get('completedActivitiesTesting', [])
    else:
        activities = opportunity.get('completedActivities', [])

    activities = sortActivities(activities)

    alreadyProcessedActivities = opportunity.get('alreadyProcessedActivities', {})

    # Ensure checkedDict is always a dict on the opportunity
    checkedDict = opportunity.get('checkedDict') or {}
    if not isinstance(checkedDict, dict):
        checkedDict = {}
    opportunity["checkedDict"] = checkedDict  # <-- make it live on the opp
    
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
            has_msg_body = bool(((act.get("message") or {}).get("body") or "").strip())
            
            if (not has_msg_body) and (not DEBUGMODE) and (not OFFLINE_MODE):
                fullAct = get_activity_by_id_v1(activityId, token, subscription_id)


            # --- KBB-style normalization: top reply only + plain-text fallback ---
            customerMsg = (fullAct.get("message") or {})
            raw_body_html = (customerMsg.get("body") or "").strip()
            customer_body = _top_reply_only(raw_body_html)

            if not customer_body:
                # Simple HTML → text fallback if _top_reply_only returns empty
                import re as _re
                no_tags = _re.sub(r"(?is)<[^>]+>", " ", raw_body_html)
                customer_body = _re.sub(r"\s+", " ", no_tags).strip()

            customerMsgDict = {
                "msgFrom": "customer",
                "customerName": customerInfo.get("firstName"),
                "subject": customerMsg.get("subject"),
                "body": customer_body,          # <-- use cleaned top-reply text
                "date": fullAct.get("completedDate"),
            }

            # append the customer's message to the thread
            opportunity.setdefault('messages', []).append(customerMsgDict)
            messages = opportunity['messages']
            checkedDict["last_msg_by"] = "customer"
            opportunity['checkedDict'] = checkedDict  # ensure persisted even if it was missing
            
            # 🚫 Unified opt-out / decline check — re-use KBB logic on the CLEANED body
            if _kbb_is_optout_text(customer_body) or _kbb_is_decline(customer_body):

                log.info("Customer opted out or declined interest. Marking opportunity inactive.")
                opportunity['isActive'] = False
                checkedDict['exit_reason'] = customer_body[:250]
                checkedDict['exit_type'] = "customer_declined"
                opportunity['checkedDict'] = checkedDict

                # mark this activity as processed with a minimal stub
                apa = opportunity.get("alreadyProcessedActivities") or {}
                if not isinstance(apa, dict):
                    apa = {}
                apa[activityId] = {
                    "activityId": activityId,
                    "completedDate": fullAct.get("completedDate"),
                    "activityType": fullAct.get("activityType"),
                    "activityName": fullAct.get("activityName"),
                }
                opportunity["alreadyProcessedActivities"] = apa

                if not OFFLINE_MODE:
                    opportunity["followUP_date"] = None
                    airtable_save(opportunity, extra_fields={"follow_up_at": None})

                wJson(opportunity, f"jsons/process/{opportunity['opportunityId']}.json")
                return
            
            # --- Step 2: try to auto-schedule an appointment from this reply ---
            created_appt_ok = False
            appt_human = None
            try:
                # Skip if we already know about a future appointment
                patti_meta = opportunity.get("patti") or {}
                appt_due_utc = patti_meta.get("appt_due_utc")
                already_scheduled = False
                if appt_due_utc:
                    try:
                        appt_dt = _dt.fromisoformat(str(appt_due_utc).replace("Z", "+00:00"))
                        if appt_dt > _dt.now(_tz.utc):
                            already_scheduled = True
                    except Exception:
                        pass

                appt_iso = ""
                conf = 0.0
                if not already_scheduled:
                    proposed = extract_appt_time(customer_body or "", tz="America/Los_Angeles")
                    appt_iso = (proposed.get("iso") or "").strip()
                    conf = float(proposed.get("confidence") or 0.0)

                if appt_iso and conf >= 0.60:
                    try:
                        dt_local = _dt.fromisoformat(appt_iso.replace("Z", "+00:00"))
                        due_dt_iso_utc = dt_local.astimezone(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                        
                        schedule_activity(
                            token,
                            subscription_id,
                            opportunity['opportunityId'],
                            due_dt_iso_utc=due_dt_iso_utc,
                            activity_name="Sales Appointment",
                            activity_type="Appointment",
                            comments=f"Auto-scheduled from Patti based on customer reply: {customer_body[:200]}"
                        )
                        created_appt_ok = True
                        appt_human = fmt_local_human(dt_local)
                        
                        patti_meta["mode"] = "scheduled"
                        patti_meta["appt_due_utc"] = due_dt_iso_utc
                        # GPT reply will confirm this, so mark to prevent duplicates.
                        patti_meta["appt_confirm_email_sent"] = True
                        opportunity["patti"] = patti_meta

                        
                        log.info(
                            "✅ Auto-scheduled appointment from reply for %s at %s (conf=%.2f)",
                            opportunity['opportunityId'],
                            appt_human,
                            conf,
                        )
                    except Exception as e:
                        log.error(
                            "Failed to auto-schedule appointment from reply for %s (appt_iso=%r): %s",
                            opportunity['opportunityId'],
                            appt_iso,
                            e,
                        )
            except Exception as e:
                log.warning(
                    "Reply-based appointment detection failed for %s: %s",
                    opportunity.get('opportunityId'),
                    e,
                )

            # ✅ continue with GPT reply generation
            if created_appt_ok and appt_human:
                prompt = f"""
            The customer and Patti have been emailing about a potential sales appointment.

            Patti has just scheduled an appointment in the CRM based on the most recent customer reply.
            Appointment time (local dealership time): {appt_human}.

            Write Patti's next email reply using the messages list below. Patti should:
            - Warmly confirm the appointment for {appt_human}
            - Thank the customer and set expectations for the visit
            - NOT ask the customer to choose a time again.

            Here are the messages (Python list of dicts):
            {messages}
            """
            else:
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

            
            subject   = response["subject"]
            body_html = response["body"]
            
            # strip any duplicated Patti signature the model added
            body_html = re.sub(
                r"(?is)(?:\n\s*)?patti\s*(?:\r?\n)+virtual assistant.*?$",
                "",
                body_html
            )

            # --- Normalize Patti body & add CTA + footer (same as initial email) ---
            from kbb_ico import _patch_address_placeholders, build_patti_footer, _PREFS_RE
            
            # Clean up paragraphs / bullets
            body_html = normalize_patti_body(body_html)
            
            # Patch rooftop/address placeholders (e.g. LegacySalesApptSchLink, dealership name)
            body_html = _patch_address_placeholders(body_html, rooftop_name)
            
            # Decide which CTA behavior to use based on appointment state
            patti_meta = opportunity.get("patti") or {}
            mode = (patti_meta.get("mode") or "").strip().lower()
            
            sub_status = (
                (fresh_opp.get("subStatus") or fresh_opp.get("substatus") or "")
                or (opportunity.get("subStatus") or opportunity.get("substatus") or "")
            ).strip().lower()
            
            has_booked_appt = (mode == "scheduled") or ("appointment" in sub_status) or bool(patti_meta.get("appt_due_utc"))
            
            if has_booked_appt:
                body_html = rewrite_sched_cta_for_booked(body_html)
                body_html = _ANY_SCHED_LINE_RE.sub("", body_html).strip()
            else:
                body_html = append_soft_schedule_sentence(body_html, rooftop_name)

            
            # Strip any extraneous prefs/unsubscribe footer GPT might add
            body_html = _PREFS_RE.sub("", body_html).strip()
            
            # Add Patti’s signature/footer with the Tustin Kia logo
            body_html = body_html + build_patti_footer(rooftop_name)

            
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
            
            # mark this Read Email activity as processed (stub only)
            apa = opportunity.get("alreadyProcessedActivities") or {}
            if not isinstance(apa, dict):
                apa = {}
            apa[activityId] = {
                "activityId":   fullAct.get("id") or activityId,
                "completedDate": fullAct.get("completedDate"),
                "activityType":  fullAct.get("activityType"),
                "activityName":  fullAct.get("activityName"),
            }
            opportunity["alreadyProcessedActivities"] = apa
            
            nextDate = currDate + _td(hours=24)
            opportunity['followUP_date']  = nextDate.isoformat()
            opportunity['followUP_count'] = 0
            
            # 🔔 NEW: send the follow-up email + persist to ES, then stop
            if not OFFLINE_MODE:
                # figure out sender from rooftop
                rt = get_rooftop_info(subscription_id)
                rooftop_sender = rt.get("sender") or TEST_FROM
            
                # pick customer email (prefer preferred & not doNotEmail)
                cust   = opportunity.get("customer") or {}
                emails = cust.get("emails") or []
                customer_email = None
                for e in emails:
                    if e.get("doNotEmail"):
                        continue
                    if e.get("isPreferred"):
                        customer_email = e.get("address")
                        break
                if not customer_email and emails:
                    customer_email = emails[0].get("address")
            
                if customer_email:
                    try:
                        from patti_mailer import send_patti_email

                        send_patti_email(
                            token=token,
                            subscription_id=subscription_id,
                            opp_id=opportunity["opportunityId"],
                            rooftop_name=rooftop_name,
                            rooftop_sender=rooftop_sender,
                            to_addr=customer_email,
                            subject=subject,
                            body_html=body_html,
                            cc_addrs=[],
                        )

                    except Exception as e:
                        log.warning(
                            "Failed to send Patti follow-up email for opp %s: %s",
                            opportunity["opportunityId"],
                            e,
                        )
            
                # persist updated opportunity (messages, followUP_date, etc.)
                airtable_save(opportunity)
            
            # write debug json + stop processing this opp for this run
            wJson(opportunity, f"jsons/process/{opportunity['opportunityId']}.json")
            return

def _derive_appointment_from_sched_activities(opportunity, tz_name="America/Los_Angeles"):
    """Inspect scheduledActivities for a future appointment and, if found,
    update opportunity['patti']['mode'] / ['appt_due_utc'] so Patti will
    pause cadence nudges once an appointment is on the books.
    Returns True if state was updated, False otherwise.
    """
    try:
        sched = opportunity.get("scheduledActivities") or []
        if not isinstance(sched, list):
            return False

        # If we already have a future appt_due_utc recorded, don't override it.
        patti_meta = opportunity.get("patti") or {}
        appt_due_utc = patti_meta.get("appt_due_utc")
        if appt_due_utc:
            try:
                existing_dt = _dt.fromisoformat(str(appt_due_utc).replace("Z", "+00:00"))
                if existing_dt > _dt.now(_tz.utc):
                    return False
            except Exception:
                # fall through and allow re-deriving if parsing fails
                pass

        now_utc = _dt.now(_tz.utc)
        candidates = []

        for a in sched:
            raw_name = (a.get("activityName") or a.get("name") or "").strip().lower()
            t = a.get("activityType")

            # Treat anything clearly labeled as an appointment as such
            t_str = str(t).strip().lower() if t is not None else ""
            is_appt = (
                "appointment" in raw_name
                or t_str in ("2", "appointment")
            )
            if not is_appt:
                continue

            # Many booking-link activities use dueDateTime / startDateTime
            due_raw = (
                a.get("dueDateTime")
                or a.get("dueDate")
                or a.get("startDateTime")
                or a.get("activityDate")
                or a.get("completedDate")
            )
            if not due_raw:
                continue
            try:
                due_dt = _dt.fromisoformat(str(due_raw).replace("Z", "+00:00"))
            except Exception:
                continue


            if due_dt > now_utc:
                candidates.append(due_dt)

        if not candidates:
            return False

        # Use the earliest future appointment
        due_dt = min(candidates)
        due_dt_iso_utc = due_dt.astimezone(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        patti_meta["mode"] = "scheduled"
        patti_meta["appt_due_utc"] = due_dt_iso_utc
        opportunity["patti"] = patti_meta

        return True
    except Exception as e:
        # Never break the main job because of a best-effort helper
        try:
            log.warning("Failed to derive appointment from scheduledActivities for %s: %s",
                        opportunity.get("opportunityId"), e)
        except Exception:
            pass
        return False


def processHit(hit):
    currDate = _dt.now(_tz.utc)

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

    # --- Customer: tolerate missing + self-heal from Fortellis ---
    customer = opportunity.get("customer") or {}
    customerId = customer.get("id")
    
    if not customerId and not OFFLINE_MODE:
        try:
            fresh_opp = get_opportunity(opportunityId, token, subscription_id)
            if isinstance(fresh_opp, dict):
                # hydrate missing customer
                if fresh_opp.get("customer"):
                    opportunity["customer"] = fresh_opp.get("customer") or {}
                    customer = opportunity["customer"]
                    customerId = customer.get("id")
    
                # hydrate other commonly-missing fields
                if fresh_opp.get("salesTeam") is not None:
                    opportunity["salesTeam"] = fresh_opp.get("salesTeam") or []
                if fresh_opp.get("source") is not None:
                    opportunity["source"] = fresh_opp.get("source")
                if fresh_opp.get("upType") is not None:
                    opportunity["upType"] = fresh_opp.get("upType")
                if fresh_opp.get("status") is not None:
                    opportunity["status"] = fresh_opp.get("status")
                if fresh_opp.get("subStatus") is not None:
                    opportunity["subStatus"] = fresh_opp.get("subStatus")
                if fresh_opp.get("isActive") is not None:
                    opportunity["isActive"] = fresh_opp.get("isActive")
    
                # persist once so future runs are clean
                airtable_save(opportunity)
    
        except Exception as e:
            log.warning("Customer hydrate failed opp=%s err=%s", opportunityId, e)
    
    # final safety gate
    if not customerId:
        log.warning("Opp %s missing customer.id after hydrate; skipping.", opportunityId)
        return


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
            opportunity.setdefault("patti", {})["transient_error"] = None
            airtable_save(opportunity)



    except Exception as e:
        # Downgrade to a transient error so we retry next run (no hard skip)
        log.warning("Transient get_opportunity failure for %s: %s", opportunityId, str(e)[:200])
        if not OFFLINE_MODE:
            # increment a lightweight failure counter using what we already have in memory
            prev = (opportunity.get("patti") or {}).get("transient_error") or {}
            fail_count = (prev.get("count") or 0) + 1
            # update in-memory opp then persist to Airtable
            patti = opportunity.setdefault("patti", {})
            patti["transient_error"] = {
                "code": "get_opportunity_failed",
                "message": str(e)[:200],
                "at": _dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "count": fail_count,
            }
            patti["skip"] = False
            patti["skip_reason"] = None
            
            # Persist blob to Airtable (instead of ES partial update)
            if not OFFLINE_MODE:
                airtable_save(opportunity)   # or save_opp(opportunity)

        # We can’t proceed without fresh_opp; exit gracefully and let the next run retry.
        return
    
    # keep this — we still skip inactive opps
    if not is_active_opp(fresh_opp):
        log.info("Skipping opp %s (inactive from Fortellis).", opportunityId)
        if not OFFLINE_MODE:
            patti = opportunity.setdefault("patti", {})
            patti["skip"] = True
            patti["skip_reason"] = "inactive_opportunity"
            patti["transient_error"] = None
            patti["inactive_at"] = _dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            patti["inactive_snapshot"] = {
                "status": fresh_opp.get("status"),
                "subStatus": fresh_opp.get("subStatus"),
                "isActive": fresh_opp.get("isActive"),
            }
            
            # optional (but recommended): also mark inactive at the top level so your Due Now view stops pulling it
            opportunity["isActive"] = False
            
            if not OFFLINE_MODE:
                # also clear follow-up so it won't keep showing as due
                opportunity["followUP_date"] = None
                airtable_save(opportunity, extra_fields={"follow_up_at": None})

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
                airtable_save(opportunity)

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
    
    currDate = _dt.now(_tz.utc)
    docToUpdate = {
        "scheduledActivities": activities.get("scheduledActivities", []),
        "completedActivities": activities.get("completedActivities", []),
        "updated_at": currDate
    }
    opportunity.update(docToUpdate)
    
    # Best-effort: if the CRM already has a future appointment scheduled
    # (for example, via a booking link), mirror that into Patti's state so
    # she pauses cadence nudges but continues to watch for replies.
    has_appt = _derive_appointment_from_sched_activities(opportunity)
    patti_meta = opportunity.get("patti") or {}
    if has_appt:
        patti_meta["mode"] = "scheduled"
        # if _derive_appointment_from_sched_activities returns / sets due date somewhere, store it:
        # patti_meta["appt_due_utc"] = derived_due_utc
        opportunity["patti"] = patti_meta

    
    # If we now know there’s an appointment, flip the CRM substatus in Fortellis
    if has_appt and not OFFLINE_MODE:
        try:
            resp = set_opportunity_substatus(
                token,
                subscription_id,
                opportunityId,
                sub_status="Appointment Set",
            )
            log.info(
                "Non-KBB appt: SubStatus update response: %s",
                getattr(resp, "status_code", "n/a"),
            )
        except Exception as e:
            log.warning("Non-KBB appt: set_opportunity_substatus failed: %s", e)

    # 🚫 Global guard: if this opp is already appointment-set, stop Patti's cadence
    patti_meta = opportunity.get("patti") or {}
    mode = patti_meta.get("mode")
    sub_status = (
        (fresh_opp.get("subStatus") or fresh_opp.get("substatus") or "")
        or (opportunity.get("subStatus") or opportunity.get("substatus") or "")
    ).strip().lower()

    has_booked_appt = (mode == "scheduled") or ("appointment" in sub_status)

    if has_booked_appt:
        log.info(
            "Opp %s has booked appointment (mode=%r, subStatus=%r); "
            "suppressing Patti follow-up cadence.",
            opportunityId,
            mode,
            sub_status,
        )
        opportunity["patti"] = patti_meta

        if not OFFLINE_MODE:
            opportunity.pop("completedActivitiesTesting", None)
            airtable_save(opportunity)

        wJson(opportunity, f"jsons/process/{opportunityId}.json")
        return

    # normal ES cleanup when there is *no* appointment yet
    if not OFFLINE_MODE:
        opportunity.pop("completedActivitiesTesting", None)
        airtable_save(opportunity)


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

    # 🔒 Extra safety: if we see any Fortellis send-email activities, 
    # assume Patti has already contacted this lead at least once.
    if not patti_already_contacted:
        for act in completedActivities:
            name = (act.get("activityName") or "").strip()
            comments = (act.get("comments") or "").lower()
            if name == "Fortellis - Send Email" and "sent via fortellis email service" in comments:
                patti_already_contacted = True
                checkedDict["patti_already_contacted"] = True
                opportunity["checkedDict"] = checkedDict
                break


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
                    "completedDate": newest.get("completedDate")
                        or _dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
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
            
            # Save ONLY a minimal stub for this activity
            src = (firstActivityFull or firstActivity or {}) or {}
            opportunity["alreadyProcessedActivities"][act_id] = {
                "activityId": src.get("activityId") or src.get("id") or act_id,
                "completedDate": src.get("completedDate"),
                "activityType": src.get("activityType"),
                "activityName": src.get("activityName"),
            }

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
            
                checkedDict = opportunity.get("checkedDict") or {}
                checkedDict["exit_type"] = "customer_declined"
                checkedDict["exit_reason"] = (inquiry_text or "")[:250]
                opportunity["checkedDict"] = checkedDict
            
                opportunity["isActive"] = False
                opportunity["followUP_date"] = None    
            
                patti_meta = opportunity.get("patti") or {}
                patti_meta["email_blocked_do_not_email"] = True
                opportunity["patti"] = patti_meta
            
                if not OFFLINE_MODE:
                    airtable_save(opportunity, extra_fields={"follow_up_at": None})
            
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
            
                wJson(opportunity, f"jsons/process/{opportunityId}.json")
                return
            
            if customerFirstMsgDict.get('salesAlreadyContact', False):
                opportunity['isActive'] = False
                opportunity["followUP_date"] = None   
                opportunity['checkedDict']['is_sales_contacted'] = True
                if not OFFLINE_MODE:
                    airtable_save(opportunity, extra_fields={"follow_up_at": None})

            
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
                    # Patti will confirm this appointment in the outgoing email,
                    # so mark the confirmation as sent to avoid duplicates later.
                    patti_meta["appt_confirm_email_sent"] = True
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
        
        # --- Normalize Patti body ---
        body_html = normalize_patti_body(body_html)
        
        # --- patch the rooftop/address placeholders ---
        body_html = _patch_address_placeholders(body_html, rooftop_name)
        
        # Decide which CTA behavior to use based on appointment state
        patti_meta = opportunity.get("patti") or {}
        mode = (patti_meta.get("mode") or "").strip().lower()
        
        sub_status = (
            (fresh_opp.get("subStatus") or fresh_opp.get("substatus") or "")
            or (opportunity.get("subStatus") or opportunity.get("substatus") or "")
        ).strip().lower()
        
        has_booked_appt = (mode == "scheduled") or ("appointment" in sub_status) or bool(patti_meta.get("appt_due_utc"))
        
        if has_booked_appt:
            body_html = rewrite_sched_cta_for_booked(body_html)
            body_html = _ANY_SCHED_LINE_RE.sub("", body_html).strip()
        else:
            body_html = append_soft_schedule_sentence(body_html, rooftop_name)
        
        # Strip GPT footer if added
        body_html = _PREFS_RE.sub("", body_html).strip()
        
        # --- add Patti’s signature/footer (same as KBB) ---
        body_html = body_html + build_patti_footer(rooftop_name)
        
        opportunity["body_html"] = body_html
        
        # Append message to opportunity log
        msg_entry = {
            "msgFrom": "patti",
            "subject": subject,
            "body": body_html,
            "date": currDate
        }
        
        if "messages" in opportunity:
            opportunity["messages"].append(msg_entry)
        else:
            opportunity["messages"] = [msg_entry]
        
        # ---------------------------
        #   FIX: Only mark as sent if actual success
        # ---------------------------
        sent_ok = False
        
        if OFFLINE_MODE:
            sent_ok = True
        else:
            if customer_email:
                try:
                    from patti_mailer import send_patti_email
                    
                    send_patti_email(
                        token=token,
                        subscription_id=subscription_id,
                        opp_id=opportunity["opportunityId"],
                        rooftop_name=rooftop_name,
                        rooftop_sender=rooftop_sender,
                        to_addr=customer_email,
                        subject=subject,
                        body_html=body_html,
                        cc_addrs=[],
                    )

                    sent_ok = True   # <-- ONLY HERE DO WE MARK SUCCESS
                except Exception as e:
                    log.warning(
                        "Failed to send Patti general lead email for opp %s: %s",
                        opportunityId,
                        e,
                    )
            else:
                log.warning(
                    "No customer_email for opp %s – cannot send Patti general lead email.",
                    opportunityId,
                )
        
        # ---------------------------
        #   Only update Patti's state IF sent_ok is True
        # ---------------------------
        if sent_ok:
            checkedDict["patti_already_contacted"] = True
            checkedDict["last_msg_by"] = "patti"
            opportunity["checkedDict"] = checkedDict
        
            nextDate = currDate + _td(hours=24)
            next_iso = nextDate.isoformat()
        
            opportunity["followUP_date"] = next_iso
            opportunity["followUP_count"] = 0
        
            airtable_save(
                opportunity,
                extra_fields={"follow_up_at": next_iso}
            )

        else:
            log.warning(
                "Did NOT mark Patti as contacted for opp %s because sendEmail failed.",
                opportunityId,
            )

        
        # Persist Patti state + messages into ES
        airtable_save(opportunity)

    else:
        # handle follow-ups messages
        checkActivities(opportunity, currDate, rooftop_name)

        # --- One-time confirmation for appointments booked via the online link ---
        patti_meta = opportunity.get("patti") or {}
        appt_due_utc = patti_meta.get("appt_due_utc")
        appt_confirm_sent = patti_meta.get("appt_confirm_email_sent", False)

        # If we see a scheduled appointment but Patti never confirmed it,
        # assume it came from the booking link and send a confirmation now.
        if appt_due_utc and not appt_confirm_sent:
            try:
                # Convert stored UTC ISO to local time for human-friendly text
                appt_dt_utc = _dt.fromisoformat(str(appt_due_utc).replace("Z", "+00:00"))
                # TODO: if you have per-rooftop timezones, swap this out
                local_tz = ZoneInfo("America/Los_Angeles")
                appt_dt_local = appt_dt_utc.astimezone(local_tz)

                appt_dt_local = appt_dt_utc.astimezone(local_tz)
                appt_human = fmt_local_human(appt_dt_local)
            except Exception:
                appt_human = appt_due_utc

            messages = opportunity.get("messages") or []
            prompt = f"""
            The customer used the online booking link and there is now a scheduled
            sales appointment in the CRM.

            Appointment time (local dealership time): {appt_human}.

            Write Patti's next email reply using the messages list below. Patti should:
            - Warmly confirm the appointment for {appt_human}
            - Thank the guest and set expectations for the visit
            - NOT ask the customer to choose a time again.

            Here are the messages (Python list of dicts):
            {messages}
            """

            response = run_gpt(
                prompt,
                customer_name,
                rooftop_name,
                prevMessages=True
            )
            subject   = response["subject"]
            body_html = response["body"]

            # Normalize + patch + CTA + footer (same as other Patti emails)
            body_html = normalize_patti_body(body_html)
            from kbb_ico import _patch_address_placeholders, build_patti_footer, _PREFS_RE
            body_html = _patch_address_placeholders(body_html, rooftop_name)

            patti_meta = opportunity.get("patti") or {}
            mode = (patti_meta.get("mode") or "").strip().lower()
            
            sub_status = (
                (fresh_opp.get("subStatus") or fresh_opp.get("substatus") or "")
                or (opportunity.get("subStatus") or opportunity.get("substatus") or "")
            ).strip().lower()
            
            has_booked_appt = (mode == "scheduled") or ("appointment" in sub_status) or bool(patti_meta.get("appt_due_utc"))
            
            if has_booked_appt:
                body_html = rewrite_sched_cta_for_booked(body_html)
                body_html = _ANY_SCHED_LINE_RE.sub("", body_html).strip()
            else:
                body_html = append_soft_schedule_sentence(body_html, rooftop_name)


            body_html = _PREFS_RE.sub("", body_html).strip()
            body_html = body_html + build_patti_footer(rooftop_name)

            # Append to thread
            opportunity.setdefault("messages", []).append({
                "msgFrom": "patti",
                "subject": subject,
                "body": body_html,
                "date": currDate,
                "action": response.get("action"),
                "notes": response.get("notes"),
            })

            checkedDict["last_msg_by"] = "patti"
            opportunity["checkedDict"] = checkedDict

            # Mark confirmation as sent so we never do this twice
            patti_meta["appt_confirm_email_sent"] = True
            opportunity["patti"] = patti_meta

            # Send email through Fortellis + persist
            if not OFFLINE_MODE:
                rt = get_rooftop_info(subscription_id)
                rooftop_sender = rt.get("sender") or TEST_FROM

                cust   = opportunity.get("customer") or {}
                emails = cust.get("emails") or []
                customer_email = None
                for e in emails:
                    if e.get("doNotEmail"):
                        continue
                    if e.get("isPreferred"):
                        customer_email = e.get("address")
                        break
                if not customer_email and emails:
                    customer_email = emails[0].get("address")

                if customer_email:
                    try:
                        from patti_mailer import send_patti_email
                        
                        send_patti_email(
                            token=token,
                            subscription_id=subscription_id,
                            opp_id=opportunity["opportunityId"],
                            rooftop_name=rooftop_name,
                            rooftop_sender=rooftop_sender,
                            to_addr=customer_email,
                            subject=subject,
                            body_html=body_html,
                            cc_addrs=[],
                        )

                    except Exception as e:
                        log.warning(
                            "Failed to send Patti booking-link appt confirmation for opp %s: %s",
                            opportunity["opportunityId"],
                            e,
                        )

                airtable_save(opportunity)

            # Debug JSON + stop this run
            wJson(opportunity, f"jsons/process/{opportunity['opportunityId']}.json")
            return

        
        fud = opportunity.get("followUP_date")

        if isinstance(fud, str):
            dt = _dt.fromisoformat(fud)
        elif isinstance(fud, _dt):
            dt = fud
        else:
            # No previous follow-up recorded.
            # Seed a follow-up date 24h from now and DO NOT send a nudge on this run.
            dt = currDate + _td(hours=24)
            opportunity['followUP_date'] = dt.isoformat()
            opportunity.setdefault('followUP_count', 0)
            if not OFFLINE_MODE:
                airtable_save(opportunity)
            wJson(opportunity, f"jsons/process/{opportunityId}.json")
            return  # ⬅️ important: skip the cadence logic below for this run
        
        # Make it timezone-aware (UTC)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_tz.utc)
        
        followUP_date = dt
        followUP_count = int(opportunity.get("followUP_count") or 0)

        
        # Make it timezone-aware (UTC)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_tz.utc)
        
        followUP_date = dt
        
        # -- followUP_count normalization --
        followUP_count = int(opportunity.get("followUP_count") or 0)

    
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
            opportunity["followUP_date"] = None   
            if not OFFLINE_MODE:
                airtable_save(opportunity, extra_fields={"follow_up_at": None})
            wJson(opportunity, f"jsons/process/{opportunityId}.json")
            return

        elif followUP_date <= currDate:
            # Use full thread history but be explicit that this is NOT a first email.
            messages = opportunity.get("messages") or []
        
            prompt = f"""
        You are generating a FOLLOW-UP email, not a first welcome message.
        
        Context:
        - The guest originally inquired about: {vehicle_str}
        - Patti has already been in touch with the guest.

        Use the full message history below to see what’s already been discussed,
        then write the next short follow-up from Patti that makes sense given
        where the conversation left off.
        
        messages between Patti and the customer (python list of dicts):
        {messages}
        
        Follow-up requirements:
        - Do NOT repeat the full opener or dealership Why Buys from the first email.
        - Assume they already read your original message.
        - Keep it short: 2–4 sentences max.
        - Sound like you’re checking in on a thread you already started
          (e.g., “I wanted to follow up on my last note about the Sportage.”).
        - Make one simple, low-pressure ask (e.g., “Are you still considering the Sportage?” or
          “Would you like me to check availability or options for you?”).
        - Use a subject line that clearly looks like a follow-up on their vehicle inquiry,
          not a brand-new outreach.
        
        Return ONLY valid JSON with keys: subject, body.
            """.strip()
        
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

            # ✅ SEND the follow-up (currently missing)
            sent_ok = False
            customer_email = None
            
            if not OFFLINE_MODE:
                from patti_mailer import send_patti_email  # wrapper: Outlook send + CRM comment
            
                cust = opportunity.get("customer") or {}
                emails = cust.get("emails") or []
            
                # pick preferred + not doNotEmail, else first not doNotEmail
                for e in emails:
                    if e.get("doNotEmail"):
                        continue
                    if e.get("isPreferred") and e.get("address"):
                        customer_email = e["address"]
                        break
            
                if not customer_email:
                    for e in emails:
                        if e.get("doNotEmail"):
                            continue
                        if e.get("address"):
                            customer_email = e["address"]
                            break
            
                if customer_email:
                    try:
                        send_patti_email(
                            token=token,
                            subscription_id=subscription_id,
                            opp_id=opportunityId,
                            rooftop_name=rooftop_name,
                            rooftop_sender=rooftop_sender,
                            to_addr=customer_email,
                            subject=subject,
                            body_html=body_html,
                            cc_addrs=[],
                        )
                        sent_ok = True
                    except Exception as e:
                        log.warning("Follow-up send failed for opp %s: %s", opportunityId, e)
            
            # Only record + advance cadence if we actually sent (or you're in OFFLINE_MODE)
            if sent_ok or OFFLINE_MODE:
                opportunity.setdefault("messages", []).append(
                    {
                        "msgFrom": "patti",
                        "subject": subject,
                        "body": body_html,
                        "date": currDate,
                        "action": response.get("action"),
                        "notes": response.get("notes"),
                    }
                )
            
                opportunity.setdefault("checkedDict", {})["last_msg_by"] = "patti"
            
                nextDate = currDate + _td(days=1)
                next_iso = nextDate.isoformat()
                
                opportunity["followUP_date"] = next_iso
                opportunity["followUP_count"] = int(opportunity.get("followUP_count") or 0) + 1
                
                airtable_save(
                    opportunity,
                    extra_fields={"follow_up_at": next_iso}
                )

            
                if not OFFLINE_MODE:
                    airtable_save(opportunity)
    
    wJson(opportunity, f"jsons/process/{opportunityId}.json")


# ---- Rolling ES lookback window (default 6 days) ----
if __name__ == "__main__":
    test_opp_id = os.getenv("TEST_OPPORTUNITY_ID", "").strip()

    if test_opp_id:
        log.info("TEST_OPPORTUNITY_ID=%s set; running single-opportunity test mode", test_opp_id)
        rec = find_by_opp_id(test_opp_id)
        if not rec:
            log.warning("TEST_OPPORTUNITY_ID %s not found in Airtable; exiting.", test_opp_id)
        else:
            opp = opp_from_record(rec)
            processHit(opp)

    else:
        if not OFFLINE_MODE:
            # pull from Airtable view instead of ES
            records = query_view("Due Now", max_records=200)

            for rec in records:
                rec_id = rec.get("id")
                token = acquire_lock(rec_id, lock_minutes=10)
                if not token:
                    continue

                try:
                    opp = opp_from_record(rec)
                    processHit(opp)
                finally:
                    release_lock(rec_id, token)
