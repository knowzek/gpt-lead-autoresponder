from helpers import wJson, rJson
from fortellis import (
    SUB_MAP,
    get_token, 
    get_opportunity,
    get_activities,
    get_activity_by_id_v1
)

if __name__ == "__main__":
    # subscription_id = "2c61b27b-b239-4b54-bd34-dfd73aa5a568"
    subscription_id = "bb4a4f18-1693-4450-a08e-40d8df30c139"
    # opportunityId = "6f3636eb-3eac-f011-814f-00505690ec8c"
    opportunityId = "ac4cc77d-2dac-f011-814f-00505690ec8c"

    token = get_token(subscription_id)

    opp : dict = get_opportunity(opportunityId, token, subscription_id)
    customerID = opp.get("customer", {}).get("id", None)
    activities = get_activities(opportunityId, customerID, token, subscription_id)
    
    opp.update(
        {
            "scheduledActivities": activities.get("scheduledActivities", []),
            "completedActivities": activities.get("completedActivities", [])
        }
    )

    wJson(opp, f"jsons/{opportunityId}.json")

    opp = rJson(f"jsons/{opportunityId}.json")

    if not 'completedActivitiesFull' in opp:
        opp['completedActivitiesFull'] = []

    # contKey = True
    for act in opp['completedActivities']:
        # if act['activityId'] == "463e997d-3fac-f011-814f-00505690ec8c":
        #     contKey = False
        #     continue

        # if contKey:
        #     continue
            
        try:
            newItem = get_activity_by_id_v1(act['activityId'], token, subscription_id)
            opp['completedActivitiesFull'].append(newItem)
        except:
            opp['completedActivitiesFull'].append({
                'activityId': act['activityId']
            })
        wJson(opp, f"jsons/{opportunityId}.json")
