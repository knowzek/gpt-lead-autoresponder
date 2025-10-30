from helpers import rJson, wJson, get_names_in_dir
from esQuerys import esClient, getNewData
from fortellis import (
    SUB_MAP,
    get_token, 
    get_opportunity,
    get_activities,
    get_activity_by_id_v1
)
from test6 import sortActivities

def findActivityByType(activities, typeNo):
    for act in activities:
        if act['activityType'] == typeNo:
            return act
    return None



if __name__ == "__main__":
    data = getNewData()

    lst = get_names_in_dir("jsons/newOPPs/")
    lst = [id.replace('.json', '') for id in lst]

    for hit in data:
        opp = hit['_source']
        subscription_id = opp['_subscription_id']
        opportunityId = opp['opportunityId']
        if opportunityId in lst:
            print("skipping:", opportunityId)
            oldOpp = rJson(f"jsons/newOPPs/{opportunityId}.json")
            if 'InternetUpActivity' in oldOpp:
                oldOpp['firstActivity'] = oldOpp['InternetUpActivity']
                del oldOpp['InternetUpActivity']
                wJson(oldOpp, f"jsons/newOPPs/{opportunityId}.json")
            continue
        print("process:", opportunityId)
        try:
            token = get_token(subscription_id)
            customerID = opp.get("customer", {}).get("id", None)
            completedActivities = opp['completedActivities']
            # act = findActivityByType(completedActivities, 13)
            act = sortActivities(completedActivities)[0]
            newItem = get_activity_by_id_v1(act['activityId'], token, subscription_id)
            # print()
            opp['firstActivity'] = newItem
            wJson(opp, f"jsons/newOPPs/{opportunityId}.json")
        except:
            print(act)
            exit()

