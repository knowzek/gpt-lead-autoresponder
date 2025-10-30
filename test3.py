from fortellis import get_activities, get_token
from helpers import wJson, rJson
from esQuerys import esClient
from datetime import datetime


subscription_id =  "a4efeb74-2289-43d5-9814-1049fd35e894"
opportunityId =  "2b8d0b6b-3fac-f011-814f-00505690ec8c"
customer_id = "248d0b6b-3fac-f011-814f-00505690ec8c"
# token = get_token(subscription_id)

# activities = get_activities(opportunityId, customer_id, token, subscription_id)
# wJson(activities, "jsons/activities.json")

activities = rJson("jsons/activities.json")

currDate = datetime.now().date()
docToUpdate = {
    "scheduledActivities": activities.get("scheduledActivities", []),
    "completedActivities": activities.get("completedActivities", []),
    "updated_at": currDate
}
esClient.update(index="opportunities", id=opportunityId, doc=docToUpdate)

