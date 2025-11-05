from helpers import wJson, rJson
from fortellis import (
    SUB_MAP,
    get_token,
    get_recent_opportunities,   
    get_opportunity,
    get_customer_by_url,
    get_activity_by_url,
    get_activity_by_id_v1,
    send_opportunity_email_activity,
    add_opportunity_comment,
    add_vehicle_sought,
    schedule_activity,
    complete_activity,
    search_activities_by_opportunity,  # <-- add this
)

if __name__ == "__main__":
    lead = {
        "_subscription_id": "bb4a4f18-1693-4450-a08e-40d8df30c139",
        "opportunityId": "2036c7b6-86a7-f011-814f-00505690ec8c",
        "activityId": None,
        "source": "Mazda",
        "upType": "Internet",
        "soughtVehicles": [
            {
                "id": "2136c7b6-86a7-f011-814f-00505690ec8c",
                "isNew": True,
                "yearFrom": 2025,
                "yearTo": 2025,
                "make": "Mazda",
                "model": "MX-5 Miata",
                "isPrimary": True
            }
        ],
        "salesTeam": [
            {
                "id": "b67e5380-34d0-ea11-a97e-005056b72b57",
                "firstName": "Veronica",
                "lastName": "Paco",
                "isPrimary": True,
                "isPositionPrimary": True,
                "positionName": "Salesperson",
                "positionCode": "S",
                "links": []
            }
        ],
        "customer": {
            "id": "1e36c7b6-86a7-f011-814f-00505690ec8c",
            "links": [
                {
                    "rel": "self",
                    "href": "https://api.fortellis.io/sales/v1/elead/customers/1e36c7b6-86a7-f011-814f-00505690ec8c",
                    "method": "GET",
                    "title": "Fetch Customer"
                }
            ]
        },
        "tradeIns": None,
        "createdBy": None
    }

    subscription_id = lead.get("_subscription_id")
    opportunity_id = lead.get("opportunityId")

    token = get_token(subscription_id)
    print(token)

    # recent_acts = search_activities_by_opportunity(
    #     opportunity_id, token, subscription_id, page=1, page_size=50
    # )

    # wJson(recent_acts, "jsons/recent_acts.json")

    # opportunity = get_opportunity(opportunity_id, token, subscription_id)

    # wJson(opportunity, "jsons/opportunity.json")

    # customer_url = "https://api.fortellis.io/sales/v1/elead/customers/1e36c7b6-86a7-f011-814f-00505690ec8c"

    # customer_data = get_customer_by_url(customer_url, token, subscription_id)

    # wJson(customer_data, "jsons/customer_data.json")





    pass