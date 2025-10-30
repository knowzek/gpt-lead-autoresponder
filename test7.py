from fortellis import get_opportunity, get_token
from helpers import rJson, wJson

keys = [
    "bb4a4f18-1693-4450-a08e-40d8df30c139",
    "a4efeb74-2289-43d5-9814-1049fd35e894",
    "2c61b27b-b239-4b54-bd34-dfd73aa5a568",
    "7a05ce2c-cf00-4748-b841-45b3442665a7"
]

opp_id = "d24d9cf5-b553-f011-8166-0050569021d8"



for key in keys:
    token = get_token(key)

    try:

        opp_data = get_opportunity(opportunity_id=opp_id, token= token, dealer_key= key)
        wJson(opp_data, f"jsons/oldOPPs/{key}.json")
        print(key)
    except Exception as e:
        print(e)
        print(repr(e))
        print(str(e))