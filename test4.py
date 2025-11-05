from rooftops import SUBSCRIPTION_TO_ROOFTOP, ROOFTOP_INFO
from datetime import datetime
from esQuerys import esClient


currDate = datetime.now().date()

for key, val in SUBSCRIPTION_TO_ROOFTOP.items():
    print(val['name'])
    info = ROOFTOP_INFO[val['name']]
    fullDoc = {
        "subscriptionId": key,
        "name": val['name'],
        "email": info['email'],
        "sender": val['sender'],
        "address": info['address'],
        "created_at": currDate,
        "updated_at": currDate,     
    }

    esClient.index(index="dealerships", document=fullDoc, id=key)

