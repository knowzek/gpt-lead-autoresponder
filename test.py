import asyncio
from app.database import AsyncSessionLocal
from app.crud import create_dealership

async def main():

    dealershipTMP = {
        "subscriptionId": "a4efeb74-2289-43d5-9814-1049fd35e894",
        "name":    "Mission Viejo Kia",
        "address": "28802 Marguerite Pkwy, Mission Viejo, CA 92692",
        "email": "sales@missionviejokia.com",
        "senderEmail":  "sales@missionviejokia.edealerhub.com"
    }
    async with AsyncSessionLocal() as session:
        dealership = await create_dealership(session, **dealershipTMP)
        print(dealership)

if __name__ == "__main__":

    asyncio.run(main())