# rooftops.py

# Primary: map Fortellis Subscription-Id -> rooftop info
SUBSCRIPTION_TO_ROOFTOP = {
    # Mission Viejo Kia
    "a4efeb74-2289-43d5-9814-1049fd35e894": {
        "name":    "Mission Viejo Kia",
        "address": "28802 Marguerite Pkwy, Mission Viejo, CA 92692",
        "sender":  "sales@missionviejokia.com",
    },
    # Tustin Mazda
    "7a05ce2c-cf00-4748-b841-45b3442665a7": {
        "name":    "Tustin Mazda",
        "address": "28 Auto Center Dr, Tustin, CA 92782",
        "sender":  "sales@tustinmazda.com",
    },
   # Tustin Kia
    "c27d7f4f-4a4c-45c8-8154-a5de48421fc3": {
        "name":    "Tustin Kia",
        "address": "",
        "sender":  "sales@tustinkia.com",
    },
  # Tustin Hyundai
    "2c61b27b-b239-4b54-bd34-dfd73aa5a568": {
        "name":    "Tustin Hyundai",
        "address": "16 Auto Center Dr, Tustin, CA 92782",
        "sender":  "sales@tustinhyundai.com",
    },
  # Huntington Beach Mazda
    "cbb4a4f18-1693-4450-a08e-40d8df30c139": {
        "name":    "Huntington Beach Mazda",
        "address": "16800 Beach Blvd, Huntington Beach, CA 92647",
        "sender":  "sales@huntingtonbeachmazda.com",
    },
}

# If your code uses an internal dealer_key that isn't the literal Subscription-Id,
# map that dealer_key -> Subscription-Id here.
DEALERKEY_TO_SUBSCRIPTION = {
    # e.g., "mv-kia": "1a1077bb-7340-430a-8ed8-e7f67155674a",
}

ROOFTOP_INFO = {
    "Mission Viejo Kia": {
        "address": "24041 El Toro Rd, Lake Forest, CA 92630",
        "email": "sales@missionviejokia.com"
    },
    "Tustin Mazda": {
        "address": "28 Auto Center Dr, Tustin, CA 92782",
        "email": "sales@tustinmazda.com"
    },
    "Huntington Beach Mazda": {
        "address": "16800 Beach Blvd, Huntington Beach, CA 92647",
        "email": "sales@huntingtonbeachmazda.com"
    },
    "Tustin Hyundai": {
        "address": "16 Auto Center Dr, Tustin, CA 92782",
        "email": "sales@tustinhyundai.com"
    },
    "Tustin Kia": {
        "address": "",
        "email": "sales@tustinkia.com"
    },
}

def get_rooftop_info(dealer_key_or_subscription: str) -> dict:
    """
    Return a dict with {name, sender, address} for a given dealer_key or Subscription-Id.
    - Looks up dealer_key -> subscription, then subscription -> {name, sender}
    - Fills address from ROOFTOP_INFO[name] if present
    - Falls back gracefully if mappings are incomplete
    """
    # If we were given a dealer_key, translate it to a subscription id
    sub = DEALERKEY_TO_SUBSCRIPTION.get(dealer_key_or_subscription, dealer_key_or_subscription)

    # Get basic rooftop record from the subscription map
    record = SUBSCRIPTION_TO_ROOFTOP.get(sub, {})

    # Name & sender from subscription record (if any)
    name = record.get("name")
    sender = record.get("sender", "")

    # If name is still unknown, allow passing a literal rooftop name directly
    if not name and dealer_key_or_subscription in ROOFTOP_INFO:
        name = dealer_key_or_subscription

    # Final fallback name to keep things running
    if not name:
        name = "Patterson Auto Group"

    # Address from canonical info (if available)
    address = ROOFTOP_INFO.get(name, {}).get("address", "")

    return {"name": name, "sender": sender, "address": address}
