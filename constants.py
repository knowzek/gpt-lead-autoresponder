import os

from helpers import rJson, wJson, _html_to_text
from dotenv import load_dotenv
load_dotenv()

# === Modes & Safety ==================================================
USE_EMAIL_MODE = False                              # legacy inbox mode off by default
SAFE_MODE = os.getenv("PATTI_SAFE_MODE", "1") in ("1","true","True")  # blocks real customer emails

TEST_FROM = os.getenv("FORTELLIS_TEST_FROM", "sales@claycooleygenesisofmesquite.edealerhub.com")
TEST_TO   = os.getenv("FORTELLIS_TEST_TO",   "rishabhrajendraprasad.shukla@cdk.com")

MICKEY_EMAIL = os.getenv("MICKEY_EMAIL")  # proof recipient
ELIGIBLE_UPTYPES = "internet"
PROOF_RECIPIENTS = [
    "knowzek@gmail.com",
    "mickeyt@the-dms.com",
    "dev.almousa@gmail.com"
]

# Preferred activity type + rooftop-safe fallback
ACTIVITY_COMBOS = [
    ("Send Email", 3),          # try first
    ("Send Email/Letter", 14),  # fallback
]

# Per-run cache: sub_id -> (name, type) that succeeded
GOOD_ACTIVITY_COMBO: dict[str, tuple[str, int]] = {}

PATTI_FIRST_REPLY_SENTINEL = "[patti:first-reply]"




# === Dealership name → dealer_key (keys must match your SUB_MAP env) ==
DEALERSHIP_TO_KEY = {
    "Tustin Mazda": "tustin-mazda",
    "Huntington Beach Mazda": "huntington-beach-mazda",
    "Tustin Hyundai": "tustin-hyundai",
    "Mission Viejo Kia": "mission-viejo-kia",
}

# === SRP URL bases ===================================================
DEALERSHIP_URL_MAP = {
    "Tustin Mazda": "https://www.tustinmazda.com/used-inventory/",
    "Huntington Beach Mazda": "https://www.huntingtonbeachmazda.com/used-inventory/",
    "Tustin Hyundai": "https://www.tustinhyundai.com/used-inventory/",
    "Mission Viejo Kia": "https://www.missionviejokia.com/used-inventory/",
    "Patterson Auto Group": "https://www.pattersonautos.com/used-inventory/",
}

# === Salesperson & dealership inference (unchanged, trimmed) =========
SALES_PERSON_MAP = {
    "Madeleine": "Madeleine Demo",
    "Pavan": "Pavan Singh",
    "Joe B": "Joe B",
    "Desk Manager 1": "Jim Feinstein",
    "Bloskie, Terry": "Terry Bloskie",
    "Test606, CLB": "Roozbeh",
}

DEALERSHIP_MAP = {
    "Podium": "Tustin Hyundai",
    "Podium Webchat": "Tustin Hyundai",
    "CarNow": "Mission Viejo Kia",
    "Madeleine": "Tustin Mazda",
    "Pavan": "Tustin Hyundai",
    "Joe B": "Huntington Beach Mazda",
    "Bloskie, Terry": "Tustin Hyundai",
    "Test606, CLB": "Tustin Hyundai",
    "Desk Manager 1": "Mission Viejo Kia",
}

CONTACT_INFO_MAP = {
    "Tustin Hyundai":    "Tustin Hyundai, 16 Auto Center Dr, Tustin, CA 92782 | (714) 838-4554 | https://www.tustinhyundai.com/",
    "Mission Viejo Kia": "Mission Viejo Kia, 24041 El Toro Rd, Lake Forest, CA 92630 | (949) 768-7900 | https://www.missionviejokia.com/",
    "Tustin Mazda":      "Tustin Mazda, 28 Auto Center Dr, Tustin, CA 92782 | (714) 258-2300 | https://www.tustinmazda.com/",
    "Huntington Beach Mazda": "Huntington Beach Mazda, 16800 Beach Blvd, Huntington Beach, CA 92647 | (714) 847-7686 | https://www.huntingtonbeachmazda.com/",
    "Patterson Auto Group":   "Patterson Auto Group, 123 Main St, Irvine, CA 92618 | (949) 555-0100 | https://www.pattersonautos.com/",
}



WINDOW_MIN = int(os.getenv("DELTA_WINDOW_MINUTES", "1440"))  
PAGE_SIZE  = int(os.getenv("DELTA_PAGE_SIZE", "500"))

CUSTOMER_URL = "https://api.fortellis.io/sales/v1/elead/customers"
