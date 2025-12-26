# feature_flags.py

TEST_OPP_IDS = {
    "050a81e9-78d4-f011-814f-00505690ec8c",  # your current Mazda KBB test opp
    "e7f79ae6-0cb9-f011-814f-00505690ec8c",
}

TEST_CUSTOMER_EMAILS = {
    "kristin@blackoctopusai.com",
    "knowzek@gmail.com",
    "mickeyt@the-dms.com",
}


def is_test_opp(opportunity: dict) -> bool:
    """
    Return True if this opportunity should flow through the new
    Outlook-based Patti path (for now: just your test opps).
    """
    if not opportunity:
        return False

    # By ID
    opp_id = opportunity.get("opportunityId") or opportunity.get("id")
    if opp_id in TEST_OPP_IDS:
        return True

    # By customer email(s)
    cust = opportunity.get("customer") or {}
    for e in cust.get("emails") or []:
        addr = (e.get("address") or "").strip().lower()
        if addr in TEST_CUSTOMER_EMAILS:
            return True

    # Sometimes you also index customerEmail flat:
    flat_email = (opportunity.get("customerEmail") or "").strip().lower()
    if flat_email in TEST_CUSTOMER_EMAILS:
        return True

    return False
