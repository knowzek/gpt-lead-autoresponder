import os
import gspread
from google.oauth2 import service_account

# Load credentials from environment variables
def _get_creds():
    creds_info = {
        "type": "service_account",
        "project_id": os.getenv("GOOGLE_PROJECT_ID"),
        "private_key_id": os.getenv("GOOGLE_PRIVATE_KEY_ID"),
        "private_key": os.getenv("GOOGLE_PRIVATE_KEY").replace('\\n', '\n'),
        "client_email": os.getenv("GOOGLE_CLIENT_EMAIL"),
        "client_id": os.getenv("GOOGLE_CLIENT_ID"),
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{os.getenv('GOOGLE_CLIENT_EMAIL')}"
    }

    return service_account.Credentials.from_service_account_info(
        creds_info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )


# Connect to the Sheet
def _connect():
    creds = _get_creds()
    client = gspread.authorize(creds)
    spreadsheet = os.getenv("STATE_SPREADSHEET_NAME")
    worksheet = os.getenv("STATE_WORKSHEET_NAME")
    print(f"🔗 Connecting to spreadsheet: {spreadsheet}, worksheet: {worksheet}")
    sheet = client.open(spreadsheet).worksheet(worksheet)
    print(f"✅ Sheet loaded with {len(sheet.get_all_values())} rows")
    return sheet


# Check if activity ID was already processed
def was_processed(activity_id):
    sheet = _connect()
    activity_ids = sheet.col_values(1)
    print(f"🧾 Activity IDs in sheet: {activity_ids}")
    print(f"🔍 Comparing against: {activity_id}")
    return activity_id in activity_ids


# Add a new activity ID to the sheet
def mark_processed(activity_id):
    sheet = _connect()
    print(f"✍️ Marking lead as processed: {activity_id}")
    sheet.append_row([activity_id])

