import os
import json
import pandas as pd
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- Facebook API Credentials ---
access_token = os.environ['FB_ACCESS_TOKEN']
app_id = os.environ['FB_APP_ID']
app_secret = os.environ['FB_APP_SECRET']
ad_account_id = 'act_1235239368340932'  # Replace with your Ad Account ID

FacebookAdsApi.init(app_id, app_secret, access_token)
account = AdAccount(ad_account_id)

# --- Define Columns to Pull ---
fields = [
    'date_start', 'publisher_platform', 'objective',
    'campaign_name', 'adset_name', 'ad_name',
    'spend', 'reach', 'frequency', 'impressions', 'cpm',
    'inline_link_clicks', 'cpc', 'ctr', 'actions'
]

params = {
    'level': 'ad',
    'date_preset': 'maximum',  # Pulls full historical data
    'time_increment': 1,
    'breakdowns': ['publisher_platform'],
    'action_breakdowns': ['action_type'],
    'limit': 500
}

# --- Fetch Meta Ads Insights ---
ads = account.get_insights(fields=fields, params=params)
df = pd.DataFrame(ads)

# --- Extract relevant actions ---
def extract_action(actions, action_type):
    if isinstance(actions, list):
        for action in actions:
            if action.get('action_type') == action_type:
                return int(action.get('value', 0))
    return 0

if 'actions' in df.columns:
    df['leads'] = df['actions'].apply(lambda x: extract_action(x, 'lead'))
    df['messaging_conversations_started'] = df['actions'].apply(lambda x: extract_action(x, 'onsite_web_chat'))
    df.drop(columns=['actions'], inplace=True)

# --- Google Sheets Setup ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
service_account_info = json.loads(os.environ['GOOGLE_SHEET_CREDS'])
service_account_info['private_key'] = service_account_info['private_key'].replace('\\n', '\n')
creds = Credentials.from_service_account_info(service_account_info, scopes=scope)
client = gspread.authorize(creds)

# --- Open Spreadsheet ---
spreadsheet = client.open("Ad_Report")  # Change if needed
worksheet = spreadsheet.worksheet("Sheet1")

# --- Load existing data ---
existing_data = pd.DataFrame(worksheet.get_all_records())

# --- Merge without duplicating rows (by date_start, ad_name, publisher_platform) ---
merge_keys = ['date_start', 'ad_name', 'publisher_platform']
if not existing_data.empty:
    df = pd.concat([existing_data, df])
    df.drop_duplicates(subset=merge_keys, keep='last', inplace=True)

# --- Write back to sheet (preserve formulas by rewriting entire sheet, formulas will auto-update) ---
worksheet.clear()
set_with_dataframe(worksheet, df)

print("✅ Meta Ads report updated successfully.")
