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
ad_account_id = 'act_1235239368340932'  # replace with actual ad account ID

FacebookAdsApi.init(app_id, app_secret, access_token)
account = AdAccount(ad_account_id)

# --- Report Parameters ---
fields = [
    'date_start', 'publisher_platform', 'country', 'objective', 'campaign_name',
    'adset_name', 'ad_name', 'spend', 'reach', 'frequency', 'impressions',
    'cpm', 'inline_link_clicks', 'cpc', 'ctr', 'actions'
]

params = {
    'level': 'ad',
    'date_preset': 'today',
    'time_increment': 1,
    'breakdowns': ['country'],  # Removed publisher_platform to avoid error
    'action_breakdowns': ['action_type'],
    'limit': 500
}

# --- Fetch Data ---
ads = account.get_insights(fields=fields, params=params)
df = pd.DataFrame(ads)

# --- Extract Action Metrics ---
def extract_action(action_list, action_type):
    if isinstance(action_list, list):
        for action in action_list:
            if action.get('action_type') == action_type:
                return int(action.get('value', 0))
    return 0

if 'actions' in df.columns:
    df['post_engagements'] = df['actions'].apply(lambda x: extract_action(x, 'post_engagement'))
    df['leads'] = df['actions'].apply(lambda x: extract_action(x, 'lead'))
    df['messaging_conversations_started'] = df['actions'].apply(lambda x: extract_action(x, 'onsite_web_chat'))
    df.drop(columns=['actions'], inplace=True)

# Rename for consistency with sheet columns
df.rename(columns={
    'date_start': 'Day',
    'publisher_platform': 'Platform',
    'country': 'Country',
    'objective': 'Objective',
    'campaign_name': 'Campaign name',
    'adset_name': 'Ad set name',
    'ad_name': 'Ad name',
    'spend': 'Amount spent (INR)',
    'reach': 'Reach',
    'frequency': 'Frequency',
    'impressions': 'Impressions',
    'cpm': 'CPM (cost per 1,000 impressions)',
    'inline_link_clicks': 'Link clicks',
    'cpc': 'CPC (cost per link click)',
    'ctr': 'CTR (all)',
    'post_engagements': 'Post engagements',
    'leads': 'Leads',
    'messaging_conversations_started': 'Messaging conversations started'
}, inplace=True)

# Add empty columns for formula-based fields
df['Engagement rate'] = ''
df['Cost per Lead'] = ''
df['Cost per new messaging contact'] = ''
df['Result Type'] = ''
df['Results'] = ''

# --- Google Sheets Setup ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
service_account_info = json.loads(os.environ['GOOGLE_SHEET_CREDS'])
service_account_info['private_key'] = service_account_info['private_key'].replace('\\n', '\n')

creds = Credentials.from_service_account_info(service_account_info, scopes=scope)
client = gspread.authorize(creds)

spreadsheet = client.open("Ad_Report")
worksheet = spreadsheet.worksheet("Sheet1")

# --- Clear Today's Rows ---
existing_data = pd.DataFrame(worksheet.get_all_records())
today_str = datetime.today().strftime('%Y-%m-%d')

if not existing_data.empty and 'Day' in existing_data.columns:
    existing_data = existing_data[existing_data['Day'] != today_str]
    worksheet.clear()
    set_with_dataframe(worksheet, existing_data)

# --- Append Fresh Data ---
existing_data = pd.DataFrame(worksheet.get_all_records())
final_data = pd.concat([existing_data, df], ignore_index=True)
worksheet.clear()
set_with_dataframe(worksheet, final_data)

print("✅ Report updated successfully.")
