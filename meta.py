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
ad_account_id = 'act_1235239368340932'

FacebookAdsApi.init(app_id, app_secret, access_token)
account = AdAccount(ad_account_id)

# --- Report Fields ---
fields = [
    'date_start', 'campaign_name', 'adset_name', 'ad_name', 'objective',
    'spend', 'reach', 'impressions', 'inline_link_clicks', 'actions'
]

params = {
    'level': 'ad',
    'date_preset': 'last_3d',
    'time_increment': 1,
    'breakdowns': ['country'],
    'action_breakdowns': ['action_type'],
    'limit': 500
}

# --- Fetch Insights ---
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
    df['Meta leads'] = df['actions'].apply(lambda x: extract_action(x, 'lead'))
    df['Meta leads'] = df['Meta leads'].where(df['Meta leads'] > 0, df['actions'].apply(lambda x: extract_action(x, 'onsite_conversion.lead_grouped')))
    df['Messaging conversations started'] = df['actions'].apply(lambda x: extract_action(x, 'onsite_web_chat'))
    df['Post engagements'] = df['actions'].apply(lambda x: extract_action(x, 'post_engagement'))
    df['Purchases'] = df['actions'].apply(lambda x: extract_action(x, 'purchase'))
    df.drop(columns=['actions'], inplace=True)

# --- Google Sheets Setup ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
service_account_info = json.loads(os.environ['GOOGLE_SHEET_CREDS'])
service_account_info['private_key'] = service_account_info['private_key'].replace('\\n', '\n')

creds = Credentials.from_service_account_info(service_account_info, scopes=scope)
client = gspread.authorize(creds)

spreadsheet = client.open("Ad_Report")
worksheet = spreadsheet.worksheet("Sheet1")

# --- Load existing data ---
existing_data = pd.DataFrame(worksheet.get_all_records())

# --- Rename and reorder columns to match your structure ---
df = df.rename(columns={
    'date_start': 'Day',
    'campaign_name': 'Campaign name',
    'adset_name': 'Ad set name',
    'ad_name': 'Ad name',
    'objective': 'Objective',
    'country': 'Country',
    'spend': 'Amount spent (INR)',
    'reach': 'Reach',
    'impressions': 'Impressions',
    'inline_link_clicks': 'Link clicks'
})

desired_order = [
    'Day', 'Campaign name', 'Ad set name', 'Ad name', 'Objective', 'Country',
    'Amount spent (INR)', 'Reach', 'Impressions', 'Post engagements',
    'Link clicks', 'Meta leads', 'Messaging conversations started', 'Purchases'
]

df = df[desired_order]

# --- Proceed only if new data exists ---
if not df.empty:
    # Drop overlapping dates
    date_range = df['Day'].unique().tolist()
    if not existing_data.empty and 'Day' in existing_data.columns:
        existing_data = existing_data[~existing_data['Day'].isin(date_range)]

    # Merge and upload
    final_data = pd.concat([existing_data, df], ignore_index=True)
    worksheet.clear()
    set_with_dataframe(worksheet, final_data)
    print(f"✅ Report updated with {len(df)} new rows.")
else:
    print("⚠️ No new data returned from Meta Ads API — Sheet not modified.")
