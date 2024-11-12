from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

creds = Credentials.from_service_account_file('path/to/service_account.json')
service = build('sheets', 'v4', credentials=creds)

sheet_id = 'your_sheet_id'
range_name = 'Sheet1!A1:E10'  # Adjust as needed

result = service.spreadsheets().values().get(
    spreadsheetId=sheet_id, range=range_name).execute()
values = result.get('values', [])

from xero_python.accounting import AccountingApi
from xero_python.api_client import ApiClient
from xero_python.api_client.configuration import Configuration
from xero_python.api_client.oauth2 import OAuth2Token

client_id = 'YOUR_CLIENT_ID'
client_secret = 'YOUR_CLIENT_SECRET'
refresh_token = 'YOUR_REFRESH_TOKEN'

token = OAuth2Token(
    client_id=client_id,
    client_secret=client_secret,
    refresh_token=refresh_token
)

api_client = ApiClient(
    oauth2_token=token,
    configuration=Configuration(oauth2_token=token)
)

accounting_api = AccountingApi(api_client)