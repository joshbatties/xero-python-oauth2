import os
import sys
import logging
from datetime import datetime, timezone
import traceback
from typing import Optional, Dict

import pandas as pd
from dateutil import parser
import requests
from xero_python.api_client import Configuration, ApiClient
from xero_python.api_client.oauth2 import OAuth2Token
from xero_python.exceptions import ApiException
from xero_python.accounting import AccountingApi, Contact, Contacts, Invoice, Invoices, LineItem, LineAmountTypes
from xero_python.identity import IdentityApi
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('invoice_automation.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class InvoiceProcessor:
    def __init__(self):
        self.tenant_id = os.environ.get('XERO_TENANT_ID', '').strip('"')
        self.client_id = os.environ.get('XERO_CLIENT_ID', '').strip('"')
        self.client_secret = os.environ.get('XERO_CLIENT_SECRET', '').strip('"')
        
        # Initialize services
        self.api_client = None
        self.accounting_api = None
        self.sheets_service = self._initialize_sheets_service()
        
        # Charge descriptions mapping
        self.charge_descriptions = {
            'BRK': 'Brokerage',
            'CDS': 'Customs Duties',
            'DST': 'Destination Charges',
            'FRT': 'Freight Charges',
            'INS': 'Insurance',
            'LOD': 'Loading Charges',
            'ORG': 'Origin Charges',
            'OBR': 'Other Brokerage',
            'OBO': 'Other Charges',
            'TRN': 'Transportation'
        }

    def _get_xero_access_token(self) -> str:
        """Get access token directly from Xero"""
        try:
            logger.info("Getting Xero access token...")
            
            # Set the correct scopes as a single space-separated string
            scopes = 'offline_access openid profile email accounting.transactions accounting.contacts.read'
            
            token_url = 'https://identity.xero.com/connect/token'
            response = requests.post(
                token_url,
                auth=(self.client_id, self.client_secret),
                data={
                    'grant_type': 'client_credentials',
                    'scope': scopes
                }
            )
            
            logger.info(f"Token request status: {response.status_code}")
            
            if response.status_code != 200:
                logger.error(f"Token response: {response.text}")
                raise Exception(f"Failed to get access token. Status: {response.status_code}, Response: {response.text}")
            
            token_data = response.json()
            access_token = token_data.get('access_token')
            
            if not access_token:
                raise Exception("No access token in response")
                
            logger.info("Successfully obtained access token")
            return access_token
            
        except Exception as e:
            logger.error(f"Error getting access token: {str(e)}")
            raise

    def _initialize_xero_client(self, access_token: str) -> None:
        """Initialize Xero API client with access token"""
        try:
            configuration = Configuration(
                access_token=access_token
            )
            
            self.api_client = ApiClient(configuration)
            self.accounting_api = AccountingApi(self.api_client)
            logger.info("Successfully initialized Xero client")
            
        except Exception as e:
            logger.error(f"Failed to initialize Xero client: {str(e)}")
            raise

    def _initialize_sheets_service(self):
        """Initialize and return Google Sheets service"""
        try:
            logger.info("Initializing Google Sheets service...")
            
            credentials_path = 'credentials.json'
            if not os.path.exists(credentials_path):
                raise FileNotFoundError("credentials.json not found")
            
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            )
            
            return build('sheets', 'v4', credentials=credentials)
            
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets service: {str(e)}")
            raise

    def authenticate_xero(self) -> bool:
        """Authenticate with Xero"""
        try:
            logger.info("Authenticating with Xero...")
            
            # Get access token
            access_token = self._get_xero_access_token()
            
            # Initialize client with token
            self._initialize_xero_client(access_token)
            
            # Verify tenant ID
            if not self.tenant_id:
                identity_api = IdentityApi(self.api_client)
                connections = identity_api.get_connections()
                for connection in connections:
                    if connection.tenant_type == "ORGANISATION":
                        self.tenant_id = connection.tenant_id
                        logger.info(f"Using tenant ID: {self.tenant_id}")
                        break
                if not self.tenant_id:
                    raise ValueError("No valid Xero organization found")
            
            logger.info("Successfully authenticated with Xero")
            return True
            
        except Exception as e:
            logger.error(f"Failed to authenticate with Xero: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def get_contact_id(self) -> Optional[str]:
        """Get the first contact ID from Xero"""
        try:
            logger.info(f"Getting contacts for tenant {self.tenant_id}")
            contacts = self.accounting_api.get_contacts(
                xero_tenant_id=self.tenant_id
            )
            if contacts.contacts:
                contact_id = contacts.contacts[0].contact_id
                logger.info(f"Using contact ID: {contact_id}")
                return contact_id
            else:
                logger.error("No contacts found in Xero")
                return None
        except Exception as e:
            logger.error(f"Failed to get contact ID: {str(e)}")
            return None

    def create_invoice(self, row: pd.Series, contact_id: str):
        """Create an invoice object from row data"""
        try:
            line_items = []
            for code, description in self.charge_descriptions.items():
                if code in row and float(row[code]) != 0:
                    amount = float(row[code])
                    if row['Type'].upper() == 'CRD':
                        amount = -abs(amount)
                    
                    line_items.append({
                        "Description": f"{description} - {row['Job Invoice #']}",
                        "Quantity": 1.0,
                        "UnitAmount": amount,
                        "AccountCode": "200",
                        "TaxType": "NONE",
                        "LineAmount": amount
                    })

            if not line_items:
                raise ValueError(f"No valid charges found for shipment {row['Shipment']}")

            # Parse dates
            month, day, year = map(int, row['Inv. Date'].split('/'))
            date_str = f"{year}-{month:02d}-{day:02d}"
            due_date = pd.to_datetime(date_str) + pd.Timedelta(days=30)
            due_date_str = due_date.strftime('%Y-%m-%d')

            invoice_data = {
                "Invoices": [{
                    "Type": "ACCRECCREDIT" if row['Type'].upper() == 'CRD' else "ACCREC",
                    "Contact": {"ContactID": contact_id},
                    "LineItems": line_items,
                    "Date": date_str,
                    "DueDate": due_date_str,
                    "Reference": row['Job Invoice #'],
                    "Status": "DRAFT"
                }]
            }
            
            return invoice_data
            
        except Exception as e:
            logger.error(f"Failed to create invoice object: {str(e)}")
            raise

    def run(self):
        """Main execution function"""
        try:
            logger.info("Starting automated invoice processing")
            
            # Authenticate with Xero
            if not self.authenticate_xero():
                logger.error("Failed to authenticate with Xero")
                return False
            
            # Get contact ID
            contact_id = self.get_contact_id()
            if not contact_id:
                logger.error("Failed to get contact ID")
                return False
            
            # Get spreadsheet data
            spreadsheet_id = os.environ.get('GOOGLE_SHEETS_SPREADSHEET_ID', '').strip('"')
            if not spreadsheet_id:
                logger.error("Spreadsheet ID not found in environment variables")
                return False
            
            # Fetch sheet data
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range='Sheet1'
            ).execute()
            
            values = result.get('values', [])
            if not values:
                logger.error("No data found in spreadsheet")
                return False
            
            # Process data
            df = pd.DataFrame(values[1:], columns=values[0])
            logger.info(f"Processing {len(df)} invoices")
            
            # Process each row
            results = []
            for idx, row in df.iterrows():
                try:
                    logger.info(f"Processing invoice {row['Job Invoice #']}")
                    
                    # Create invoice data
                    invoice_data = self.create_invoice(row, contact_id)
                    
                    # Submit to Xero
                    response = self.accounting_api.create_invoices(
                        xero_tenant_id=self.tenant_id,
                        invoices=invoice_data,
                        summarize_errors=False
                    )
                    
                    # Record success
                    invoice_id = response.invoices[0].invoice_id if response.invoices else None
                    results.append({
                        'shipment': row['Shipment'],
                        'job_invoice': row['Job Invoice #'],
                        'status': 'success',
                        'invoice_id': invoice_id
                    })
                    logger.info(f"Successfully created invoice {invoice_id}")
                    
                except Exception as e:
                    logger.error(f"Failed to process row {idx + 2}: {str(e)}")
                    results.append({
                        'shipment': row['Shipment'],
                        'job_invoice': row['Job Invoice #'],
                        'status': 'error',
                        'error': str(e)
                    })
            
            # Log summary
            successful = sum(1 for r in results if r['status'] == 'success')
            failed = sum(1 for r in results if r['status'] == 'error')
            
            logger.info(f"Processing complete. Successful: {successful}, Failed: {failed}")
            return True
            
        except Exception as e:
            logger.error(f"Automation failed: {str(e)}")
            logger.error(traceback.format_exc())
            return False

if __name__ == '__main__':
    try:
        processor = InvoiceProcessor()
        success = processor.run()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)