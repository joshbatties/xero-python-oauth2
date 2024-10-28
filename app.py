import os
import sys
import logging
from datetime import datetime, timezone
import traceback
from typing import Optional

import pandas as pd
from dateutil import parser
from xero_python.accounting import AccountingApi, Contact, Contacts, Invoice, Invoices, LineItem, LineAmountTypes
from xero_python.api_client import ApiClient
from xero_python.api_client.configuration import Configuration
from xero_python.api_client.oauth2 import OAuth2Token
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
        # Initialize Xero client
        self.api_client = self._initialize_xero_client()
        self.tenant_id = None
        self.accounting_api = None
        
        # Initialize Google Sheets service
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

    def _save_token(self, token):
        """Token saver function for OAuth2"""
        logger.info("Saving token...")
        self._current_token = token
        return token

    def _initialize_xero_client(self) -> ApiClient:
        """Initialize and return Xero API client"""
        try:
            client_id = os.environ.get('XERO_CLIENT_ID')
            client_secret = os.environ.get('XERO_CLIENT_SECRET')
            
            if not client_id or not client_secret:
                raise ValueError("Xero credentials not found in environment variables")
            
            # Remove quotes if they exist
            client_id = client_id.strip('"')
            client_secret = client_secret.strip('"')
            
            logger.info("Initializing Xero client...")
            
            # Create OAuth2Token instance
            oauth2_token = OAuth2Token(
                client_id=client_id,
                client_secret=client_secret
            )
            
            # Create Configuration
            config = Configuration(
                debug=False,
                oauth2_token=oauth2_token
            )
            
            # Create API client
            api_client = ApiClient(config, pool_threads=1)
            
            # Set the token saver
            api_client.oauth2_token_saver = self._save_token
            
            return api_client
            
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
        """Authenticate with Xero and get tenant ID"""
        try:
            logger.info("Authenticating with Xero...")
            
            # Get access token using client credentials
            token = self.api_client.get_client_credentials_token()
            logger.info("Successfully obtained access token")
            
            # Initialize the APIs with the authenticated client
            identity_api = IdentityApi(self.api_client)
            self.accounting_api = AccountingApi(self.api_client)
            
            # Get the first organization's tenant ID
            connections = identity_api.get_connections()
            for connection in connections:
                if connection.tenant_type == "ORGANISATION":
                    self.tenant_id = connection.tenant_id
                    logger.info(f"Successfully authenticated with Xero. Tenant ID: {self.tenant_id}")
                    return True
                    
            logger.error("No valid Xero organization found")
            return False
            
        except Exception as e:
            logger.error(f"Failed to authenticate with Xero: {str(e)}")
            return False

    def get_contact_id(self) -> Optional[str]:
        """Get the first contact ID from Xero"""
        try:
            contacts = self.accounting_api.get_contacts(self.tenant_id)
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

    def get_sheet_data(self):
        """Fetch data from Google Sheets"""
        try:
            spreadsheet_id = os.environ.get('GOOGLE_SHEETS_SPREADSHEET_ID')
            if not spreadsheet_id:
                raise ValueError("GOOGLE_SHEETS_SPREADSHEET_ID not found in environment variables")
            
            # Remove quotes if they exist
            spreadsheet_id = spreadsheet_id.strip('"')
            
            logger.info(f"Fetching data from Google Sheets...")
            
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range='Sheet1'
            ).execute()
            
            values = result.get('values', [])
            if not values:
                raise ValueError("No data found in spreadsheet")
                
            logger.info(f"Successfully fetched {len(values)} rows from Google Sheets")
            return values
            
        except Exception as e:
            logger.error(f"Failed to fetch spreadsheet data: {str(e)}")
            raise

    def process_spreadsheet_data(self, sheet_data) -> pd.DataFrame:
        """Process raw sheet data into a DataFrame"""
        try:
            df = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])
            
            # Convert charge columns to float
            for charge_code in self.charge_descriptions.keys():
                if charge_code in df.columns:
                    df[charge_code] = pd.to_numeric(df[charge_code], errors='coerce').fillna(0)
            
            logger.info(f"Successfully processed {len(df)} rows of data")
            return df
            
        except Exception as e:
            logger.error(f"Failed to process spreadsheet data: {str(e)}")
            raise

    def create_line_items(self, row: pd.Series) -> list[LineItem]:
        """Create line items for an invoice"""
        line_items = []
        is_credit_note = row['Type'].upper() == 'CRD'
        
        for code, description in self.charge_descriptions.items():
            if code in row:
                amount = float(row[code])
                if amount != 0:
                    if is_credit_note:
                        amount = -abs(amount)
                    else:
                        amount = abs(amount)
                    
                    line_item = LineItem(
                        description=f"{description} - {row['Job Invoice #']}",
                        quantity=1.0,
                        unit_amount=amount,
                        account_code="200",
                        tax_type="NONE",
                        line_amount=amount
                    )
                    line_items.append(line_item)
        return line_items

    def create_invoice(self, row: pd.Series, contact_id: str) -> Invoice:
        """Create an invoice object from row data"""
        try:
            line_items = self.create_line_items(row)
            if not line_items:
                raise ValueError(f"No valid charges found for shipment {row['Shipment']}")

            # Parse dates
            month, day, year = map(int, row['Inv. Date'].split('/'))
            date_str = f"{year}-{month:02d}-{day:02d}T00:00:00Z"
            date_value = parser.parse(date_str)
            
            due_date = date_value + pd.Timedelta(days=30)
            due_date_str = due_date.strftime('%Y-%m-%dT00:00:00Z')
            due_date_value = parser.parse(due_date_str)

            is_credit_note = row['Type'].upper() == 'CRD'
            
            return Invoice(
                type="ACCRECCREDIT" if is_credit_note else "ACCREC",
                contact=Contact(contact_id=contact_id),
                line_items=line_items,
                date=date_value,
                due_date=due_date_value,
                reference=row['Job Invoice #'],
                status="DRAFT",
                line_amount_types=LineAmountTypes.EXCLUSIVE
            )
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
                
            # Get and process sheet data
            sheet_data = self.get_sheet_data()
            df = self.process_spreadsheet_data(sheet_data)
            
            # Process each row
            results = []
            for idx, row in df.iterrows():
                try:
                    logger.info(f"Processing invoice {row['Job Invoice #']}")
                    
                    # Create and submit invoice
                    invoice = self.create_invoice(row, contact_id)
                    response = self.accounting_api.create_invoices(
                        self.tenant_id,
                        invoices=Invoices(invoices=[invoice]),
                        summarize_errors=False
                    )
                    
                    # Record success
                    invoice_id = response.invoices[0].invoice_id if response.invoices else None
                    results.append({
                        'shipment': row['Shipment'],
                        'job_invoice': row['Job Invoice #'],
                        'status': 'success',
                        'type': row['Type'],
                        'invoice_id': invoice_id,
                        'amount': float(row.get('Total Invoice', 0)),
                        'date': row['Inv. Date']
                    })
                    logger.info(f"Successfully created invoice {invoice_id}")
                    
                except Exception as e:
                    logger.error(f"Failed to process row {idx + 2}: {str(e)}")
                    results.append({
                        'shipment': row['Shipment'],
                        'job_invoice': row['Job Invoice #'],
                        'status': 'error',
                        'type': row['Type'],
                        'error': str(e)
                    })
            
            # Log summary
            successful = sum(1 for r in results if r['status'] == 'success')
            failed = sum(1 for r in results if r['status'] == 'error')
            total_amount = sum(float(r.get('amount', 0)) for r in results if r['status'] == 'success')
            
            logger.info(f"Processing complete. Successful: {successful}, Failed: {failed}, Total Amount: {total_amount}")
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