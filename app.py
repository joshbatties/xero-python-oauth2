# -*- coding: utf-8 -*-
import os
import sys
import logging
from datetime import datetime, timezone
import traceback
from typing import Optional

import pandas as pd
from dateutil import parser
from flask import Flask
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

class AutomatedInvoiceProcessor:
    def __init__(self):
        self.app = Flask(__name__)
        self.app.config.from_pyfile("config.py")
        
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

    def _initialize_xero_client(self) -> ApiClient:
        """Initialize and return Xero API client"""
        try:
            token = OAuth2Token(
                client_id=self.app.config["CLIENT_ID"],
                client_secret=self.app.config["CLIENT_SECRET"]
            )
            
            config = Configuration(
                debug=self.app.config["DEBUG"],
                oauth2_token=token
            )
            
            return ApiClient(config, pool_threads=1)
        except Exception as e:
            logger.error(f"Failed to initialize Xero client: {str(e)}")
            raise

    def _initialize_sheets_service(self):
        """Initialize and return Google Sheets service"""
        try:
            credentials_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), 
                'credentials.json'
            )
            
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            
            return build('sheets', 'v4', credentials=credentials)
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets service: {str(e)}")
            raise

    async def authenticate_xero(self) -> bool:
        """Authenticate with Xero and get tenant ID"""
        try:
            # Get access token using client credentials
            token = await self.api_client.get_client_credentials_token()
            
            # Set the token
            self.api_client.configuration.oauth2_token = token
            
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

    def get_sheet_data(self, spreadsheet_id: str):
        """Fetch data from Google Sheets"""
        try:
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
            amount = float(row.get(code, 0))
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

    async def run(self):
        """Main execution function"""
        try:
            logger.info("Starting automated invoice processing")
            
            # Authenticate with Xero
            if not await self.authenticate_xero():
                logger.error("Failed to authenticate with Xero")
                return False
                
            # Get contact ID
            contact_id = self.get_contact_id()
            if not contact_id:
                logger.error("Failed to get contact ID")
                return False
                
            # Get spreadsheet ID from environment
            spreadsheet_id = os.getenv('GOOGLE_SHEETS_SPREADSHEET_ID')
            if not spreadsheet_id:
                logger.error("Spreadsheet ID not configured")
                return False
                
            # Get and process sheet data
            sheet_data = self.get_sheet_data(spreadsheet_id)
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
    processor = AutomatedInvoiceProcessor()
    import asyncio
    success = asyncio.run(processor.run())
    sys.exit(0 if success else 1)