import os
import sys
import json
import time
import logging
import traceback
from datetime import datetime, timezone
from dateutil import parser
import pandas as pd
from typing import List, Dict, Any
from google.oauth2 import service_account
from googleapiclient.discovery import build
from xero_python.api_client import ApiClient, Configuration
from xero_python.api_client.oauth2 import OAuth2Token
from xero_python.accounting import (
    AccountingApi,
    Invoices,
    Invoice,
    Contact,
    LineItem,
    LineAmountTypes
)
from xero_python.exceptions import AccountingBadRequestException

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_sheets_service():
    """Create and return a Google Sheets service instance"""
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    try:
        credentials_info = json.loads(os.getenv('GOOGLE_CREDENTIALS'))
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info, scopes=SCOPES
        )
        service = build('sheets', 'v4', credentials=credentials)
        return service
    except Exception as e:
        logger.error(f"Error creating sheets service: {str(e)}")
        raise

class InvoiceProcessor:
    def __init__(self, api_client, tenant_id: str):
        self.accounting_api = AccountingApi(api_client)
        self.tenant_id = tenant_id
        self.sheets_service = create_sheets_service()
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

    def create_invoice(self, row: pd.Series, contact_id: str) -> Invoice:
        try:
            line_items = self.create_line_items(row)
            if not line_items:
                raise ValueError(f"No valid charges found for shipment {row['Shipment']}")

            # Convert MM/DD/YYYY to YYYY-MM-DDT00:00:00Z format
            month, day, year = map(int, row['Inv. Date'].split('/'))
            date_str = f"{year}-{month:02d}-{day:02d}T00:00:00Z"
            date_value = parser.parse(date_str)

            # Calculate due date (30 days later)
            due_date = date_value + pd.Timedelta(days=30)
            due_date_str = due_date.strftime('%Y-%m-%dT00:00:00Z')
            due_date_value = parser.parse(due_date_str)

            is_credit_note = row['Type'].upper() == 'CRD'

            invoice = Invoice(
                type="ACCRECCREDIT" if is_credit_note else "ACCREC",
                contact=Contact(contact_id=contact_id),
                line_items=line_items,
                date=date_value,
                due_date=due_date_value,
                reference=row['Job Invoice #'],
                status="DRAFT",
                line_amount_types=LineAmountTypes.EXCLUSIVE
            )

            logger.debug(f"Created invoice object: {invoice.to_dict()}")
            return invoice

        except Exception as e:
            logger.error(f"Error creating invoice object: {str(e)}", exc_info=True)
            raise

    def get_sheet_data(self, spreadsheet_id: str) -> List[List[str]]:
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range='Sheet1'
            ).execute()

            values = result.get('values', [])
            if not values:
                raise ValueError("No data found in spreadsheet")
            return values
        except Exception as e:
            logger.error(f"Failed to fetch spreadsheet data: {str(e)}", exc_info=True)
            raise

    def process_spreadsheet_data(self, sheet_data: List[List[str]]) -> pd.DataFrame:
        try:
            if not sheet_data:
                raise ValueError("No data found in spreadsheet")

            df = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

            # Convert charge columns to float
            for charge_code in self.charge_descriptions.keys():
                df[charge_code] = pd.to_numeric(df[charge_code], errors='coerce').fillna(0)

            logger.info(f"Processed spreadsheet data into DataFrame with {len(df)} rows.")
            return df
        except Exception as e:
            logger.error(f"Error processing spreadsheet data: {str(e)}", exc_info=True)
            raise

    def create_line_items(self, row: pd.Series) -> List[LineItem]:
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
                    account_code="200",  # Update account code as needed
                    tax_type="NONE",
                    line_amount=amount
                )
                line_items.append(line_item)
        return line_items

    def process_invoices(self, df: pd.DataFrame, contact_id: str) -> List[Dict[str, Any]]:
        results = []

        for idx, row in df.iterrows():
            try:
                logger.info(f"Processing row {idx + 2}: Date={row['Inv. Date']}, Job Invoice={row['Job Invoice #']}")

                invoice = self.create_invoice(row, contact_id)

                response = self.accounting_api.create_invoices(
                    self.tenant_id,
                    invoices=Invoices(invoices=[invoice]),
                    summarize_errors=False
                )

                invoice_id = None
                if response and response.invoices:
                    invoice_id = response.invoices[0].invoice_id
                    logger.info(f"Created invoice with ID: {invoice_id}")

                results.append({
                    'shipment': row['Shipment'],
                    'job_invoice': row['Job Invoice #'],
                    'status': 'success',
                    'type': row['Type'],
                    'invoice_id': invoice_id,
                    'amount': float(row.get('Total Invoice', 0)),
                    'date': row['Inv. Date']
                })

            except Exception as e:
                logger.error(f"Error processing row {idx + 2}: {str(e)}", exc_info=True)
                error_message = str(e)
                if isinstance(e, AccountingBadRequestException):
                    error_message = f"Xero API Error: {e.reason}"

                results.append({
                    'shipment': row['Shipment'],
                    'job_invoice': row['Job Invoice #'],
                    'status': 'error',
                    'type': row['Type'],
                    'error': error_message
                })
                continue

        return results

def main():
    try:
        # Required environment variables
        required_env_vars = [
            'XERO_CLIENT_ID',
            'XERO_CLIENT_SECRET',
            'XERO_ACCESS_TOKEN',
            'REFRESH_TOKEN',
            'XERO_TENANT_ID',
            'GOOGLE_CREDENTIALS',
            'GOOGLE_SHEETS_SPREADSHEET_ID'
        ]

        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            logger.error(f"Error: Missing environment variables: {', '.join(missing_vars)}")
            sys.exit(1)

        # Load environment variables
        client_id = os.getenv('XERO_CLIENT_ID')
        client_secret = os.getenv('XERO_CLIENT_SECRET')
        access_token = os.getenv('XERO_ACCESS_TOKEN')
        refresh_token = os.getenv('REFRESH_TOKEN')
        tenant_id = os.getenv('XERO_TENANT_ID')
        spreadsheet_id = os.getenv('GOOGLE_SHEETS_SPREADSHEET_ID')

        # Initialize OAuth2Token without token parameters
        oauth2_token = OAuth2Token(
            client_id=client_id,
            client_secret=client_secret
        )

        # Set the token values
        oauth2_token.set_token({
            'access_token': access_token,
            'refresh_token': refresh_token,
            'expires_at': time.time() - 3600  # Set to a time in the past to force refresh
        })

        # Configure Xero API client
        api_client = ApiClient(
            Configuration(oauth2_token=oauth2_token)
        )

        # Refresh Xero token
        try:
            new_token = api_client.refresh_oauth2_token()
            logger.info("Xero token refreshed successfully.")
            # Note: You cannot update GitHub Secrets during runtime.
            # If necessary, print the new refresh token (be cautious with sensitive data).
            # logger.info(f"New refresh token: {new_token['refresh_token']}")
        except Exception as e:
            logger.error(f"Failed to refresh Xero token: {e}", exc_info=True)
            sys.exit(1)

        accounting_api = AccountingApi(api_client)

        # Get contact ID
        try:
            contacts = accounting_api.get_contacts(tenant_id)
            if contacts.contacts:
                contact_id = contacts.contacts[0].contact_id
                logger.info(f"Using contact ID: {contact_id}")
            else:
                logger.error("No contacts found in Xero.")
                sys.exit(1)
        except Exception as e:
            logger.error(f"Error fetching contacts: {e}", exc_info=True)
            sys.exit(1)

        # Initialize processor
        processor = InvoiceProcessor(api_client, tenant_id)

        # Get spreadsheet data
        try:
            sheet_data = processor.get_sheet_data(spreadsheet_id)
            logger.info(f"Fetched {len(sheet_data)} rows from the spreadsheet.")
            if len(sheet_data) > 1:
                logger.debug(f"Sample data row: {sheet_data[1]}")
        except Exception as e:
            logger.error(f"Error fetching spreadsheet data: {e}", exc_info=True)
            sys.exit(1)

        # Process spreadsheet data
        try:
            df = processor.process_spreadsheet_data(sheet_data)
            logger.info(f"DataFrame columns: {df.columns.tolist()}")
        except Exception as e:
            logger.error(f"Error processing spreadsheet data: {e}", exc_info=True)
            sys.exit(1)

        # Validate required columns
        required_columns = ['Inv. Date', 'Type', 'Job Invoice #', 'Shipment']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {', '.join(missing_columns)}")
            sys.exit(1)

        # Process invoices
        results = processor.process_invoices(df, contact_id)

        # Output results
        successful = sum(1 for r in results if r['status'] == 'success')
        failed = sum(1 for r in results if r['status'] == 'error')
        total_amount = sum(float(r.get('amount', 0)) for r in results if r['status'] == 'success')

        logger.info(f"Processed {len(results)} invoices: {successful} successful, {failed} failed.")
        logger.info(f"Total amount invoiced: {total_amount:.2f}")

        # Optionally, output detailed results
        for result in results:
            if result['status'] == 'success':
                logger.info(f"Invoice {result['job_invoice']} created successfully.")
            else:
                logger.error(f"Failed to create invoice {result['job_invoice']}: {result['error']}")

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
