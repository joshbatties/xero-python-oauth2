import os
import json
import traceback
from datetime import datetime, timezone
from dateutil import parser
import pandas as pd
from typing import List, Dict, Any
from google.oauth2 import service_account
from googleapiclient.discovery import build
from xero_python.api_client import ApiClient, Configuration
from xero_python.api_client.oauth2 import OAuth2Token
from xero_python.accounting import AccountingApi, Invoices, Invoice, Contact, LineItem, LineAmountTypes
from xero_python.exceptions import AccountingBadRequestException

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
        print(f"Error creating sheets service: {str(e)}")
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
            print(f"Error creating invoice object: {str(e)}")
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
            raise Exception(f"Failed to fetch spreadsheet data: {str(e)}")

    def process_spreadsheet_data(self, sheet_data: List[List[str]]) -> pd.DataFrame:
        try:
            if not sheet_data:
                raise ValueError("No data found in spreadsheet")

            df = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

            # Convert charge columns to float
            for charge_code in self.charge_descriptions.keys():
                df[charge_code] = pd.to_numeric(df[charge_code], errors='coerce').fillna(0)

            return df
        except Exception as e:
            raise Exception(f"Error processing spreadsheet data: {str(e)}")

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
                print(f"Processing row {idx + 2}: Date={row['Inv. Date']}, Job Invoice={row['Job Invoice #']}")

                invoice = self.create_invoice(row, contact_id)

                response = self.accounting_api.create_invoices(
                    self.tenant_id,
                    invoices=Invoices(invoices=[invoice]),
                    summarize_errors=False
                )

                invoice_id = None
                if response and response.invoices:
                    invoice_id = response.invoices[0].invoice_id
                    print(f"Created invoice with ID: {invoice_id}")

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
                print(f"Error processing row {idx + 2}: {str(e)}")
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
        # Load environment variables
        client_id = os.getenv('XERO_CLIENT_ID')
        client_secret = os.getenv('XERO_CLIENT_SECRET')
        access_token = os.getenv('XERO_ACCESS_TOKEN')
        refresh_token = os.getenv('REFRESH_TOKEN')
        tenant_id = os.getenv('XERO_TENANT_ID')
        spreadsheet_id = os.getenv('GOOGLE_SHEETS_SPREADSHEET_ID')

        if not all([client_id, client_secret, access_token, refresh_token, tenant_id, spreadsheet_id]):
            print("One or more environment variables are missing.")
            return

        # Configure Xero API client
        oauth2_token = OAuth2Token(
            client_id=client_id,
            client_secret=client_secret,
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=0  # Force token refresh
        )

        api_client = ApiClient(
            Configuration(oauth2_token=oauth2_token)
        )

        # Refresh Xero token
        try:
            new_token = api_client.refresh_oauth2_token()
            print("Xero token refreshed successfully.")
        except Exception as e:
            print(f"Failed to refresh Xero token: {e}")
            return

        accounting_api = AccountingApi(api_client)

        # Get contact ID
        try:
            contacts = accounting_api.get_contacts(tenant_id)
            if contacts.contacts:
                contact_id = contacts.contacts[0].contact_id
                print(f"Using contact ID: {contact_id}")
            else:
                print("No contacts found in Xero.")
                return
        except Exception as e:
            print(f"Error fetching contacts: {e}")
            return

        # Initialize processor
        processor = InvoiceProcessor(api_client, tenant_id)

        # Get spreadsheet data
        try:
            sheet_data = processor.get_sheet_data(spreadsheet_id)
            print(f"Fetched {len(sheet_data)} rows from the spreadsheet.")
        except Exception as e:
            print(f"Error fetching spreadsheet data: {e}")
            return

        # Process spreadsheet data
        try:
            df = processor.process_spreadsheet_data(sheet_data)
            print(f"Processed spreadsheet data into DataFrame with {len(df)} rows.")
        except Exception as e:
            print(f"Error processing spreadsheet data: {e}")
            return

        # Validate required columns
        required_columns = ['Inv. Date', 'Type', 'Job Invoice #', 'Shipment']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Missing required columns: {', '.join(missing_columns)}")
            return

        # Process invoices
        results = processor.process_invoices(df, contact_id)

        # Output results
        successful = sum(1 for r in results if r['status'] == 'success')
        failed = sum(1 for r in results if r['status'] == 'error')
        total_amount = sum(float(r.get('amount', 0)) for r in results if r['status'] == 'success')

        print(f"Processed {len(results)} invoices: {successful} successful, {failed} failed.")
        print(f"Total amount invoiced: {total_amount:.2f}")

        # Optionally, you can write results to a log file or send notifications

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        traceback.print_exc()

if __name__ == '__main__':
    main()
