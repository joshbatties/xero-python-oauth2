# -*- coding: utf-8 -*-
import os
from functools import wraps
from io import BytesIO
from logging.config import dictConfig
from dateutil import parser
from datetime import datetime, timezone
import traceback
from flask import jsonify
from flask import Flask, url_for, render_template, session, redirect, json, send_file
from flask_oauthlib.contrib.client import OAuth, OAuth2Application
from flask_session import Session
from xero_python.accounting import AccountingApi, ContactPerson, Contact, Contacts, Invoice, Invoices, LineItem
from xero_python.api_client import ApiClient, serialize
from xero_python.api_client.configuration import Configuration
from xero_python.api_client.oauth2 import OAuth2Token
from xero_python.exceptions import AccountingBadRequestException, ApiException
from xero_python.identity import IdentityApi
from xero_python.utils import getvalue
from xero_python.accounting import (
    AccountingApi, 
    ContactPerson, 
    Contact, 
    Contacts, 
    Invoice, 
    Invoices, 
    LineItem,
    LineAmountTypes
)
import logging_settings
from utils import jsonify, serialize_model

dictConfig(logging_settings.default_settings)

# configure main flask application
app = Flask(__name__)

# Configure session before initializing Flask-Session
app.secret_key = os.urandom(24)  # Required for session security
app.config.update(
    SESSION_TYPE='filesystem',
    SESSION_FILE_DIR=os.path.join(os.getcwd(), 'flask_session'),
    SESSION_PERMANENT=False,
    PERMANENT_SESSION_LIFETIME=3600,  # Session lifetime in seconds (1 hour)
    SESSION_FILE_THRESHOLD=500,  # Maximum number of sessions stored in filesystem
    SESSION_COOKIE_NAME='xero_flask_session'  # Explicitly set session cookie name
)

# Make sure session directory exists
os.makedirs(app.config['SESSION_FILE_DIR'], exist_ok=True)

# Load other configurations
app.config.from_object("default_settings")
app.config.from_pyfile("config.py", silent=True)

if app.config["ENV"] != "production":
    # allow oauth2 loop to run over http (used for local testing only)
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

# Initialize Flask-Session after all configurations
Session(app)

# configure flask-oauthlib application
oauth = OAuth(app)
xero = oauth.remote_app(
    name="xero",
    version="2",
    client_id=app.config["CLIENT_ID"],
    client_secret=app.config["CLIENT_SECRET"],
    endpoint_url="https://api.xero.com/",
    authorization_url="https://login.xero.com/identity/connect/authorize",
    access_token_url="https://identity.xero.com/connect/token",
    refresh_token_url="https://identity.xero.com/connect/token",
    scope="offline_access openid profile email accounting.transactions "
    "accounting.journals.read accounting.transactions payroll.payruns accounting.reports.read "
    "files accounting.settings.read accounting.settings accounting.attachments payroll.payslip payroll.settings files.read openid assets.read profile payroll.employees projects.read email accounting.contacts.read accounting.attachments.read projects assets accounting.contacts payroll.timesheets accounting.budgets.read",
)

# configure xero-python sdk client
api_client = ApiClient(
    Configuration(
        debug=app.config["DEBUG"],
        oauth2_token=OAuth2Token(
            client_id=app.config["CLIENT_ID"], client_secret=app.config["CLIENT_SECRET"]
        ),
    ),
    pool_threads=1,
)

class TokenRefreshError(Exception):
    """Custom exception for token refresh failures"""
    pass

def is_token_expired(token):
    """Check if the token is expired or will expire soon"""
    if not token or 'expires_at' not in token:
        return True
    
    expires_at = datetime.fromtimestamp(token['expires_at'], timezone.utc)
    now = datetime.now(timezone.utc)
    return (expires_at - now).total_seconds() < 30

# Token management functions
@xero.tokengetter
@api_client.oauth2_token_getter
def obtain_xero_oauth2_token():
    return session.get("token")

@xero.tokensaver
@api_client.oauth2_token_saver
def store_xero_oauth2_token(token):
    session["token"] = token
    session.modified = True

def refresh_token_if_expired_decorator(f):
    """Decorator to handle automatic token refresh"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        max_retries = 1
        retries = 0
        
        while retries <= max_retries:
            try:
                token = obtain_xero_oauth2_token()
                
                if is_token_expired(token):
                    try:
                        new_token = api_client.refresh_oauth2_token()
                        store_xero_oauth2_token(new_token)
                    except Exception as e:
                        raise TokenRefreshError(f"Failed to refresh token: {str(e)}")
                
                return f(*args, **kwargs)
                
            except ApiException as e:
                if hasattr(e, 'status') and e.status == 401 and retries < max_retries:
                    try:
                        new_token = api_client.refresh_oauth2_token()
                        store_xero_oauth2_token(new_token)
                        retries += 1
                        continue
                    except Exception as refresh_error:
                        raise TokenRefreshError(f"Failed to refresh token after 401: {str(refresh_error)}")
                raise
            
            except TokenRefreshError:
                return redirect(url_for("login", _external=True))
                
        return f(*args, **kwargs)
    return decorated_function

def xero_token_required(function):
    @wraps(function)
    @refresh_token_if_expired_decorator
    def decorator(*args, **kwargs):
        xero_token = obtain_xero_oauth2_token()
        if not xero_token:
            return redirect(url_for("login", _external=True))
        return function(*args, **kwargs)
    return decorator

# Helper function
def get_xero_tenant_id():
    token = obtain_xero_oauth2_token()
    if not token:
        return None

    identity_api = IdentityApi(api_client)
    for connection in identity_api.get_connections():
        if connection.tenant_type == "ORGANISATION":
            return connection.tenant_id

# Routes
@app.route("/")
def index():
    xero_access = dict(obtain_xero_oauth2_token() or {})
    return render_template(
        "code.html",
        title="Home | oauth token",
        code=json.dumps(xero_access, sort_keys=True, indent=4),
    )

@app.route("/tenants")
@xero_token_required
def tenants():
    identity_api = IdentityApi(api_client)
    accounting_api = AccountingApi(api_client)

    available_tenants = []
    for connection in identity_api.get_connections():
        tenant = serialize(connection)
        if connection.tenant_type == "ORGANISATION":
            organisations = accounting_api.get_organisations(
                xero_tenant_id=connection.tenant_id
            )
            tenant["organisations"] = serialize(organisations)
        available_tenants.append(tenant)

    return render_template(
        "code.html",
        title="Xero Tenants",
        code=json.dumps(available_tenants, sort_keys=True, indent=4),
    )

@app.route("/create-contact-person")
@xero_token_required
def create_contact_person():
    xero_tenant_id = get_xero_tenant_id()
    accounting_api = AccountingApi(api_client)

    contact_person = ContactPerson(
        first_name="John",
        last_name="Smith",
        email_address="john.smith@24locks.com",
        include_in_emails=True,
    )
    contact = Contact(
        name="FooBar123",
        first_name="Foo",
        last_name="Bar",
        email_address="ben.bowden@24locks.com",
        contact_persons=[contact_person],
    )
    contacts = Contacts(contacts=[contact])
    try:
        created_contacts = accounting_api.create_contacts(
            xero_tenant_id, contacts=contacts
        )
    except AccountingBadRequestException as exception:
        sub_title = "Error: " + exception.reason
        code = jsonify(exception.error_data)
    else:
        sub_title = "Contact {} created.".format(
            getvalue(created_contacts, "contacts.0.name", "")
        )
        code = serialize_model(created_contacts)

    return render_template(
        "code.html", title="Create Contacts", code=code, sub_title=sub_title
    )

@app.route("/create-multiple-contacts")
@xero_token_required
def create_multiple_contacts():
    xero_tenant_id = get_xero_tenant_id()
    accounting_api = AccountingApi(api_client)

    contact = Contact(
        name="George Jetson 123",
        first_name="George",
        last_name="Jetson",
        email_address="george.jetson@aol.com",
    )
    contacts = Contacts(contacts=[contact, contact])
    try:
        created_contacts = accounting_api.create_contacts(
            xero_tenant_id, contacts=contacts, summarize_errors=False
        )
    except AccountingBadRequestException as exception:
        sub_title = "Error: " + exception.reason
        result_list = None
        code = jsonify(exception.error_data)
    else:
        sub_title = ""
        result_list = []
        for contact in created_contacts.contacts:
            if contact.has_validation_errors:
                error = getvalue(contact.validation_errors, "0.message", "")
                result_list.append("Error: {}".format(error))
            else:
                result_list.append("Contact {} created.".format(contact.name))
        code = serialize_model(created_contacts)

    return render_template(
        "code.html",
        title="Create Multiple Contacts",
        code=code,
        result_list=result_list,
        sub_title=sub_title,
    )

@app.route("/invoices")
@xero_token_required
def get_invoices():
    xero_tenant_id = get_xero_tenant_id()
    accounting_api = AccountingApi(api_client)

    invoices = accounting_api.get_invoices(
        xero_tenant_id, statuses=["DRAFT", "SUBMITTED"]
    )
    code = serialize_model(invoices)
    sub_title = "Total invoices found: {}".format(len(invoices.invoices))

    return render_template(
        "code.html", title="Invoices", code=code, sub_title=sub_title
    )

@app.route('/create_invoice')
@xero_token_required
def create_invoice():
    accounting_api = AccountingApi(api_client)
    xero_tenant_id = get_xero_tenant_id()

    try:
        contacts = accounting_api.get_contacts(xero_tenant_id)
        if contacts.contacts:
            contact_id = contacts.contacts[0].contact_id
        else:
            return "No contacts found"
    except Exception as e:
        return f"Error fetching contacts: {e}"

    date_value = parser.parse('2024-10-24T00:00:00Z')
    due_date_value = parser.parse('2024-11-24T00:00:00Z')
    line_item = LineItem(description="Service", quantity=1.0, unit_amount=100.0)
    invoice = Invoice(
        type="ACCREC",
        contact=Contact(contact_id=contact_id),
        line_items=[line_item],
        date=date_value,
        due_date=due_date_value,
        reference="Invoice Reference",
        status="DRAFT"
    )

    try:
        response = accounting_api.create_invoices(
            xero_tenant_id,
            invoices=Invoices(invoices=[invoice]),
            summarize_errors=True,
        )
        return str(response)
    except Exception as e:
        return f"Error creating invoice: {e}"

@app.route("/login")
def login():
    redirect_url = url_for("oauth_callback", _external=True)
    response = xero.authorize(callback_uri=redirect_url)
    return response

@app.route("/callback")
def oauth_callback():
    try:
        response = xero.authorized_response()
    except Exception as e:
        print(e)
        raise
    if response is None or response.get("access_token") is None:
        return "Access denied: response=%s" % response
    store_xero_oauth2_token(response)
    return redirect(url_for("index", _external=True))

@app.route("/logout")
def logout():
    store_xero_oauth2_token(None)
    return redirect(url_for("index", _external=True))

@app.route("/export-token")
@xero_token_required
def export_token():
    token = obtain_xero_oauth2_token()
    buffer = BytesIO("token={!r}".format(token).encode("utf-8"))
    buffer.seek(0)
    return send_file(
        buffer,
        mimetype="x.python",
        as_attachment=True,
        attachment_filename="oauth2_token.py",
    )

@app.route("/refresh-token")
@xero_token_required
def refresh_token():
    xero_token = obtain_xero_oauth2_token()
    new_token = api_client.refresh_oauth2_token()
    return render_template(
        "code.html",
        title="Xero OAuth2 token",
        code=jsonify({"Old Token": xero_token, "New token": new_token}),
        sub_title="token refreshed",
    )

import os
from datetime import datetime
import pandas as pd
from typing import List, Dict, Any
from google.oauth2 import service_account
from googleapiclient.discovery import build
from xero_python.accounting import Invoice, Contact, LineItem, Invoices, AccountingApi
from xero_python.exceptions import AccountingBadRequestException

def create_sheets_service():
    """Create and return a Google Sheets service instance"""
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    try:
        # Get the absolute path to the credentials file
        credentials_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 
            'credentials.json'  # Place credentials.json in same directory as your app
        )
        
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path, scopes=SCOPES
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

    def date_to_ms_timestamp(self, date_str: str) -> int:
        """Convert MM/DD/YYYY date string to milliseconds timestamp"""
        month, day, year = map(int, date_str.split('/'))
        dt = datetime(year, month, day, tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

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
                    account_code="200",
                    tax_type="NONE",
                    line_amount=amount
                )
                line_items.append(line_item)
        return line_items


    def process_invoices(self, df: pd.DataFrame, contact_id: str) -> List[Dict[str, Any]]:
        results = []
        
        for _, row in df.iterrows():
            try:
                # Debug output
                print(f"Processing row: Date={row['Inv. Date']}, Job Invoice={row['Job Invoice #']}")
                
                invoice = self.create_invoice(row, contact_id)
                
                response = self.accounting_api.create_invoices(
                    self.tenant_id,
                    invoices=Invoices(invoices=[invoice]),
                    summarize_errors=True
                )

                results.append({
                    'shipment': row['Shipment'],
                    'job_invoice': row['Job Invoice #'],
                    'status': 'success',
                    'type': row['Type'],
                    'invoice_id': response.invoices[0].invoice_id if response.invoices else None,
                    'amount': float(row.get('Total Invoice', 0)),
                    'date': row['Inv. Date']
                })

            except Exception as e:
                print(f"Error processing invoice: {str(e)}")  # Debug output
                results.append({
                    'shipment': row['Shipment'],
                    'job_invoice': row['Job Invoice #'],
                    'status': 'error',
                    'type': row['Type'],
                    'error': str(e)
                })
                continue

        return results

@app.route('/create_invoices_from_sheet')
@xero_token_required
def create_invoices_from_sheet():
    try:
        xero_tenant_id = get_xero_tenant_id()
        if not xero_tenant_id:
            return jsonify({'status': 'error', 'message': 'No Xero tenant found'}), 400

        # Get the first contact for testing
        contacts = AccountingApi(api_client).get_contacts(xero_tenant_id)
        if not contacts.contacts:
            return jsonify({'status': 'error', 'message': 'No contacts found in Xero'}), 400
        
        contact_id = contacts.contacts[0].contact_id
        print(f"Using contact ID: {contact_id}")

        # Initialize processor
        processor = InvoiceProcessor(api_client, xero_tenant_id)
        
        try:
            spreadsheet_id = os.getenv('GOOGLE_SHEETS_SPREADSHEET_ID')
            if not spreadsheet_id:
                return jsonify({
                    'status': 'error',
                    'message': 'Spreadsheet ID not configured'
                }), 500
            
            sheet_data = processor.get_sheet_data(spreadsheet_id)
            if sheet_data:
                print(f"Found {len(sheet_data)} rows in spreadsheet")
            
        except Exception as e:
            return jsonify({
                'status': 'error',
                'message': f'Google Sheets error: {str(e)}'
            }), 500

        try:
            df = processor.process_spreadsheet_data(sheet_data)
            print(f"Processed DataFrame columns: {df.columns.tolist()}")
            
            # Validate required columns
            required_columns = ['Inv. Date', 'Type', 'Job Invoice #', 'Shipment']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                return jsonify({
                    'status': 'error',
                    'message': f'Missing required columns: {", ".join(missing_columns)}'
                }), 400

            if not df.empty:
                sample_row = df.iloc[0]
                print(f"Sample row - Date: {sample_row['Inv. Date']}, Type: {sample_row['Type']}")

            results = []
            for idx, row in df.iterrows():
                try:
                    # Create invoice and get response
                    invoice = processor.create_invoice(row, contact_id)
                    
                    # Create the invoice in Xero
                    response = processor.accounting_api.create_invoices(
                        xero_tenant_id,
                        invoices=Invoices(invoices=[invoice]),
                        summarize_errors=False
                    )
                    
                    # Get the invoice ID from the response
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

            # Calculate statistics
            successful = sum(1 for r in results if r['status'] == 'success')
            failed = sum(1 for r in results if r['status'] == 'error')
            total_amount = sum(float(r.get('amount', 0)) for r in results if r['status'] == 'success')

            return jsonify({
                'status': 'success',
                'message': f'Processed {len(results)} invoices',
                'summary': {
                    'total_processed': len(results),
                    'successful': successful,
                    'failed': failed,
                    'total_amount': round(total_amount, 2)
                },
                'results': results,
                'debug': {
                    'column_names': df.columns.tolist(),
                    'sample_row': df.iloc[0].to_dict() if not df.empty else None,
                }
            })

        except Exception as e:
            print(f"Error processing data: {str(e)}")
            print(traceback.format_exc())
            return jsonify({
                'status': 'error',
                'message': f'Error processing data: {str(e)}',
                'traceback': traceback.format_exc()
            }), 500

    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        print(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': str(e),
            'traceback': traceback.format_exc()
        }), 500
    
if __name__ == '__main__':
    app.run(host='localhost', port=5000)