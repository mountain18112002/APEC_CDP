from googleapiclient.discovery import build
from google.oauth2 import service_account
import gspread
from gspread_dataframe import set_with_dataframe
from app.clean_data import merge_dataframes
from datetime import datetime as dt

# Google API authentication and file paths
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = "./app/idyllic-web-411714-b56bff29d865.json"
PARENT_FOLDER_ID = "1LTJzVFcK6nGevcUTz1FS_x0EfYU185mX"

# Get dataframes from the processing_data module
report_sale_1, report_sale_2 = merge_dataframes()

# Authenticate Google Drive API using service account
creds_drive = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service_drive = build("drive", "v3", credentials=creds_drive)

# Authenticate Google Sheets API using service account
gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)

# Function to check if a file with the given name already exists
def file_exists_in_drive(file_name):
    query = f"'{PARENT_FOLDER_ID}' in parents and name='{file_name}' and mimeType='application/vnd.google-apps.spreadsheet'"
    response = service_drive.files().list(q=query, fields='files(id)').execute()
    files = response.get('files', [])
    return bool(files)

# Function to create a new Google Sheet and upload a dataframe
def create_and_upload_sheet(dataframe, sheet_name):
    try:
        # Check if the file already exists in the parent folder
        if file_exists_in_drive(sheet_name):
            print(f"File '{sheet_name}' already exists on Google Drive. Skipping creation.")
            return

        # File doesn't exist, create a new one with the given name
        spreadsheet = gc.create(sheet_name, folder_id=PARENT_FOLDER_ID)
        worksheet = spreadsheet.sheet1
        set_with_dataframe(worksheet, dataframe)
        print(f"Dataframe '{sheet_name}' uploaded successfully.")
    except Exception as e:
        print(f"Error uploading dataframe '{sheet_name}': {str(e)}")

# Function to automatically upload all dataframes
def upload_all_dataframes():
    dataframes = [report_sale_1, report_sale_2]
    for i, dataframe in enumerate(dataframes, start=1):
        current_date = dt.now().strftime('%y%m%d')
        sheet_name = f"ReportSale_{current_date}_{i}"
        create_and_upload_sheet(dataframe, sheet_name)


# import gspread
# from gspread_dataframe import set_with_dataframe
# from pydrive.auth import GoogleAuth
# from pydrive.drive import GoogleDrive
# import pandas as pd
# from app.clean_data import merge_dataframes

# SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# def create_google_sheet(sheet_name, drive_folder_id, drive_app):
#     existing_sheets = drive_app.ListFile({'q': f"'{drive_folder_id}' in parents and trashed=false"}).GetList()
#     for existing_sheet in existing_sheets:
#         if existing_sheet['title'] == sheet_name:
#             print(f"Sheet '{sheet_name}' already exists in Google Drive. Skipping creation.")
#             return None
    
#     file_metadata = {
#         'title': sheet_name,
#         'mimeType': 'application/vnd.google-apps.spreadsheet',
#         'parents': [{'id': drive_folder_id}]
#     }
#     new_file = drive_app.CreateFile(file_metadata)
#     new_file.Upload()

#     return new_file['id']

# def upload_dataframe_to_sheet(dataframe, spreadsheet_key, sheet_name, gauth):
#     credentials = gauth.credentials
#     gc = gspread.Client(auth=credentials)
#     spreadsheet = gc.open_by_key(spreadsheet_key)
#     try:
#         worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=1)
#     except gspread.exceptions.WorksheetNotFound:
#         worksheet = spreadsheet.worksheet(sheet_name)

#     set_with_dataframe(worksheet, dataframe)

# def job():
#     gauth = GoogleAuth()
#     credentials_path = r'./app/client_secrets.json'
#     gauth.LoadClientConfigFile(credentials_path)
#     gauth.LocalWebserverAuth()

#     drive_app = GoogleDrive(gauth)
#     DRIVE_FOLDER_ID = '130zldI5RZ6QFTihYR8hZyUBslP3fBYP7'
#     report_sale_180124_1, report_sale_180124_2 = merge_dataframes()

#     for i, dataframe in enumerate([report_sale_180124_1, report_sale_180124_2]):
#         sheet_name = f'ReportSale180124_{i + 1}' 
#         spreadsheet_key = create_google_sheet(sheet_name, DRIVE_FOLDER_ID, drive_app)
#         if spreadsheet_key is not None:
#             upload_dataframe_to_sheet(dataframe, spreadsheet_key, sheet_name, gauth)




