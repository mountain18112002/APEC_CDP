import gspread
from gspread_dataframe import set_with_dataframe
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pandas as pd
from app.clean_data import merge_dataframes

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

def create_google_sheet(sheet_name, drive_folder_id, drive_app):
    existing_sheets = drive_app.ListFile({'q': f"'{drive_folder_id}' in parents and trashed=false"}).GetList()
    for existing_sheet in existing_sheets:
        if existing_sheet['title'] == sheet_name:
            print(f"Sheet '{sheet_name}' already exists in Google Drive. Skipping creation.")
            return None
    
    file_metadata = {
        'title': sheet_name,
        'mimeType': 'application/vnd.google-apps.spreadsheet',
        'parents': [{'id': drive_folder_id}]
    }
    new_file = drive_app.CreateFile(file_metadata)
    new_file.Upload()

    return new_file['id']

def upload_dataframe_to_sheet(dataframe, spreadsheet_key, sheet_name, gauth):
    credentials = gauth.credentials
    gc = gspread.Client(auth=credentials)
    spreadsheet = gc.open_by_key(spreadsheet_key)
    try:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=1)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.worksheet(sheet_name)

    set_with_dataframe(worksheet, dataframe)

def job():
    gauth = GoogleAuth()
    credentials_path = r'./app/client_secrets.json'
    gauth.LoadClientConfigFile(credentials_path)
    gauth.LocalWebserverAuth()

    drive_app = GoogleDrive(gauth)
    DRIVE_FOLDER_ID = '130zldI5RZ6QFTihYR8hZyUBslP3fBYP7'
    report_sale_180124_1, report_sale_180124_2 = merge_dataframes()

    for i, dataframe in enumerate([report_sale_180124_1, report_sale_180124_2]):
        sheet_name = f'ReportSale180124_{i + 1}' 
        spreadsheet_key = create_google_sheet(sheet_name, DRIVE_FOLDER_ID, drive_app)
        if spreadsheet_key is not None:
            upload_dataframe_to_sheet(dataframe, spreadsheet_key, sheet_name, gauth)

def run():
    job()
