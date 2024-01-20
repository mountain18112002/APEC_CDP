from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import time
import pandas as pd
import schedule
import pytz
from datetime import datetime as dt
import gspread
from gspread_dataframe import set_with_dataframe
from processing_data import getdf  


# Google API authentication and file paths
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = "service_account.json"
# PARENT_FOLDER_ID = "1n1dGgRSYgxQfOZI36jzGYHw8ITzDyyHJ"
PARENT_FOLDER_ID = "1C2fYye72mY5pQeHaZtg4r-_S4to_fpHk"
STATUS_FILE = "./check_point/status.txt"

# Get dataframes from the processing_data module
report_sale_1, report_sale_2 = getdf()

# Authenticate Google Drive API
def authenticate_drive():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return creds

# Authenticate Google Sheets API
def authenticate_sheets():
    gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
    return gc

# Load status from a file containing information about uploaded dataframes
def load_status():
    status = {}
    if os.path.exists(STATUS_FILE):
        with open(STATUS_FILE, 'r') as txtfile:
            lines = txtfile.readlines()
            for line in lines:
                file_name, uploaded, timestamp = line.strip().split(',')
                status[file_name] = {'uploaded': uploaded, 'timestamp': dt.fromtimestamp(float(timestamp))}
    return status

# Save status to a file
def save_status(status):
    with open(STATUS_FILE, 'w') as txtfile:
        for file, data in status.items():
            date_str = data['timestamp'].astimezone(pytz.timezone('Asia/Ho_Chi_Minh')).strftime('%Y-%m-%d %H:%M:%S')
            txtfile.write(f"{file},{data['uploaded']},{date_str}\n")

# Generate a unique name for a dataframe based on the current date and a suffix
def generate_dataframe_name(suffix):
    current_date = dt.now().strftime('%Y%m%d')
    return f"report_sale_{current_date}{suffix}"

# Upload dataframes to Google Drive and update the status
def upload_data(dataframes, status):
    creds_drive = authenticate_drive()
    service_drive = build("drive", "v3", credentials=creds_drive)
    gc = authenticate_sheets()

    for suffix, dataframe in dataframes:
        name = generate_dataframe_name(suffix)

        if name not in status or status[name]['uploaded'] != 'uploaded':
            try:
                # Check if the file already exists in the parent folder
                query = f"'{PARENT_FOLDER_ID}' in parents and name='{name}' and mimeType='application/vnd.google-apps.spreadsheet'"
                response = service_drive.files().list(q=query, fields='files(id)').execute()
                files = response.get('files', [])

                if files:
                    # File already exists, open it
                    file_id = files[0]['id']
                    spreadsheet = gc.open_by_key(file_id)
                else:
                    # File doesn't exist, create a new one with the generated name
                    spreadsheet = gc.create(name, folder_id=PARENT_FOLDER_ID)

                worksheet = spreadsheet.sheet1
                set_with_dataframe(worksheet, dataframe)

                status[name] = {'uploaded': 'uploaded', 'timestamp': dt.now()}
                print(f"Dataframe '{name}' uploaded successfully.")
            except Exception as e:
                print(f"Error uploading dataframe '{name}': {str(e)}")

    save_status(status)

# Define a job that uploads dataframes and update the status
def job():
    dataframes = [("_1", report_sale_1), ("_2", report_sale_2)]
    status = load_status()
    if dataframes:
        upload_data(dataframes, status)
    print("#############################", "No Upload")

# job()

# Run 14:00 everyday
schedule.every().day.at("14:00", "Asia/Ho_Chi_Minh").do(job)
while True:
    schedule.run_pending()
    time.sleep(1)
