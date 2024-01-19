from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload
import os
import time
import csv

SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = "service_account.json"
PARENT_FOLDER_ID = "1C2fYye72mY5pQeHaZtg4r-_S4to_fpHk"
# PARENT_FOLDER_ID = "1n1dGgRSYgxQfOZI36jzGYHw8ITzDyyHJ"

OUTPUT_FOLDER = "../pre_processing/out_put/"
STATUS_FILE = "./check_point/status.csv"

def get_data():
    files = os.listdir(OUTPUT_FOLDER)
    file_paths = [os.path.join(OUTPUT_FOLDER, file) for file in files]
    return file_paths

def authenticate():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return creds

def load_status():
    status = {}
    if os.path.exists(STATUS_FILE):
        with open(STATUS_FILE, 'r') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                status[row[0]] = row[1]
    return status

def save_status(status):
    with open(STATUS_FILE, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for file, uploaded in status.items():
            writer.writerow([file, uploaded])

def upload_data(file_paths, status):
    creds = authenticate()
    service = build("drive", "v3", credentials=creds)

    for file_path in file_paths:
        if file_path not in status or status[file_path] != 'uploaded':
            file_metadata = {
                "name": os.path.basename(file_path),
                "parents": [PARENT_FOLDER_ID]
            }

            media = MediaFileUpload(file_path, resumable=True)

            file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()

            status[file_path] = 'uploaded'
            print(f"File '{os.path.basename(file_path)}' uploaded successfully. File ID: {file.get('id')}")

    save_status(status)

def main():
    while True:
        file_paths = get_data()
        status = load_status()
        if file_paths:
            upload_data(file_paths, status)
        time.sleep(10) 

if __name__ == "__main__":
    main()
