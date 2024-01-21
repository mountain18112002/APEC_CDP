from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime
import os


class Logger:
    """
    A class for logging messages to a file.

    Attributes:
        TIME_TAG (str): Current date in the format "%d%m%y".
        LOG_DIR (str): Directory path for storing log files.
        LOG_FILE_NAME (str): Name of the log file.
        LOG_FILE_PATH (str): Full path of the log file.

    Methods:
        log(log_text): Logs the given text to the log file.
        reset(timetag): Resets the log file for the given timetag.
    """

    TIME_TAG = datetime.now().strftime("%d%m%y")
    LOG_DIR = './app/logs'
    LOG_FILE_NAME = f'LOG{TIME_TAG}.txt'
    LOG_FILE_PATH = os.path.join(LOG_DIR, LOG_FILE_NAME)

    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)

    def log(log_text):
        """
        Logs the given text to the log file.

        Args:
            log_text (str): The text to be logged.
        """
        print(f"LOG:  {log_text}")
        with open(Logger.LOG_FILE_PATH, 'a') as f:
            f.write(
                f"[{datetime.now().strftime('%H:%M:%S')}] {log_text}\n")

    def reset(timetag):
        """
        Resets the log file for the given timetag.

        Args:
            timetag (str): The timetag to reset the log file for.
        """
        reset_path = f'{Logger.LOG_DIR}/LOG{Logger.TIME_TAG}.txt'
        if os.path.exists(reset_path):
            os.remove(reset_path)


def create_service(client_secret_file, api_name, api_version, scopes, prefix=''):
    """
    Creates a service instance for the specified API.

    Args:
        client_secret_file (str): The path to the client secret file.
        api_name (str): The name of the API.
        api_version (str): The version of the API.
        scopes (list): The list of scopes required for the API.
        prefix (str, optional): A prefix to be added to the token file name. Defaults to ''.

    Returns:
        object: The service instance for the specified API, or None if creation fails.
    """

    CLIENT_SECRET_FILE = client_secret_file
    API_SERVICE_NAME = api_name
    API_VERSION = api_version
    SCOPES = scopes

    creds = None
    working_dir = os.getcwd()
    token_dir = 'app\\token_files'
    token_file = f'token_{API_SERVICE_NAME}_{API_VERSION}{prefix}.json'
    token_path = os.path.join(working_dir, token_dir, token_file)

    # Check if token dir exists first, if not, create the folder
    if not os.path.exists(os.path.join(working_dir, token_dir)):
        os.mkdir(os.path.join(working_dir, token_dir))

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(os.path.join(working_dir, token_dir, token_file), 'w') as token:
            token.write(creds.to_json())

    try:
        service = build(API_SERVICE_NAME, API_VERSION,
                        credentials=creds, static_discovery=False)
        Logger.log(
            f'{API_SERVICE_NAME} {API_VERSION} service created successfully.')
        return service
    except Exception as e:
        Logger.log(e)
        Logger.log(
            f'failed to create service instance for {API_SERVICE_NAME}.')
        os.remove(token_path)
        return None


def create_services():
    """
    Creates and returns Google Sheets and Google Drive services.

    Returns:
        sheets_service (googleapiclient.discovery.Resource): Google Sheets service.
        drive_service (googleapiclient.discovery.Resource): Google Drive service.
    """

    # Create Google Sheets service
    sheets_service = create_service(
        client_secret_file='client_secrets.json',
        api_name='sheets',
        api_version='v4',
        scopes=['https://www.googleapis.com/auth/spreadsheets'])

    # Create Google Drive service
    drive_service = create_service(
        client_secret_file='client_secrets.json',
        api_name='drive',
        api_version='v3',
        scopes=['https://www.googleapis.com/auth/drive'])

    return sheets_service, drive_service


def check_spreadsheet_exists(drive_service, folder_id, file_name):
    """
    Checks if a spreadsheet file with the given name exists in the specified folder.

    Args:
        drive_service (googleapiclient.discovery.Resource): The Google Drive service object.
        folder_id (str): The ID of the folder to search for the spreadsheet file.
        file_name (str): The name of the spreadsheet file to check.

    Returns:
        str: The ID of the existing spreadsheet file, or None if it doesn't exist.
    """

    q1 = "mimeType='application/vnd.google-apps.spreadsheet'"
    q2 = f"'{folder_id}' in parents"

    files = drive_service.files().list(
        q=f"{q1} and {q2} and trashed = false",
        fields='nextPageToken, files(id, name)',
        spaces='drive', pageToken=None).execute().get('files')

    spreadsheet_id = None
    if file_name in [file.get('name') for file in files]:
        Logger.log(f'file {file_name} already exists.')
        spreadsheet_id = [file.get('id') for file in files
                          if file.get('name') == file_name][0]

    return spreadsheet_id


def create_spreadsheet(sheets_service, drive_service, sheets_body, folder_id):
    """
    Creates a new spreadsheet using the Google Sheets API and moves it to a specified folder in Google Drive.

    Args:
        sheets_service (googleapiclient.discovery.Resource): The Google Sheets service object.
        drive_service (googleapiclient.discovery.Resource): The Google Drive service object.
        sheets_body (dict): The body of the request to create the spreadsheet.
        folder_id (str): The ID of the folder to move the spreadsheet to.

    Returns:
        str: The ID of the created spreadsheet.
    """

    sheets_file = sheets_service.spreadsheets().create(body=sheets_body).execute()
    spreadsheet_id = sheets_file.get('spreadsheetId')
    move_file_to_folder(
        service=drive_service,
        file_id=spreadsheet_id,
        folder_id=folder_id)
    Logger.log(f'created file: {sheets_file.get("spreadsheetId")}')
    return spreadsheet_id


def move_file_to_folder(service, file_id, folder_id):
    """
    Moves a file to a specified folder in Google Drive.

    Args:
        service (googleapiclient.discovery.Resource): The Google Drive service object.
        file_id (str): The ID of the file to be moved.
        folder_id (str): The ID of the folder to move the file to.

    Returns:
        dict: The updated file object after the move operation, or None if an error occurs.
    """

    # Retrieve the existing parents to remove
    try:
        file = service.files().get(fileId=file_id,
                                   fields='parents').execute()
        previous_parents = ",".join(file.get('parents'))
    except HttpError as e:
        Logger.log(e)
        return None

    # Move the file to the new folder
    try:
        file = service.files().update(fileId=file_id,
                                      addParents=folder_id,
                                      removeParents=previous_parents,
                                      fields='id, parents').execute()
        return file
    except HttpError as e:
        Logger.log(e)
        return None
