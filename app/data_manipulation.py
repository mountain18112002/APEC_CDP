import zipfile
import pandas as pd
from app.my_utils import Logger


def read_csv_from_zip(path_to_zip):
    """
    Reads CSV files from a zip file and returns a list of file names and a list of dataframes.

    Parameters:
        path_to_zip (str): The path to the zip file.

    Returns:
        tuple: A tuple containing a list of file names and a list of dataframes.
    """

    with zipfile.ZipFile(path_to_zip, 'r') as zip_ref:
        # get csv file name in zip
        file_list = [f for f in zip_ref.namelist()
                     if f.lower().endswith('csv')]

        # read file and append to a list
        df_list = []
        for file in file_list:
            with zip_ref.open(file) as csv_file:
                df_list.append(pd.read_csv(csv_file))
                print(f'Read from {file} with shape {df_list[-1].shape}.')

    return file_list, df_list


def show(df, n_rows=10):
    """
    Display the specified number of rows from a DataFrame.

    Parameters:
        df (DataFrame): The DataFrame to display.
        n_rows (int): The number of rows to display. Default is 10.

    Returns:
        None
    """
    pd.set_option('display.max_rows', n_rows)
    display(df)  # Display the DataFrame


def clean_data(df, location):
    """
    Clean the data by performing the following steps:
    1. Drop duplicate rows
    2. Add location column
    3. Rearrange columns order
    4. Fill NAs
    5. Casting datatype

    Args:
        df (pandas.DataFrame): The input DataFrame to be cleaned.
        location (str): The location value to be added as a new column.

    Returns:
        pandas.DataFrame: The cleaned DataFrame.
    """

    # 1. Drop duplicate rows
    df = df.drop_duplicates()

    # 2. Add location column
    df['location'] = location

    # 3. Rearrange columns order
    df = df[['booking_no', 'booking_date', 'location', 'chkin_time', 'chout_time',
             'created_date', 'last_name', 'phone', 'source_name', 'meal_plan_name',
             'roomtype_name', 'room_id', 'phu_thu', 'doanh_thu']]

    # 4. Fill NAs
    for col in ['phone', 'room_id', 'phu_thu', 'doanh_thu']:
        df[col] = df[col].fillna(0)
    for col in ['chkin_time', 'chout_time']:
        df[col] = df[col].fillna(df['created_date'])

    # 5. Casting datatype
    for col in ['booking_date', 'chkin_time', 'chout_time', 'created_date']:
        df[col] = pd.to_datetime(df[col])
    df['room_id'] = df['room_id'].astype(int)

    return df


def split_data(df):
    """
    Splits the input DataFrame into two separate DataFrames based on the presence of 'booking_no' values.

    Parameters:
        df (pandas.DataFrame): The input DataFrame to be split.

    Returns:
        list: A list containing two DataFrames. The first DataFrame contains rows where 'booking_no' is not null,
              and the second DataFrame contains rows where 'booking_no' is null.
    """
    df1 = df[~df['booking_no'].isnull()].reset_index().drop(columns=['index'])
    df2 = df[df['booking_no'].isnull()].reset_index().drop(columns=['index'])
    df2['booking_no'] = df2['booking_no'].fillna("nan")
    return [df1, df2]


def update_df_to_sheet(service, spreadsheet_id, sheet_name, df):
    """
    Updates a Google Sheet with the contents of a DataFrame.

    Args:
        service: The Google Sheets service object.
        spreadsheet_id (str): The ID of the spreadsheet.
        sheet_name (str): The name of the sheet to update.
        df (pandas.DataFrame): The DataFrame containing the data to update.

    Returns:
        The result of the update operation.
    """

    # Convert Timestamp objects to their string representation
    df = df.map(lambda x: x.isoformat()
                if isinstance(x, pd.Timestamp) else x)

    data = [df.columns.values.tolist()] + df.values.tolist()
    body = {'values': data}

    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=sheet_name,
        valueInputOption='RAW',
        body=body).execute()

    Logger.log(f"updated sheet: {sheet_name} with {df.shape[0]} rows.")
    return result
