from app.my_utils import *
from app.data_manipulation import *
import schedule
import time

sheets_body = {
    'properties': {
        'title': f'report_sale_{Logger.TIME_TAG}',
    },
    'sheets': [
        {
            'properties': {
                'title': f'report_sale_{Logger.TIME_TAG}_1'
            }
        },
        {
            'properties': {
                'title': f'report_sale_{Logger.TIME_TAG}_2'
            }
        }
    ]
}

folder_id = '1VZRTiTKhmhuc4boDIIlOgeU9pSoo0AM5'
file_name = sheets_body.get('properties').get('title')


def get_data():
    # Read data from zip file
    file_list, df_list = read_csv_from_zip('app/data/report_sale.zip')
    location_list = [f.split('.')[0].split('/')[1] for f in file_list]
    report_sale_list = [[], []]

    # Clean data
    for df, location in zip(df_list, location_list):
        df = clean_data(df, location=location)
        for i, df in enumerate(split_data(df)):
            report_sale_list[i].append(df)

    report_sale_list = [pd.concat(df, ignore_index=True)
                        for df in report_sale_list]

    return report_sale_list


def update_data_to_sheet(sheets_service, spreadsheet_id, data):
    for i in range(len(data)):
        update_df_to_sheet(
            service=sheets_service,
            spreadsheet_id=spreadsheet_id,
            sheet_name=f'report_sale_{Logger.TIME_TAG}_{i+1}',
            df=data[i])


def schedule_job():
    sheets_service, drive_service = create_services()

    spreadsheet_id = check_spreadsheet_exists(
        drive_service, folder_id, file_name)

    if not spreadsheet_id:
        spreadsheet_id = create_spreadsheet(
            sheets_service, drive_service, sheets_body, folder_id)

    update_data_to_sheet(sheets_service, spreadsheet_id, get_data())


if __name__ == '__main__':
    # Schedule the job to run every day at a specific time (adjust as needed)
    schedule.every().day.at("04:00").do(schedule_job)

    # Keep the script running to allow the scheduled jobs to execute
    while True:
        schedule.run_pending()
        time.sleep(86400)  # wait one day
