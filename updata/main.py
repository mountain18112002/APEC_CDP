import sys
from app.app import upload_all_dataframes
import schedule
import time

if __name__ == "__main__":
    upload_all_dataframes()
    # schedule.every().day.at("08:00").do(upload_all_dataframes)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
