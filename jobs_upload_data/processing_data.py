import pandas as pd
import datetime
import os

def processing(df):
    # convert to datetime
    columns_to_convert = ["booking_date", "chkin_time", "chout_time", "created_date"]
    df[columns_to_convert] = df[columns_to_convert].apply(pd.to_datetime)
    
    #remove duplicated
    df_no_duplicates = df.drop_duplicates(subset=df.columns.difference(['created_date']), keep='last')
    
    # processing null 
    columns_to_fill = ['phone', 'phu_thu', 'doanh_thu', "chkin_time", "chout_time"]
    df_no_duplicates[columns_to_fill] = df_no_duplicates[columns_to_fill].fillna(0)
    
    df_no_duplicates["room_id"] = df_no_duplicates["room_id"].fillna(9999999)
    df_no_duplicates["room_id"] = df_no_duplicates["room_id"].astype('int64')
    return df_no_duplicates

def split_dataframe(df):
    dat_not_booking = df[df['booking_no'].isna()]
    dat_not_booking.drop('booking_no', axis=1, inplace=True)
    dat_booking = df[df['booking_no'].notna()]
    return dat_booking, dat_not_booking

def getdf():
    today = datetime.date.today().strftime('%Y%m%d')
    folder_path = f'./report_sale/{today}'
    dfs = []

    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            
            df = pd.read_csv(file_path)
            
            file_name_without_extension = os.path.splitext(file_name)[0]
            
            df['location'] = file_name_without_extension
            
            dfs.append(df)

    combined_data = pd.concat(dfs, ignore_index=True)
    
    df_all = processing(combined_data)
    df_booking, df_no_booking = split_dataframe(df_all)
    return df_booking, df_no_booking