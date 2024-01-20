import pandas as pd

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
    df_bg = pd.read_csv("./report_sale/BG.csv")
    df_bn = pd.read_csv("./report_sale/BN.csv")
    df_kb = pd.read_csv("./report_sale/Kim Boi.csv")
    df_mn = pd.read_csv("./report_sale/MuiNe.csv")
    df_py = pd.read_csv("./report_sale/PhuYen.csv")
    df_bg["khu_vuc"] = "Bắc Giang"
    df_bn["khu_vuc"] = "Bắc Ninh"
    df_kb["khu_vuc"] = "Kim Bôi"
    df_mn["khu_vuc"] = "Mũi Né"
    df_py["khu_vuc"] = "Phú Yên"
    df_all = pd.concat([df_bg, df_bn, df_kb, df_mn, df_py], ignore_index=True)
    df_all = processing(df_all)
    df_booking, df_no_booking = split_dataframe(df_all)
    return df_booking, df_no_booking
    