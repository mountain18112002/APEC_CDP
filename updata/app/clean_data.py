import pandas as pd
import numpy as np


def clean_data(data):
    data['booking_date'] = pd.to_datetime(data['booking_date'])
    data['phone'].fillna(0, inplace=True)

    data['chkin_time'] = data['chkin_time'].fillna(0)
    data['chkin_time'] = pd.to_datetime(data['chkin_time'], errors='coerce')

    data['chout_time'] = data['chkin_time'].fillna(0)
    data['chout_time'] = pd.to_datetime(data['chkin_time'], errors='coerce')

    data['room_id'].fillna(-1, inplace=True)
    data['room_id'] = data['room_id'].astype(int)
    data['created_date'] = pd.to_datetime(data['created_date'])
    data['phu_thu'].fillna(0, inplace=True)
    data['doanh_thu'].fillna(0, inplace=True)
    report_sale_180124_2 = data[data['booking_no'].isnull()]
    report_sale_180124_1 = data[data['booking_no'].notnull()]
    report_sale_180124_2 = report_sale_180124_2.drop('booking_no', axis=1)
    return report_sale_180124_1, report_sale_180124_2

def merge_dataframes():
    data_merge_1 = pd.DataFrame(columns=['booking_date', 'booking_no', 'Khu Vực', 'source_name', 'meal_plan_name', 
                                 'roomtype_name', 'last_name', 'phone', 'chkin_time', 'chout_time', 'room_id', 
                                 'created_date', 'phu_thu', 'doanh_thu'])

    data_merge_2 = pd.DataFrame(columns=['booking_date', 'booking_no', 'Khu Vực', 'source_name', 'meal_plan_name', 
                                 'roomtype_name', 'last_name', 'phone', 'chkin_time', 'chout_time', 'room_id', 
                                 'created_date', 'phu_thu', 'doanh_thu'])
    
    data_BG = pd.read_csv(r"./app/report_sale/BG.csv")
    data_BN = pd.read_csv(r"./app/report_sale/BN.csv")
    data_kimboi = pd.read_csv(r"./app/report_sale/Kim Boi.csv")
    data_muine = pd.read_csv(r"./app/report_sale/MuiNe.csv")
    data_phuyen = pd.read_csv(r"./app/report_sale/PhuYen.csv")

    data_BG.insert(2, 'Khu Vực', 'Bắc Giang')
    data_BN.insert(2, 'Khu Vực', 'Bắc Ninh')
    data_kimboi.insert(2, 'Khu Vực', 'Kim Bôi')
    data_muine.insert(2, 'Khu Vực', 'Mũi Né')
    data_phuyen.insert(2, 'Khu Vực', 'Phú Yên')
    
    report_sale_180124_1_BG, report_sale_180124_2_BG = clean_data(data_BG)
    report_sale_180124_1_BN, report_sale_180124_2_BN = clean_data(data_BN)
    report_sale_180124_1_kimboi, report_sale_180124_2_kimboi = clean_data(data_kimboi)
    report_sale_180124_1_muine, report_sale_180124_2_muine = clean_data(data_muine)
    report_sale_180124_1_phuyen, report_sale_180124_2_phuyen = clean_data(data_phuyen)

    data_merge_1 = pd.concat([report_sale_180124_1_BG, report_sale_180124_1_BN, report_sale_180124_1_kimboi,
                              report_sale_180124_1_muine, report_sale_180124_1_phuyen], ignore_index=True)
    data_merge_2 = pd.concat([report_sale_180124_2_BG, report_sale_180124_2_BN, report_sale_180124_2_kimboi, 
                              report_sale_180124_2_muine, report_sale_180124_2_phuyen], ignore_index=True)
    return data_merge_1, data_merge_2




