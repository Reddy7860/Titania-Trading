import pandas as pd
import numpy as np
from datetime import datetime
import smartapi
import yfinance as yf
from smartapi import SmartConnect
import json
import requests
import datetime
from datetime import datetime, timedelta
import time
import os.path
from pytz import timezone
import os
import math
from pandasql import sqldf
import pandasql as pdsql
import warnings
from datetime import timedelta
import pandas_ta as ta


from pandas.io import sql
from pandasql import sqldf
import mysql.connector as mysql
import pymysql

import pyotp
# import MySQLdb
# import pymysql



from sqlalchemy.engine import result
import sqlalchemy
from sqlalchemy import create_engine, MetaData,\
Table, Column, Numeric, Integer, VARCHAR, update, delete


from sqlalchemy import create_engine

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings('ignore')


start_time = datetime.now(timezone("Asia/Kolkata"))
print("Script execution started")
print(start_time)

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

today_now = datetime.now(timezone("Asia/Kolkata"))

expiry_date = today_now

if today_now.strftime("%w") == '1':
    expiry_date = today_now + timedelta(days=3)
elif today_now.strftime("%w") == '2':
    expiry_date = today_now + timedelta(days=9)
elif today_now.strftime("%w") == '3':
    expiry_date = today_now + timedelta(days=8)
elif today_now.strftime("%w") == '4':
    expiry_date = today_now + timedelta(days=7)
elif today_now.strftime("%w") == '5':
    expiry_date = today_now + timedelta(days=6)
elif today_now.strftime("%w") == '6':
    expiry_date = today_now + timedelta(days=5)
elif today_now.strftime("%w") == '7':
    expiry_date = today_now + timedelta(days=4)

print("Expiry date")
print(expiry_date)
# expiry_date = '13-04-2022 15:00:00'
# expiry_date = datetime.strptime(expiry_date, '%d-%m-%Y %H:%M:%S')

expiry_date_char = expiry_date.strftime("%Y-%m-%d")
expiry_date_month = expiry_date.strftime("%d%b%y").upper()


def sweths_violation(stock,data):
    print("running : sweths_violation")
    now = datetime.now(timezone("Asia/Kolkata"))
#     print(now)
    current_time = now.strftime("%H:%M:%S")
    global increment

    final_data = data[data['Datetime'] >= now.strftime("%Y-%m-%d")]
    
    final_data.reset_index(level=0, inplace=True,drop = True)
    
    
    if(current_time >= "09:40:00"):    
        
        trigger_price = 0
        stage = ""
        # print(final_data)
        if((final_data.loc[0,"Close"] > final_data.loc[0,"Open"]) and abs(final_data.loc[0,"Close"] - final_data.loc[0,"Open"]) >= 0.7*abs(final_data.loc[0,"High"] - final_data.loc[0,"Low"])):
            trigger_price = final_data.loc[0,"Low"]
            stage = "Green"
        elif((final_data.loc[0,"Close"] < final_data.loc[0,"Open"]) and abs(final_data.loc[0,"Close"] - final_data.loc[0,"Open"]) >= 0.7*abs(final_data.loc[0,"High"] - final_data.loc[0,"Low"])):
            trigger_price = final_data.loc[0,"High"]
            stage = "Red"
        else:
            next
        satisfied_df = pd.DataFrame(columns=['Datetime', 'Open', 'High','Low', 'Close', 'Adj Close','Volume','date','Call'])

        # print(satisfied_df)
        for j in range(4,len(final_data)):
            if stage == "Green":
                if final_data.loc[j,"Close"] < trigger_price:
                    temp_call_data = final_data.loc[j,]
                    temp_call_data = temp_call_data.append(pd.Series("Sell",index=["Call"]))
                    satisfied_df = satisfied_df.append(temp_call_data, ignore_index = True)
                    call = "Sell"
            elif stage == "Red":
                if final_data.loc[j,"Close"] > trigger_price:
                    temp_call_data = final_data.loc[j,]
                    temp_call_data = temp_call_data.append(pd.Series("Buy",index=["Call"]))
                    satisfied_df = satisfied_df.append(temp_call_data, ignore_index = True)
                    call = "Buy"
            else:
                next

        # print(satisfied_df)
        if not satisfied_df.empty:
            satisfied_df = satisfied_df.head(1)

            satisfied_df.reset_index(inplace = True, drop = True)

            ind_time = datetime.now(timezone("Asia/Kolkata"))
            time_delta = ind_time - satisfied_df.loc[0,"Datetime"]
            time_delta_mins = time_delta.total_seconds()/60


            Signal_df.loc[increment,"Strategy"] = "Sweths Violation"
            Signal_df.loc[increment,"Stock"] = stock
            Signal_df.loc[increment,"Signal"] = satisfied_df.loc[0,"Call"]
            Signal_df.loc[increment,"Datetime"] = satisfied_df.loc[0,"Datetime"]
            Signal_df.loc[increment,"Value"] = satisfied_df.loc[0,"Close"]

#             Signal_df.loc[increment,"SMA_Call"] = technical_data.loc[0,"SMA_Call"]
#             Signal_df.loc[increment,"RSI_Call"] = technical_data.loc[0,"RSI_Call"]
#             Signal_df.loc[increment,"MACD_Call"] = technical_data.loc[0,"MACD_Call"]
#             Signal_df.loc[increment,"Pivot_Call"] = technical_data.loc[0,"Pivot_Call"]
#             Signal_df.loc[increment,"BB_Call"] = technical_data.loc[0,"BB_Call"]
#             Signal_df.loc[increment,"VWAP_Call"] = technical_data.loc[0,"VWAP_Call"]
#             Signal_df.loc[increment,"SuperTrend_Call"] = technical_data.loc[0,"SuperTrend_Call"]
#             Signal_df.loc[increment,"PCR_Call"] = pcr_call

            increment = increment+1
                
    else:
        print("Strategy not live due to time")


def cowboy(stock,data):
    print("running : cowboy")
    now = datetime.now(timezone("Asia/Kolkata"))
    
    final_levels_df = pd.read_csv("~/Downloads/Reddy_Stocks_Application/data/cowboy_data.csv",index_col=False)
#     final_levels_df = pd.read_csv("~/Downloads/Reddy_Stocks_Application/data/cowboy_data.csv",index_col=False)
    
    global increment
    
#     print(final_levels_df)
    # for idx in range(0,len(nse_data)):
        
    satisfied_df = pd.DataFrame(columns=['Datetime', 'Open', 'High','Low', 'Close', 'Adj Close','Volume','date','Call'])

    data = data[data['Datetime'] >= now.strftime("%Y-%m-%d")]
    
    
    data.reset_index(level=0, inplace=True,drop = True)
    
    final_data = data
    
#     print(final_data)
    
    sub_df = final_levels_df.loc[(final_levels_df['Stock'] == stock)]
    sub_df.reset_index(inplace = True, drop = True)
    
    if len(sub_df) > 0:
#         print(final_data)
        # print(sub_df)
        # print(sub_df.loc[0,"Rider_Bullish"])
        if sub_df.loc[0,"Rider_Bullish"] == "Yes":
            satisfied_df = pd.DataFrame(columns=['Datetime', 'Open', 'High','Low', 'Close', 'Adj Close','Volume','date','Call'])
            
            for j in range(0,len(final_data)):
                if final_data.loc[j,"Close"] > sub_df.loc[0,"Bullish_Level"]:
                    temp_call_data = final_data.loc[j,]
                    temp_call_data = temp_call_data.append(pd.Series("Buy",index=["Call"]))
                    satisfied_df = satisfied_df.append(temp_call_data, ignore_index = True)
                else:
                    next
        elif sub_df.loc[0,"Rider_Bearish"] == "Yes":
            
            
            
            for j in range(0,len(final_data)):
                if final_data.loc[j,"Close"] < sub_df.loc[0,"Bearish_Level"]:
                    temp_call_data = final_data.loc[j,]
                    temp_call_data = temp_call_data.append(pd.Series("Sell",index=["Call"]))
                    satisfied_df = satisfied_df.append(temp_call_data, ignore_index = True)
                else:
                    next
    
    
    
    if not satisfied_df.empty:
        satisfied_df = satisfied_df.head(1)

        satisfied_df['Datetime'] = pd.to_datetime(satisfied_df['Datetime'], infer_datetime_format=True, utc=True )
        satisfied_df['Datetime'] = satisfied_df['Datetime'].dt.tz_convert('Asia/Kolkata')
            
        satisfied_df.reset_index(inplace = True, drop = True)

        ind_time = datetime.now(timezone("Asia/Kolkata"))
#         time_delta = ind_time - satisfied_df.loc[0,"Datetime"]
#         time_delta_mins = time_delta.total_seconds()/60

#         pcr_call = technical_data.loc[0,"PCR_Call"]

        

        
            
        Signal_df.loc[increment,"Strategy"] = "Cowboy"
        Signal_df.loc[increment,"Stock"] = stock
        Signal_df.loc[increment,"Signal"] = satisfied_df.loc[0,"Call"]
        Signal_df.loc[increment,"Datetime"] = satisfied_df.loc[0,"Datetime"]
        Signal_df.loc[increment,"Value"] = satisfied_df.loc[0,"Close"]

#         Signal_df.loc[increment,"SMA_Call"] = technical_data.loc[0,"SMA_Call"]
#         Signal_df.loc[increment,"RSI_Call"] = technical_data.loc[0,"RSI_Call"]
#         Signal_df.loc[increment,"MACD_Call"] = technical_data.loc[0,"MACD_Call"]
#         Signal_df.loc[increment,"Pivot_Call"] = technical_data.loc[0,"Pivot_Call"]
#         Signal_df.loc[increment,"BB_Call"] = technical_data.loc[0,"BB_Call"]
#         Signal_df.loc[increment,"VWAP_Call"] = technical_data.loc[0,"VWAP_Call"]
#         Signal_df.loc[increment,"SuperTrend_Call"] = technical_data.loc[0,"SuperTrend_Call"]
#         Signal_df.loc[increment,"PCR_Call"] = pcr_call

        increment = increment+1


def reds_rocket(stock,data):
    print("running : reds_rocket")
    now = datetime.now(timezone("Asia/Kolkata"))
    
    final_levels_df = pd.read_csv("~/Downloads/Reddy_Stocks_Application/data/reds_rocket.csv",index_col=False)
    
#     print(final_levels_df)
    
    global increment
    
    # for idx in range(0,len(nse_data)):
    #     stock = nse_data.loc[idx,"Yahoo_Symbol"]
    
#     print(final_levels_df)
    sub_df = final_levels_df.loc[(final_levels_df['Stock'] == stock)]
    sub_df.reset_index(inplace = True, drop = True)
    
    satisfied_df = pd.DataFrame(columns=['Datetime', 'Open', 'High','Low', 'Close', 'Adj Close','Volume','date','Call'])

    if len(sub_df) > 0:
        # Get the data  
        
        data = data[data['Datetime'] >= now.strftime("%Y-%m-%d")]
        
        data.reset_index(level=0, inplace=True,drop = True)
        
        final_data = data
        
        print(final_data)

        
        for j in range(0,len(final_data)):
            if final_data.loc[j,"Close"] > sub_df.loc[0,"Reds_High"]:
                temp_call_data = final_data.loc[j,]
                temp_call_data = temp_call_data.append(pd.Series("Buy",index=["Call"]))
                satisfied_df = satisfied_df.append(temp_call_data, ignore_index = True)
            elif final_data.loc[j,"Close"] < sub_df.loc[0,"Reds_Low"]:
                temp_call_data = final_data.loc[j,]
                temp_call_data = temp_call_data.append(pd.Series("Sell",index=["Call"]))
                satisfied_df = satisfied_df.append(temp_call_data, ignore_index = True)
        
        # print("reds_rocekt data")
        # print(satisfied_df)
        if not satisfied_df.empty:
            satisfied_df = satisfied_df.head(1)
            satisfied_df.reset_index(inplace = True, drop = True)

            ind_time = datetime.now(timezone("Asia/Kolkata"))

#             pcr_call = technical_data.loc[0,"PCR_Call"]

            
            print(time_delta_mins)
            
            Signal_df.loc[increment,"Strategy"] = "Reds Rocket"
            Signal_df.loc[increment,"Stock"] = stock
            Signal_df.loc[increment,"Signal"] = satisfied_df.loc[0,"Call"]
            Signal_df.loc[increment,"Datetime"] = satisfied_df.loc[0,"Datetime"]
            Signal_df.loc[increment,"Value"] = satisfied_df.loc[0,"Close"]

#             Signal_df.loc[increment,"SMA_Call"] = technical_data.loc[0,"SMA_Call"]
#             Signal_df.loc[increment,"RSI_Call"] = technical_data.loc[0,"RSI_Call"]
#             Signal_df.loc[increment,"MACD_Call"] = technical_data.loc[0,"MACD_Call"]
#             Signal_df.loc[increment,"Pivot_Call"] = technical_data.loc[0,"Pivot_Call"]
#             Signal_df.loc[increment,"BB_Call"] = technical_data.loc[0,"BB_Call"]
#             Signal_df.loc[increment,"VWAP_Call"] = technical_data.loc[0,"VWAP_Call"]
#             Signal_df.loc[increment,"SuperTrend_Call"] = technical_data.loc[0,"SuperTrend_Call"]
#             Signal_df.loc[increment,"PCR_Call"] = pcr_call

            increment = increment+1
            
    else:
        print("Reds Rocket Criteria not met previous day")


def reds_brahmos(stock,data):
    print("running : reds_brahmos")
    
    now = datetime.now(timezone("Asia/Kolkata"))
    
    final_levels_df = pd.read_csv("~/Downloads/Reddy_Stocks_Application/data/reds_brahmos.csv",index_col=False)
    
    global increment
    
    
    # for idx in range(0,len(nse_data)):
    #     stock = nse_data.loc[idx,"Yahoo_Symbol"]

    sub_df = final_levels_df.loc[(final_levels_df['Stock'] == stock)]
    sub_df.reset_index(inplace = True, drop = True)
    
#     print(sub_df)
    
    if len(sub_df) > 0:
        # Get the data
        
        
        data = data[data['Datetime'] >= now.strftime("%Y-%m-%d")]
        
        data.reset_index(level=0, inplace=True,drop = True)
        
        final_data = data
        
#         print(final_data)
        
        satisfied_df = pd.DataFrame(columns=['Datetime', 'Open', 'High','Low', 'Close', 'Adj Close','Volume','date','Call'])
        
        for j in range(0,len(final_data)):
            if final_data.loc[j,"Close"] > sub_df.loc[0,"Reds_High"]:
                temp_call_data = final_data.loc[j,]
                temp_call_data = temp_call_data.append(pd.Series("Buy",index=["Call"]))
                satisfied_df = satisfied_df.append(temp_call_data, ignore_index = True)
            elif final_data.loc[j,"Close"] < sub_df.loc[0,"Reds_Low"]:
                temp_call_data = final_data.loc[j,]
                temp_call_data = temp_call_data.append(pd.Series("Sell",index=["Call"]))
                satisfied_df = satisfied_df.append(temp_call_data, ignore_index = True)
                
        if not satisfied_df.empty:
            satisfied_df = satisfied_df.head(1)
            
            satisfied_df.reset_index(inplace = True, drop = True)

            ind_time = datetime.now(timezone("Asia/Kolkata"))

#             pcr_call = technical_data.loc[0,"PCR_Call"]
            
            Signal_df.loc[increment,"Strategy"] = "Reds Brahmos"
            Signal_df.loc[increment,"Stock"] = stock
            Signal_df.loc[increment,"Signal"] = satisfied_df.loc[0,"Call"]
            Signal_df.loc[increment,"Datetime"] = satisfied_df.loc[0,"Datetime"]
            Signal_df.loc[increment,"Value"] = satisfied_df.loc[0,"Close"]

#             Signal_df.loc[increment,"SMA_Call"] = technical_data.loc[0,"SMA_Call"]
#             Signal_df.loc[increment,"RSI_Call"] = technical_data.loc[0,"RSI_Call"]
#             Signal_df.loc[increment,"MACD_Call"] = technical_data.loc[0,"MACD_Call"]
#             Signal_df.loc[increment,"Pivot_Call"] = technical_data.loc[0,"Pivot_Call"]
#             Signal_df.loc[increment,"BB_Call"] = technical_data.loc[0,"BB_Call"]
#             Signal_df.loc[increment,"VWAP_Call"] = technical_data.loc[0,"VWAP_Call"]
#             Signal_df.loc[increment,"SuperTrend_Call"] = technical_data.loc[0,"SuperTrend_Call"]
#             Signal_df.loc[increment,"PCR_Call"] = pcr_call

            increment = increment+1
    else:
        print("Reds Brahmos Criteria not met previous day")       
    

def blackout(stock,data):
    print("running : blackout")
    now = datetime.now(timezone("Asia/Kolkata"))
    
    final_levels_df = pd.read_csv("~/Downloads/Reddy_Stocks_Application/data/blackout.csv",index_col=False)
    
    global increment
    
    
    # for idx in range(0,len(nse_data)):
    #     stock = nse_data.loc[idx,"Yahoo_Symbol"]

    sub_df = final_levels_df.loc[(final_levels_df['Stock'] == stock)]
    sub_df.reset_index(inplace = True, drop = True)
    
    if len(sub_df) > 0:
        # Get the data
    
        
        data = data[data['Datetime'] >= now.strftime("%Y-%m-%d")]
        
        data.reset_index(level=0, inplace=True,drop = True)
        
#         print(data)

        # Convert the date to datetime64
        data['date'] = pd.to_datetime(data['Datetime'], format='%Y-%m-%d')

        final_data = data.loc[(data['date'] >= now.strftime("%Y-%m-%d"))]

        final_data.reset_index(inplace = True, drop = True)
        
        satisfied_df = pd.DataFrame(columns=['Datetime', 'Open', 'High','Low', 'Close', 'Adj Close','Volume','date','Call'])
        
        if sub_df.loc[0,"stage"] == "Short":
            for j in range(0,len(final_data)):
                if final_data.loc[j,"Close"] < sub_df.loc[0,"target"]:
                    temp_call_data = final_data.loc[j,]
                    temp_call_data = temp_call_data.append(pd.Series("Sell",index=["Call"]))
                    satisfied_df = satisfied_df.append(temp_call_data, ignore_index = True)
        else:
            for j in range(0,len(final_data)):
                if final_data.loc[j,"Close"] > sub_df.loc[0,"target"]:
                    temp_call_data = final_data.loc[j,]
                    temp_call_data = temp_call_data.append(pd.Series("Buy",index=["Call"]))
                    satisfied_df = satisfied_df.append(temp_call_data, ignore_index = True)  
                    
        if not satisfied_df.empty:
            satisfied_df = satisfied_df.head(1)
            
            satisfied_df.reset_index(inplace = True, drop = True)

            ind_time = datetime.now(timezone("Asia/Kolkata"))

#             pcr_call = technical_data.loc[0,"PCR_Call"]
            
            Signal_df.loc[increment,"Strategy"] = "Blackout"
            Signal_df.loc[increment,"Stock"] = stock
            Signal_df.loc[increment,"Signal"] = satisfied_df.loc[0,"Call"]
            Signal_df.loc[increment,"Datetime"] = satisfied_df.loc[0,"Datetime"]
            Signal_df.loc[increment,"Value"] = satisfied_df.loc[0,"Close"]

#             Signal_df.loc[increment,"SMA_Call"] = technical_data.loc[0,"SMA_Call"]
#             Signal_df.loc[increment,"RSI_Call"] = technical_data.loc[0,"RSI_Call"]
#             Signal_df.loc[increment,"MACD_Call"] = technical_data.loc[0,"MACD_Call"]
#             Signal_df.loc[increment,"Pivot_Call"] = technical_data.loc[0,"Pivot_Call"]
#             Signal_df.loc[increment,"BB_Call"] = technical_data.loc[0,"BB_Call"]
#             Signal_df.loc[increment,"VWAP_Call"] = technical_data.loc[0,"VWAP_Call"]
#             Signal_df.loc[increment,"SuperTrend_Call"] = technical_data.loc[0,"SuperTrend_Call"]
#             Signal_df.loc[increment,"PCR_Call"] = pcr_call

            increment = increment+1
            
    else:
        print("Blackout Criteria not met previous day") 


def gap_up(stock,data):
    print("running : gap_up")
    now = datetime.now(timezone("Asia/Kolkata"))
    
    current_time = now.strftime("%H:%M:%S")
    global increment
    
    if(current_time >= "09:35:00"): 
        final_levels_df = pd.read_csv("~/Downloads/Reddy_Stocks_Application/data/gaps_strategy.csv",index_col=False)
     

        sub_df = final_levels_df.loc[(final_levels_df['Stock'] == stock)]
        sub_df.reset_index(inplace = True, drop = True)
        satisfied_df = pd.DataFrame(columns=['Datetime', 'Open', 'High','Low', 'Close', 'Adj Close','Volume','date','Call'])
        
#         print(sub_df)
        
        if len(sub_df) > 0:
            
            high_price = sub_df.loc[0,"Previous_High"]
            close_price = sub_df.loc[0,"Previous_Close"]
            
            

#             cut_offtime = now
#             # print(now)
#             if(int(now.strftime('%M')) % 5 > 0):
#                 cut_offtime = now + timedelta(minutes=-(int(now.strftime('%M'))%5))
#                 cut_offtime = cut_offtime.strftime('%Y-%m-%d %H:%M:00')
#                 # print(cut_offtime)
#             else:
#                 cut_offtime = now
#                 cut_offtime = cut_offtime.strftime('%Y-%m-%d %H:%M:00')
#                 # print(cut_offtime)
            

            # Convert the date to datetime64
            data['date'] = pd.to_datetime(data['Datetime'], format='%Y-%m-%d')

#             final_data = data.loc[(data['date'] >= now.strftime("%Y-%m-%d")) & (data['Datetime'] <= cut_offtime)]
            final_data = data.loc[(data['date'] >= now.strftime("%Y-%m-%d"))]

            final_data.reset_index(inplace = True, drop = True)
        
#             print(final_data)

            satisfied_df = pd.DataFrame(columns=['Datetime', 'Open', 'High','Low', 'Close', 'Adj Close','Volume','date','Call'])
            
            open_price = final_data.loc[0,"Open"]
            
            if open_price > close_price:
                for j in range(4,len(final_data)):
                    current_date = final_data.loc[j,"Datetime"]
                    
                    day_high = max(np.nanmax(final_data[0:j]["Close"]),np.nanmax(final_data[0:j]["Open"]))
                    
                    day_low = min(np.nanmin(final_data[0:j]["Close"]),np.nanmin(final_data[0:j]["Open"]))
                    
                    low_range = min(final_data.loc[j-1,"Low"],final_data.loc[j-2,"Low"],final_data.loc[j-3,"Low"],final_data.loc[j-4,"Low"])
                    high_range = max(final_data.loc[j-1,"High"],final_data.loc[j-2,"High"],final_data.loc[j-3,"High"],final_data.loc[j-4,"High"])
                    
                    current_close = final_data.loc[j,"Close"]
                    
                    if ((abs(high_range - low_range)/low_range * 100 < 0.4) and (current_close >= high_price) and (current_close >= day_high)):
                        temp_call_data = final_data.loc[j,]
                        temp_call_data = temp_call_data.append(pd.Series("Buy",index=["Call"]))
                        satisfied_df = satisfied_df.append(temp_call_data, ignore_index = True)
                        
                    elif ((abs(high_range - low_range)/low_range * 100 < 0.4) and (current_close <= close_price) and (current_close <= day_low)):
                        temp_call_data = final_data.loc[j,]
                        temp_call_data = temp_call_data.append(pd.Series("Sell",index=["Call"]))
                        satisfied_df = satisfied_df.append(temp_call_data, ignore_index = True)
                        

        if not satisfied_df.empty:
            satisfied_df = satisfied_df.head(1)
            
            satisfied_df.reset_index(inplace = True, drop = True)

            ind_time = datetime.now(timezone("Asia/Kolkata"))
#             time_delta = ind_time - satisfied_df.loc[0,"Datetime"]
#             time_delta_mins = time_delta.total_seconds()/60

#             pcr_call = technical_data.loc[0,"PCR_Call"]


            

#             if(time_delta_mins >= 4):
            
            Signal_df.loc[increment,"Strategy"] = "Gap_up"
            Signal_df.loc[increment,"Stock"] = stock
            Signal_df.loc[increment,"Signal"] = satisfied_df.loc[0,"Call"]
            Signal_df.loc[increment,"Datetime"] = satisfied_df.loc[0,"Datetime"]
            Signal_df.loc[increment,"Value"] = satisfied_df.loc[0,"Close"]

#             Signal_df.loc[increment,"SMA_Call"] = technical_data.loc[0,"SMA_Call"]
#             Signal_df.loc[increment,"RSI_Call"] = technical_data.loc[0,"RSI_Call"]
#             Signal_df.loc[increment,"MACD_Call"] = technical_data.loc[0,"MACD_Call"]
#             Signal_df.loc[increment,"Pivot_Call"] = technical_data.loc[0,"Pivot_Call"]
#             Signal_df.loc[increment,"BB_Call"] = technical_data.loc[0,"BB_Call"]
#             Signal_df.loc[increment,"VWAP_Call"] = technical_data.loc[0,"VWAP_Call"]
#             Signal_df.loc[increment,"SuperTrend_Call"] = technical_data.loc[0,"SuperTrend_Call"]
#             Signal_df.loc[increment,"PCR_Call"] = pcr_call

            increment = increment+1


def gap_down(stock,data):
    print("running : gap_down")
    now = datetime.now(timezone("Asia/Kolkata"))
#     now = datetime.now() + timedelta(hours=5,minutes=30)
    current_time = now.strftime("%H:%M:%S")
    global increment
    
    if(current_time >= "09:35:00"): 
        final_levels_df = pd.read_csv("~/Downloads/Reddy_Stocks_Application/data/gaps_strategy.csv",index_col=False)

        sub_df = final_levels_df.loc[(final_levels_df['Stock'] == stock)]
        sub_df.reset_index(inplace = True, drop = True)

        satisfied_df = pd.DataFrame(columns=['Datetime', 'Open', 'High','Low', 'Close', 'Adj Close','Volume','date','Call'])
        
        if len(sub_df) > 0:
#                 stock = sub_df.loc[0,"Stock"]
            high_price = sub_df.loc[0,"Previous_High"]
            close_price = sub_df.loc[0,"Previous_Close"]
            prev_low_price = sub_df.loc[0,"Previous_Low"]
            
            

            data = data[data['Datetime'] >= now.strftime("%Y-%m-%d")]

#             cut_offtime = now
#             # print(now)
#             if(int(now.strftime('%M')) % 5 > 0):
#                 cut_offtime = now + timedelta(minutes=-(int(now.strftime('%M'))%5))
#                 cut_offtime = cut_offtime.strftime('%Y-%m-%d %H:%M:00')
#                 # print(cut_offtime)
#             else:
#                 cut_offtime = now
#                 cut_offtime = cut_offtime.strftime('%Y-%m-%d %H:%M:00')
            

            # Convert the date to datetime64
            data['date'] = pd.to_datetime(data['Datetime'], format='%Y-%m-%d')

            final_data = data.loc[(data['date'] >= now.strftime("%Y-%m-%d"))]

            final_data.reset_index(inplace = True, drop = True)

            satisfied_df = pd.DataFrame(columns=['Datetime', 'Open', 'High','Low', 'Close', 'Adj Close','Volume','date','Call'])
            
            open_price = final_data.loc[0,"Open"]
            
            if open_price < close_price:
                for j in range(4,len(final_data)):
                    current_date = final_data.loc[j,"Datetime"]
                    
                    day_high = max(np.nanmax(final_data[0:j]["Close"]),np.nanmax(final_data[0:j]["Open"]))
                    
                    day_low = min(np.nanmin(final_data[0:j]["Close"]),np.nanmin(final_data[0:j]["Open"]))
                    
                    low_range = min(final_data.loc[j-1,"Low"],final_data.loc[j-2,"Low"],final_data.loc[j-3,"Low"],final_data.loc[j-4,"Low"])
                    high_range = max(final_data.loc[j-1,"High"],final_data.loc[j-2,"High"],final_data.loc[j-3,"High"],final_data.loc[j-4,"High"])
                    
                    current_close = final_data.loc[j,"Close"]
                    current_high = final_data.loc[j,"High"]
                    current_low = final_data.loc[j,"Low"]
                    
                    if ((abs(high_range - low_range)/low_range * 100 < 0.4) and (current_close >= high_price) and (current_close >= day_high)):
                        temp_call_data = final_data.loc[j,]
                        temp_call_data = temp_call_data.append(pd.Series("Buy",index=["Call"]))
                        satisfied_df = satisfied_df.append(temp_call_data, ignore_index = True)
                        
                    elif ((abs(high_range - low_range)/low_range * 100 < 0.4) and (current_close <= prev_low_price) and (current_low <= day_low)):
                        temp_call_data = final_data.loc[j,]
                        temp_call_data = temp_call_data.append(pd.Series("Sell",index=["Call"]))
                        satisfied_df = satisfied_df.append(temp_call_data, ignore_index = True)
                        

        if not satisfied_df.empty:
            satisfied_df = satisfied_df.head(1)
            
            satisfied_df.reset_index(inplace = True, drop = True)

            ind_time = datetime.now(timezone("Asia/Kolkata"))
#             time_delta = ind_time - satisfied_df.loc[0,"Datetime"]
#             time_delta_mins = time_delta.total_seconds()/60

#             pcr_call = technical_data.loc[0,"PCR_Call"]



#             if(time_delta_mins >= 4):
            
            Signal_df.loc[increment,"Strategy"] = "Gap_down"
            Signal_df.loc[increment,"Stock"] = stock
            Signal_df.loc[increment,"Signal"] = satisfied_df.loc[0,"Call"]
            Signal_df.loc[increment,"Datetime"] = satisfied_df.loc[0,"Datetime"]
            Signal_df.loc[increment,"Value"] = satisfied_df.loc[0,"Close"]

#             Signal_df.loc[increment,"SMA_Call"] = technical_data.loc[0,"SMA_Call"]
#             Signal_df.loc[increment,"RSI_Call"] = technical_data.loc[0,"RSI_Call"]
#             Signal_df.loc[increment,"MACD_Call"] = technical_data.loc[0,"MACD_Call"]
#             Signal_df.loc[increment,"Pivot_Call"] = technical_data.loc[0,"Pivot_Call"]
#             Signal_df.loc[increment,"BB_Call"] = technical_data.loc[0,"BB_Call"]
#             Signal_df.loc[increment,"VWAP_Call"] = technical_data.loc[0,"VWAP_Call"]
#             Signal_df.loc[increment,"SuperTrend_Call"] = technical_data.loc[0,"SuperTrend_Call"]
#             Signal_df.loc[increment,"PCR_Call"] = pcr_call

            increment = increment+1

def abc_5_cand(stock,data):
    print("running : abc_5_cand")
    now = datetime.now(timezone("Asia/Kolkata"))
#     now = datetime.now() + timedelta(hours=5,minutes=30)
    current_time = now.strftime("%H:%M:%S")
    global increment
    
#     if(current_time >= "09:40:00"):
       

    data['Datetime'] = pd.to_datetime(data['Datetime'])

    data = data[data['Datetime'] >= now.strftime("%Y-%m-%d")]
    
    data.reset_index(level=0, inplace=True,drop = True)


    # Convert the date to datetime64
    data['date'] = pd.to_datetime(data['Datetime'], format='%Y-%m-%d')

    final_data = data.loc[(data['date'] >= now.strftime("%Y-%m-%d"))]

    final_data.reset_index(inplace = True, drop = True)

    satisfied_df = pd.DataFrame(columns=['Datetime', 'Open', 'High','Low', 'Close', 'Adj Close','Volume','date','Call'])

    # Starting with the third candle
    for j in range(5,len(final_data)):
        # Check if the candle is a green candle
        if final_data.loc[j,"Close"] > final_data.loc[j,"Open"]:
            # Check if the prior candles are in the reversal trend
            if((final_data.loc[j-1,"Low"] < final_data.loc[j-2,"Low"]) and (final_data.loc[j-2,"Low"] < final_data.loc[j-3,"Low"])):

                # Get the breakout max in the reversal i.e., B Point
                reversal_high = max(final_data.loc[j-1,"High"],final_data.loc[j-2,"High"],final_data.loc[j-3,"High"])

                # Get the breakout min in the reversal i.e., C point
                reversal_low = min(final_data.loc[j-1,"Low"],final_data.loc[j-2,"Low"],final_data.loc[j-3,"Low"])

                # Check if the before reversal is a uptrend
                if(final_data.loc[j-3,"High"] > final_data.loc[j-4,"High"] and final_data.loc[j-4,"High"] > final_data.loc[j-5,"High"]):
                    # Get the starting point of the trend i.e., A point
                    trend_low = min(final_data.loc[j-4,"Low"],final_data.loc[j-5,"Low"])

                    # Check if the ABC pattern is completely followed
                    if(final_data.loc[j,"Close"] > reversal_high and reversal_low > trend_low):
                        temp_call_data = final_data.loc[j,]
                        temp_call_data = temp_call_data.append(pd.Series("Buy",index=["Call"]))
                        satisfied_df = satisfied_df.append(temp_call_data, ignore_index = True)

        else:
            # Check if the prior candles are in the reversal trend
            if(final_data.loc[j-1,"High"] > final_data.loc[j-2,"High"] and final_data.loc[j-2,"High"] > final_data.loc[j-3,"High"]):

                # Get the breakout max in the reversal i.e., B Point
                reversal_high = min(final_data.loc[j-1,"Low"],final_data.loc[j-2,"Low"],final_data.loc[j-3,"Low"])

                # Get the breakout min in the reversal i.e., C point
                reversal_low = max(final_data.loc[j-1,"High"],final_data.loc[j-2,"High"],final_data.loc[j-3,"High"])

                # Check if the before reversal is a uptrend
                if(final_data.loc[j-3,"Low"] < final_data.loc[j-4,"Low"] and final_data.loc[j-4,"Low"] < final_data.loc[j-5,"Low"]):
                    # Get the starting point of the trend i.e., A point
                    trend_low = max(final_data.loc[j-4,"High"],final_data.loc[j-5,"High"])

                    # Check if the ABC pattern is completely followed
                    if(final_data.loc[j,"Close"] < reversal_high and reversal_low < trend_low):
                        temp_call_data = final_data.loc[j,]
                        temp_call_data = temp_call_data.append(pd.Series("Sell",index=["Call"]))
                        satisfied_df = satisfied_df.append(temp_call_data, ignore_index = True)


    if not satisfied_df.empty:
        satisfied_df = satisfied_df.head(1)

        satisfied_df.reset_index(inplace = True, drop = True)

        ind_time = datetime.now(timezone("Asia/Kolkata"))
#         time_delta = ind_time - satisfied_df.loc[0,"Datetime"]
#         time_delta_mins = time_delta.total_seconds()/60

#         pcr_call = technical_data.loc[0,"PCR_Call"]


        Signal_df.loc[increment,"Strategy"] = "5_Cand_ABC"
        Signal_df.loc[increment,"Stock"] = stock
        Signal_df.loc[increment,"Signal"] = satisfied_df.loc[0,"Call"]
        Signal_df.loc[increment,"Datetime"] = satisfied_df.loc[0,"Datetime"]
        Signal_df.loc[increment,"Value"] = satisfied_df.loc[0,"Close"]

#         Signal_df.loc[increment,"SMA_Call"] = technical_data.loc[0,"SMA_Call"]
#         Signal_df.loc[increment,"RSI_Call"] = technical_data.loc[0,"RSI_Call"]
#         Signal_df.loc[increment,"MACD_Call"] = technical_data.loc[0,"MACD_Call"]
#         Signal_df.loc[increment,"Pivot_Call"] = technical_data.loc[0,"Pivot_Call"]
#         Signal_df.loc[increment,"BB_Call"] = technical_data.loc[0,"BB_Call"]
#         Signal_df.loc[increment,"VWAP_Call"] = technical_data.loc[0,"VWAP_Call"]
#         Signal_df.loc[increment,"SuperTrend_Call"] = technical_data.loc[0,"SuperTrend_Call"]
#         Signal_df.loc[increment,"PCR_Call"] = pcr_call

        increment = increment+1


def abc_3_cand(stock,data):
    print("running : abc_3_cand")
    now = datetime.now(timezone("Asia/Kolkata"))
    current_time = now.strftime("%H:%M:%S")
    global increment
    
    if(current_time >= "09:40:00"):
        # for idx in range(0,len(nse_data)):
        #     stock = nse_data.loc[idx,"Yahoo_Symbol"]
        
        
        # Convert the date to datetime64
        data['date'] = pd.to_datetime(data['Datetime'], format='%Y-%m-%d')
        
        final_data = data.loc[(data['date'] >= now.strftime("%Y-%m-%d"))]
        
        final_data.reset_index(inplace = True, drop = True)
        
        satisfied_df = pd.DataFrame(columns=['Datetime', 'Open', 'High','Low', 'Close', 'Adj Close','Volume','date','Call'])
        
        # Starting with the first candle
        for i in range(2,len(final_data)):
            
            if((final_data.loc[i,"Close"] > final_data.loc[i,"Open"]) and (final_data.loc[i-1,"Close"] < final_data.loc[i-1,"Open"]) and (final_data.loc[i-2,"Close"] > final_data.loc[i-2,"Open"])):
                
                if((final_data.loc[i-1,"Low"] > final_data.loc[i-2,"Low"]) and (final_data.loc[i,"Close"] > final_data.loc[i-2,"High"]) and (final_data.loc[i-1,"High"] < final_data.loc[i-2,"High"])):
                    
                    first_range = final_data.loc[i-2,"High"] - final_data.loc[i-2,"Low"]
                    second_range = final_data.loc[i-1,"High"] - final_data.loc[i-1,"Low"]
                    if(first_range/second_range >= 2):
                        temp_call_data = final_data.loc[i,]
                        temp_call_data = temp_call_data.append(pd.Series("Buy",index=["Call"]))
                        satisfied_df = satisfied_df.append(temp_call_data, ignore_index = True)
                        
            elif((final_data.loc[i,"Close"] < final_data.loc[i,"Open"]) and (final_data.loc[i-1,"Close"] > final_data.loc[i-1,"Open"]) and (final_data.loc[i-2,"Close"] < final_data.loc[i-2,"Open"])):
                
                if((final_data.loc[i-1,"Low"] > final_data.loc[i-2,"Low"]) and (final_data.loc[i,"Close"] < final_data.loc[i-2,"Low"]) and (final_data.loc[i-1,"Low"] > final_data.loc[i-2,"Low"])):
                    
                    first_range = final_data.loc[i-2,"High"] - final_data.loc[i-2,"Low"]
                    second_range = final_data.loc[i-1,"High"] - final_data.loc[i-1,"Low"]
                    if(first_range/second_range >= 2):
                        temp_call_data = final_data.loc[i,]
                        temp_call_data = temp_call_data.append(pd.Series("Sell",index=["Call"]))
                        satisfied_df = satisfied_df.append(temp_call_data, ignore_index = True)
       
                    
        if not satisfied_df.empty:
                    
            satisfied_df = satisfied_df.head(1)
            
            satisfied_df.reset_index(inplace = True, drop = True)

            ind_time = datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")

            ind_time = datetime.strptime(ind_time, '%Y-%m-%d %H:%M:%S')


            time_delta = ind_time - satisfied_df.loc[0,"Datetime"]
            time_delta_mins = time_delta.total_seconds()/60

#             pcr_call = technical_data.loc[0,"PCR_Call"]
            
            Signal_df.loc[increment,"Strategy"] = "3_Cand_ABC"
            Signal_df.loc[increment,"Stock"] = stock
            Signal_df.loc[increment,"Signal"] = satisfied_df.loc[0,"Call"]
            Signal_df.loc[increment,"Datetime"] = satisfied_df.loc[0,"Datetime"]
            Signal_df.loc[increment,"Value"] = satisfied_df.loc[0,"Close"]
            
#             Signal_df.loc[increment,"SMA_Call"] = technical_data.loc[0,"SMA_Call"]
#             Signal_df.loc[increment,"RSI_Call"] = technical_data.loc[0,"RSI_Call"]
#             Signal_df.loc[increment,"MACD_Call"] = technical_data.loc[0,"MACD_Call"]
#             Signal_df.loc[increment,"Pivot_Call"] = technical_data.loc[0,"Pivot_Call"]
#             Signal_df.loc[increment,"BB_Call"] = technical_data.loc[0,"BB_Call"]
#             Signal_df.loc[increment,"VWAP_Call"] = technical_data.loc[0,"VWAP_Call"]
#             Signal_df.loc[increment,"SuperTrend_Call"] = technical_data.loc[0,"SuperTrend_Call"]
#             Signal_df.loc[increment,"PCR_Call"] = pcr_call

            increment = increment+1 
            

def volume_breakout(stock,data):
    print("running : volume_breakout")
    now = datetime.now(timezone("Asia/Kolkata"))
    current_time = now.strftime("%H:%M:%S")
    global increment

#     obj = SmartConnect(api_key="NsXKahCV")

#     time.sleep(1)

#     data = obj.generateSession("S970011","Welcome@123")
    
    if(current_time >= "09:35:00"):
        # for idx in range(0,len(nse_data)):
        #     stock = nse_data.loc[idx,"Yahoo_Symbol"]
            
            # print(stock)
            
        hist_df = data

        hist_df = hist_df[['Datetime','Open', 'High','Low', 'Close','Volume']]


        hist_df = hist_df[hist_df['Datetime'] >= now.strftime("%Y-%m-%d")]

        hist_df.reset_index(inplace = True, drop = True)

#         print(hist_df)

        hist_df['Volume_Rank'] = hist_df['Volume'].rank(ascending=False)

        hist_df['price_change'] = abs(hist_df["Low"] - hist_df["High"])

        for idx in range(0,len(hist_df)):
            if hist_df.loc[idx,"Close"] > hist_df.loc[idx,"Open"]:
                hist_df.loc[idx,'perc_change'] = hist_df.loc[idx,'price_change'] *1.00/hist_df.loc[idx,'Low']
            else:
                hist_df.loc[idx,'perc_change'] = hist_df.loc[idx,'price_change'] *1.00/hist_df.loc[idx,'High']

        satisfied_df = pd.DataFrame(columns=['Datetime', 'Open', 'High','Low', 'Close', 'Adj Close','Volume','date','Call'])


        hist_df = hist_df.sort_values(by=['Volume_Rank'], ascending=True)
        hist_df['Datetime'] = pd.to_datetime(hist_df['Datetime'])


        first_volume_data = hist_df.loc[hist_df['Volume_Rank'] == 1]
        first_volume_data.reset_index(inplace = True, drop = True)

        breakout_high_value = first_volume_data.loc[0,'High']
        breakout_low_value = first_volume_data.loc[0,'Low']
        breakout_time = first_volume_data.loc[0,'Datetime']


        temp_final_data = hist_df.loc[hist_df['Datetime'] > breakout_time]
        temp_final_data.reset_index(inplace = True, drop = True)

        for idx in range(0,len(temp_final_data)):
            if(temp_final_data.loc[idx,"Close"] > breakout_high_value and abs(temp_final_data.loc[idx,"Close"] - breakout_high_value)/breakout_high_value*100 <= 0.4):
                temp_final_data.loc[idx,"Signal"] = "Buy"
            elif(temp_final_data.loc[idx,"Close"] < breakout_low_value and abs(temp_final_data.loc[idx,"Close"] - breakout_low_value)/breakout_low_value*100 <= 0.4):
                temp_final_data.loc[idx,"Signal"] = "Sell"
            else:
                temp_final_data.loc[idx,"Signal"] = ""

        temp_final_data = temp_final_data.loc[temp_final_data['Volume_Rank'] <= 15,]

        temp_final_data = temp_final_data.sort_values(by=['Datetime'], ascending=True)

        temp_final_data['match'] = temp_final_data.Signal.eq(temp_final_data.Signal.shift())

        final_temp_data = temp_final_data.loc[(temp_final_data['match'] == False) & ((temp_final_data['Signal'] == 'Buy') | (temp_final_data['Signal'] == 'Sell'))]

        final_temp_data.reset_index(inplace = True, drop = True)

        # print(final_temp_data)

        if not final_temp_data.empty:
            for j in range(0,len(final_temp_data)):
                temp_call_data = final_temp_data.loc[j,]
                temp_call_data = temp_call_data.append(pd.Series(final_temp_data.loc[j,"Signal"],index=["Call"]))
                satisfied_df = satisfied_df.append(temp_call_data, ignore_index = True)

        if not satisfied_df.empty:

            satisfied_df = satisfied_df.head(1)

            satisfied_df.reset_index(inplace = True, drop = True)



            # print(pcr_call)

            Signal_df.loc[increment,"Strategy"] = "Volume_Breakout"
            Signal_df.loc[increment,"Stock"] = stock
            Signal_df.loc[increment,"Signal"] = satisfied_df.loc[0,"Call"]
            Signal_df.loc[increment,"Datetime"] = satisfied_df.loc[0,"Datetime"]
            Signal_df.loc[increment,"Value"] = satisfied_df.loc[0,"Close"]

#             Signal_df.loc[increment,"SMA_Call"] = technical_data.loc[0,"SMA_Call"]
#             Signal_df.loc[increment,"RSI_Call"] = technical_data.loc[0,"RSI_Call"]
#             Signal_df.loc[increment,"MACD_Call"] = technical_data.loc[0,"MACD_Call"]
#             Signal_df.loc[increment,"Pivot_Call"] = technical_data.loc[0,"Pivot_Call"]
#             Signal_df.loc[increment,"BB_Call"] = technical_data.loc[0,"BB_Call"]
#             Signal_df.loc[increment,"VWAP_Call"] = technical_data.loc[0,"VWAP_Call"]
#             Signal_df.loc[increment,"SuperTrend_Call"] = technical_data.loc[0,"SuperTrend_Call"]
#             Signal_df.loc[increment,"PCR_Call"] = technical_data.loc[0,"PCR_Call"]

            increment = increment+1 
                
#         except Exception as e:
#             print("Historical API failed: {}".format(e))


def options_chain_volume_breakout(stock):
    print("running : options_chain_volume_breakout")
    now = datetime.now(timezone("Asia/Kolkata"))
    current_time = now.strftime("%H:%M:%S")
    
#     print(current_time)

    global increment

    current_sym = ""

    try:

        if(current_time >= "09:35:00"):
            if stock == '%5ENSEI':
                current_sym = "Nifty"
            else:
                current_sym = "BankNifty"
            try:
                fut_path ='/Users/apple/Desktop/Python_Stocks_Automation/Options_data/'+current_sym+'/' + datetime.now().strftime('%Y-%m-%d')+'_Futures_Options_Signals.csv'
                opt_path ='/Users/apple/Desktop/Python_Stocks_Automation/Options_data/'+current_sym+'/' + datetime.now().strftime('%Y-%m-%d')+'_Options_Signals.csv'

    #             print(fut_path)

                futures_data = pd.read_csv(fut_path)

                opt_data = pd.read_csv(opt_path)

            except Exception as e:
    #                 /home/ubuntu/Options_Chain/
                fut_path ='/Users/apple/Desktop/Python_Stocks_Automation/Options_data/'+current_sym+'/' + (datetime.now()- timedelta(hours=5,minutes=30)).strftime('%Y-%m-%d') +'_Futures_Options_Signals.csv'
                opt_path ='/Users/apple/Desktop/Python_Stocks_Automation/Options_data/'+current_sym+'/' + (datetime.now()- timedelta(hours=5,minutes=30)).strftime('%Y-%m-%d') +'_Options_Signals.csv'

                futures_data = pd.read_csv(fut_path)

                opt_data = pd.read_csv(opt_path)


            final_data = pd.merge(opt_data,futures_data, how='inner',left_on='Datetime', right_on='Datetime')

            # final_data = final_data[[u'Datetime',u'Call_Interpretation_x']]

            final_data = final_data[[u'Datetime',u'Call_Interpretation',u'Put_Interpretation',u'pcr_ratio_x',u'current_call_volume',u'current_put_volume',u'Call_Majority_x',u'Put_Majority_x',u'call_volume_rank_x',u'put_volume_rank_x',u'signal',u'Strike_Price',u'future_volume',u'call_traded_volume',u'call_pchange',u'call_changeinopeninterest',u'put_traded_volume',u'put_pchange',u'put_changeinopeninterest',u'pcr_ratio_y',u'fut_volume_rank',u'call_volume_rank_y',u'put_volume_rank_y',u'call_value',u'put_value']]

            # print(final_data.columns)
            final_data.columns = ['Datetime','Call_Interpretation','Put_Interpretation','overall_pcr','current_call_volume','current_put_volume','overall_Call_Majority','overall_Put_Majority','overall_call_volume_rank','overall_put_volume_rank','overall_signal','ATM_Strike_Price','future_volume','call_fut_traded_volume','ATM_call_pchange','ATM_call_changeinopeninterest','ATM_put_traded_volume','ATM_put_pchange','ATM_put_changeinopeninterest','ATM_pcr_ratio','fut_volume_rank','ATM_call_volume_rank','ATM_put_volume_rank','ATM_call_value','put_value']

            final_data['sum'] = final_data['fut_volume_rank'] + final_data['overall_call_volume_rank']+ final_data['overall_put_volume_rank']

            selected_data = final_data.sort_values('sum').head(1)

            selected_data.reset_index(inplace= True, drop = True)

            # stock = nse_data.loc[stck,"Yahoo_Symbol"]

            hist_df = yf.download(tickers=stock, period="2d", interval="1m")
            hist_df = pd.DataFrame(hist_df)

            hist_df.reset_index(level=0, inplace=True)

            hist_df['Datetime'] = pd.to_datetime(hist_df['Datetime'])


            hist_df = hist_df[hist_df['Datetime'] >= now.strftime("%Y-%m-%d")]



            hist_df.reset_index(inplace = True, drop = True)

            hist_df = hist_df[['Datetime','Open', 'High','Low', 'Close','Adj Close','Volume']]

            hist_df.columns = ['Datetime','Open', 'High','Low', 'Close','Adj Close','Volume']

            # print(hist_df.head(5))

            temp_data = hist_df[pd.to_datetime(hist_df['Datetime']) >= selected_data.iloc[0,0]]

            temp_data.reset_index(drop = True,inplace = True)

            range_low = temp_data.loc[0,"Low"]
            range_high = temp_data.loc[0,"High"]

            print("Critical range for the day")

            print(range_low)
            print(range_high)
            print(temp_data.loc[0,"Datetime"])

            temp_data['position'] = ""

            for idx in range(1,len(temp_data)):
                if temp_data.loc[idx,'Low'] <= range_low and temp_data.loc[idx,'Low'] <= range_high and temp_data.loc[idx,'High'] <= range_high and temp_data.loc[idx,'High'] <= range_low:

                    temp_data.loc[idx,"position"] = "Sell"
                elif temp_data.loc[idx,'Low'] >= range_low and temp_data.loc[idx,'Low'] >= range_high and temp_data.loc[idx,'High'] >= range_high and temp_data.loc[idx,'High'] >= range_low:
                    temp_data.loc[idx,"position"] = "Buy"

            signals_df = temp_data[temp_data['position'] != ""]

            signals_df.reset_index(drop = True,inplace = True)

            signals_df['next_1_position'] = signals_df['position'].shift(-1)
            signals_df['next_2_position'] = signals_df['next_1_position'].shift(-1)
            signals_df['prev_1_position'] = signals_df['position'].shift(1)

            final_signal_df = signals_df[(signals_df['prev_1_position'] != signals_df['position']) & (signals_df['position'] == signals_df['next_1_position'])]

            temp_Signal_df = final_signal_df[['position','Datetime','Close']]
            temp_Signal_df.reset_index(inplace = True,drop = True)
            # print(temp_Signal_df)

            temp_Signal_df['Strategy'] = 'Options_Chain_Volume'
            temp_Signal_df['Stock'] = stock
            temp_Signal_df = temp_Signal_df[['Strategy','Stock','position','Datetime','Close']]

            temp_Signal_df.columns = ['Strategy','Stock','Signal','Datetime','Value']

            if not temp_Signal_df.empty:
                satisfied_df = temp_Signal_df.tail(1)

                satisfied_df.reset_index(inplace = True, drop = True)
                # print(satisfied_df)

                Signal_df.loc[increment,"Strategy"] = satisfied_df.loc[0,'Strategy']
                Signal_df.loc[increment,"Stock"] = satisfied_df.loc[0,'Stock']
                Signal_df.loc[increment,"Signal"] = satisfied_df.loc[0,'Signal']
                Signal_df.loc[increment,"Datetime"] = satisfied_df.loc[0,'Datetime'].strftime("%Y-%m-%d %H:%M:%S")
                Signal_df.loc[increment,"Value"] = round(satisfied_df.loc[0,'Value'],2)

#                 Signal_df.loc[increment,"SMA_Call"] = technical_data.loc[0,"SMA_Call"]
#                 Signal_df.loc[increment,"RSI_Call"] = technical_data.loc[0,"RSI_Call"]
#                 Signal_df.loc[increment,"MACD_Call"] = technical_data.loc[0,"MACD_Call"]
#                 Signal_df.loc[increment,"Pivot_Call"] = technical_data.loc[0,"Pivot_Call"]
#                 Signal_df.loc[increment,"BB_Call"] = technical_data.loc[0,"BB_Call"]
#                 Signal_df.loc[increment,"VWAP_Call"] = technical_data.loc[0,"VWAP_Call"]
#                 Signal_df.loc[increment,"SuperTrend_Call"] = technical_data.loc[0,"SuperTrend_Call"]
#                 Signal_df.loc[increment,"PCR_Call"] = technical_data.loc[0,"PCR_Call"]

                increment = increment + 1

    except Exception as e:
        print("options_chain_volume_breakout failed: {}".format(e))


def place_limit_order(final_signals_df,row_index,user_id,obj):

    if(final_signals_df.loc[row_index,"Stock"] == "%5ENSEBANK"):
        final_signals_df.loc[row_index,"lotsize"] = 25.0
    else:
        final_signals_df.loc[row_index,"lotsize"] = 50.0

    print(user_id)

    # print(final_signals_df.loc[row_index,"Strategy"])
    # os.system("say 'Place the order message'")

    if user_id == "J95213" or user_id == "S1604557" or user_id == "G304915" :
        print("Checking the orders for :"+ user_id)

        chat_message= ""

        if(int(final_signals_df.loc[row_index,"Probability"]) >= 40):

            order_params = {
                "variety":"NORMAL",
                "tradingsymbol":str(final_signals_df.loc[row_index,"current_script"]),
                "symboltoken":str(final_signals_df.loc[row_index,"token"]),
                "transactiontype":"BUY",
                "exchange":"NFO",
                "ordertype":"LIMIT",
                "producttype":"CARRYFORWARD",
                "duration":"DAY",
                "price":str(final_signals_df.loc[row_index,"Strike_Buy_Price"]),
                "squareoff":str(final_signals_df.loc[row_index,"premium_Target"]),
                "stoploss":str(final_signals_df.loc[row_index,"premium_StopLoss"]),
                "quantity":str(np.float64(final_signals_df.loc[row_index,"lotsize"]))
            }

            # print(final_signals_df.loc[row_index,])
            # print(order_params)
            # time.sleep(0.8)
            placed_order_id = obj.placeOrder(order_params)
            # placed_order_id = 1
            print("the limit orders is :")
            print(placed_order_id)
            chat_message = 'Placing the Limit Order for client id :'+ str(user_id) + "\n Time :"+ str(final_signals_df.loc[row_index,"Datetime"])  + "\n  Strategy : "+ str(final_signals_df.loc[row_index,"Strategy"]) + "\n Script :"  + str(final_signals_df.loc[row_index,"current_script"]) + "\n Buy Price :" + str(final_signals_df.loc[row_index,"Strike_Buy_Price"]) + "\n Premium Target :" + str(final_signals_df.loc[row_index,"premium_Target"]) + "\n Premium Stoploss :" + str(final_signals_df.loc[row_index,"premium_StopLoss"]) + "\n Lotsize :" + str(final_signals_df.loc[row_index,"lotsize"]) + "\n Probability :" + str(final_signals_df.loc[row_index,"Probability"]) + "\n Historic Profit :" + str(final_signals_df.loc[row_index,"historic_profit"])
        else:
            placed_order_id = 1
            print("the limit orders is :")
            print(placed_order_id)
            chat_message = '************** Skipping the Limit Order for client id :'+ str(user_id) + "\n Time :"+ str(final_signals_df.loc[row_index,"Datetime"])  + "\n  Strategy : "+ str(final_signals_df.loc[row_index,"Strategy"]) + "\n Script :"  + str(final_signals_df.loc[row_index,"current_script"]) + "\n Buy Price :" + str(final_signals_df.loc[row_index,"Strike_Buy_Price"]) + "\n Premium Target :" + str(final_signals_df.loc[row_index,"premium_Target"]) + "\n Premium Stoploss :" + str(final_signals_df.loc[row_index,"premium_StopLoss"]) + "\n Lotsize :" + str(final_signals_df.loc[row_index,"lotsize"]) + "\n Probability :" + str(final_signals_df.loc[row_index,"Probability"]) + "\n Historic Profit :" + str(final_signals_df.loc[row_index,"historic_profit"])


        print("Sending the message")
        # os.system("say 'Sending the message'")

        
        # print(chat_message)

        for cht in chat_id:
          message = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + str(cht) + '&parse_mode=Markdown&text=' + str(chat_message)
          # print(message)
          # print(cht)
          send = requests.post(message)
          # print(send)
        # print("after telegram message")

        final_signals_df.loc[row_index,"order_id"] = placed_order_id
        final_signals_df.loc[row_index,"order_place"] = 1
        final_signals_df.loc[row_index,"conclusion"] = "Pending"

        my_new_order = pd.DataFrame(final_signals_df.loc[row_index,])
        my_new_order = my_new_order.transpose()
        my_new_order.to_csv('~/Downloads/Reddy_Stocks_Application/data/Nifty_Indices_Python_Trading_Communication_Alerts.csv', mode='a',sep=',', header=False, index=False)

    elif user_id == "Y68412":
        if final_signals_df.loc[row_index,"historic_profit"] == 1:
            lot_size = str(final_signals_df.loc[row_index,"lotsize"])
            order_params = {
                            "variety":"NORMAL",
                            "tradingsymbol":str(final_signals_df.loc[row_index,"current_script"]),
                            "symboltoken":str(final_signals_df.loc[row_index,"token"]),
                            "transactiontype":"BUY",
                            "exchange":"NFO",
                            "ordertype":"LIMIT",
                            "producttype":"CARRYFORWARD",
                            "duration":"DAY",
                            "price":str(final_signals_df.loc[row_index,"Strike_Buy_Price"]),
                            "squareoff":str(final_signals_df.loc[row_index,"premium_Target"]),
                            "stoploss":str(final_signals_df.loc[row_index,"premium_StopLoss"]),
                            "quantity":str(final_signals_df.loc[row_index,"lotsize"])
                    }
            time.sleep(0.8)
            placed_order_id = obj.placeOrder(order_params)
            # placed_order_id = 1
            print("the limit order is :")
            print(placed_order_id)

            # os.system("say 'Sending the message'")
            # print("Sending the message")

            # chat_message = 'Placing the Limit Order using Python for client id :'+ str(user_id) + "\n Time :"+ str(final_signals_df.loc[row_index,"Datetime"])  + "\n  Strategy : "+ str(final_signals_df.loc[row_index,"Strategy"]) + "\n Script :"  + str(final_signals_df.loc[row_index,"current_script"]) + "\n Buy Price :" + str(final_signals_df.loc[row_index,"Strike_Buy_Price"]) + "\n Premium Target :" + str(final_signals_df.loc[row_index,"premium_Target"]) + "\n Premium Stoploss :" + str(final_signals_df.loc[row_index,"premium_StopLoss"]) + "\n Lotsize :" + str(final_signals_df.loc[row_index,"lotsize"])

            # print(chat_message)

            # for cht in chat_id:
            #   message = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + str(cht) + '&parse_mode=Markdown&text=' + str(chat_message)
            #   # print(message)
            #   # print(cht)
            #   send = requests.post(message)
            #   # print(send)

            final_signals_df.loc[row_index,"order_id"] = placed_order_id
            final_signals_df.loc[row_index,"order_place"] = 1
            final_signals_df.loc[row_index,"conclusion"] = "Pending"

            my_new_order = pd.DataFrame(final_signals_df.loc[row_index,])
            my_new_order = my_new_order.transpose()
            my_new_order.to_csv('~/Downloads/Reddy_Stocks_Application/data/Nifty_Indices_Python_Trading_Communication_Alerts.csv', mode='a',sep=',', header=False, index=False)
        else:
            placed_order_id = 1

            final_signals_df.loc[row_index,"order_id"] = placed_order_id
            final_signals_df.loc[row_index,"order_place"] = 1
            final_signals_df.loc[row_index,"conclusion"] = "Pending"

            my_new_order = pd.DataFrame(final_signals_df.loc[row_index,])
            my_new_order = my_new_order.transpose()
            my_new_order.to_csv('~/Downloads/Reddy_Stocks_Application/data/Nifty_Indices_Python_Trading_Communication_Alerts.csv', mode='a',sep=',', header=False, index=False)

            print("Skipping order")

    elif user_id == "I52206":
        print("Checking the orders for I52206")

        final_signals_df.loc[row_index,"lotsize"] = 2 * final_signals_df.loc[row_index,"lotsize"]
        # lot_size = str(final_signals_df.loc[row_index,"lotsize"])

        # print(final_signals_df.loc[row_index,"lotsize"])
        # print(type(final_signals_df.loc[row_index,"lotsize"]))

        order_params = {
            "variety":"NORMAL",
            "tradingsymbol":str(final_signals_df.loc[row_index,"current_script"]),
            "symboltoken":str(final_signals_df.loc[row_index,"token"]),
            "transactiontype":"BUY",
            "exchange":"NFO",
            "ordertype":"LIMIT",
            "producttype":"CARRYFORWARD",
            "duration":"DAY",
            "price":str(final_signals_df.loc[row_index,"Strike_Buy_Price"]),
            "squareoff":str(final_signals_df.loc[row_index,"premium_Target"]),
            "stoploss":str(final_signals_df.loc[row_index,"premium_StopLoss"]),
            "quantity":str(np.float64(final_signals_df.loc[row_index,"lotsize"]))
        }
        # print(final_signals_df.loc[row_index,])
        # print(order_params)
        # time.sleep(0.8)
        # placed_order_id = obj.placeOrder(order_params)
        placed_order_id = 1
        print("the limit orders is :")
        print(placed_order_id)

        print("Sending the message")
        # os.system("say 'Sending the message'")

        chat_message = 'Placing the Limit Order for client id :'+ str(user_id) + "\n Time :"+ str(final_signals_df.loc[row_index,"Datetime"])  + "\n  Strategy : "+ str(final_signals_df.loc[row_index,"Strategy"]) + "\n Script :"  + str(final_signals_df.loc[row_index,"current_script"]) + "\n Buy Price :" + str(final_signals_df.loc[row_index,"Strike_Buy_Price"]) + "\n Premium Target :" + str(final_signals_df.loc[row_index,"premium_Target"]) + "\n Premium Stoploss :" + str(final_signals_df.loc[row_index,"premium_StopLoss"]) + "\n Lotsize :" + str(final_signals_df.loc[row_index,"lotsize"]) + "\n Probability :" + str(final_signals_df.loc[row_index,"Probability"])
        # print(chat_message)

        for cht in chat_id:
          message = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + str(cht) + '&parse_mode=Markdown&text=' + str(chat_message)
          # print(message)
          # print(cht)
          send = requests.post(message)
          # print(send)

        final_signals_df.loc[row_index,"order_id"] = placed_order_id
        final_signals_df.loc[row_index,"order_place"] = 1
        final_signals_df.loc[row_index,"conclusion"] = "Pending"

        my_new_order = pd.DataFrame(final_signals_df.loc[row_index,])
        my_new_order = my_new_order.transpose()
        my_new_order.to_csv('~/Downloads/Reddy_Stocks_Application/data/Nifty_Indices_Python_Trading_Communication_Alerts.csv', mode='a',sep=',', header=False, index=False)

    elif user_id == "A987129":
        print("Checking the orders for A987129")

        print(final_signals_df.loc[row_index,"lotsize"])
        print(type(final_signals_df.loc[row_index,"lotsize"]))
        lot_size = str(final_signals_df.loc[row_index,"lotsize"])

        chat_message =""

        if(int(final_signals_df.loc[row_index,"Probability"]) >= 40):

            order_params = {
                "variety":"NORMAL",
                "tradingsymbol":str(final_signals_df.loc[row_index,"current_script"]),
                "symboltoken":str(final_signals_df.loc[row_index,"token"]),
                "transactiontype":"BUY",
                "exchange":"NFO",
                "ordertype":"LIMIT",
                "producttype":"CARRYFORWARD",
                "duration":"DAY",
                "price":str(final_signals_df.loc[row_index,"Strike_Buy_Price"]),
                "squareoff":str(final_signals_df.loc[row_index,"premium_Target"]),
                "stoploss":str(final_signals_df.loc[row_index,"premium_StopLoss"]),
                "quantity":str(np.float64(final_signals_df.loc[row_index,"lotsize"]))
            }
            # print(final_signals_df.loc[row_index,])
            # print(order_params)
            # time.sleep(0.8)
            placed_order_id = obj.placeOrder(order_params)
            # placed_order_id = 1
            print("the limit orders is :")
            print(placed_order_id)

            print("Sending the message")
            # os.system("say 'Sending the message'")

            chat_message = 'Placing the Limit Order for client id :'+ str(user_id) + "\n Time :"+ str(final_signals_df.loc[row_index,"Datetime"])  + "\n  Strategy : "+ str(final_signals_df.loc[row_index,"Strategy"]) + "\n Script :"  + str(final_signals_df.loc[row_index,"current_script"]) + "\n Buy Price :" + str(final_signals_df.loc[row_index,"Strike_Buy_Price"]) + "\n Premium Target :" + str(final_signals_df.loc[row_index,"premium_Target"]) + "\n Premium Stoploss :" + str(final_signals_df.loc[row_index,"premium_StopLoss"]) + "\n Lotsize :" + str(final_signals_df.loc[row_index,"lotsize"]) + "\n Probability :" + str(final_signals_df.loc[row_index,"Probability"])
        else:
            placed_order_id = 1
            print("the limit orders is :")
            print(placed_order_id)
            chat_message = '************** Skipping the Limit Order for client id :'+ str(user_id) + "\n Time :"+ str(final_signals_df.loc[row_index,"Datetime"])  + "\n  Strategy : "+ str(final_signals_df.loc[row_index,"Strategy"]) + "\n Script :"  + str(final_signals_df.loc[row_index,"current_script"]) + "\n Buy Price :" + str(final_signals_df.loc[row_index,"Strike_Buy_Price"]) + "\n Premium Target :" + str(final_signals_df.loc[row_index,"premium_Target"]) + "\n Premium Stoploss :" + str(final_signals_df.loc[row_index,"premium_StopLoss"]) + "\n Lotsize :" + str(final_signals_df.loc[row_index,"lotsize"]) + "\n Probability :" + str(final_signals_df.loc[row_index,"Probability"])

        for cht in chat_id:
          message = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + str(cht) + '&parse_mode=Markdown&text=' + str(chat_message)
          # print(message)
          # print(cht)
          send = requests.post(message)
          # print(send)

        final_signals_df.loc[row_index,"order_id"] = placed_order_id
        final_signals_df.loc[row_index,"order_place"] = 1
        final_signals_df.loc[row_index,"conclusion"] = "Pending"

        my_new_order = pd.DataFrame(final_signals_df.loc[row_index,])
        my_new_order = my_new_order.transpose()
        my_new_order.to_csv('~/Downloads/Reddy_Stocks_Application/data/Nifty_Indices_Python_Trading_Communication_Alerts.csv', mode='a',sep=',', header=False, index=False)

    elif user_id == "K256027":
        print("Checking the orders for K256027")

        print(final_signals_df.loc[row_index,"lotsize"])
        print(type(final_signals_df.loc[row_index,"lotsize"]))
        lot_size = str(final_signals_df.loc[row_index,"lotsize"])

        chat_message =""

        if(int(final_signals_df.loc[row_index,"Probability"]) >= 40):

            order_params = {
                "variety":"NORMAL",
                "tradingsymbol":str(final_signals_df.loc[row_index,"current_script"]),
                "symboltoken":str(final_signals_df.loc[row_index,"token"]),
                "transactiontype":"BUY",
                "exchange":"NFO",
                "ordertype":"LIMIT",
                "producttype":"CARRYFORWARD",
                "duration":"DAY",
                "price":str(final_signals_df.loc[row_index,"Strike_Buy_Price"]),
                "squareoff":str(final_signals_df.loc[row_index,"premium_Target"]),
                "stoploss":str(final_signals_df.loc[row_index,"premium_StopLoss"]),
                "quantity":str(np.float64(final_signals_df.loc[row_index,"lotsize"]))
            }
            # print(final_signals_df.loc[row_index,])
            # print(order_params)
            # time.sleep(0.8)
            placed_order_id = obj.placeOrder(order_params)
            # placed_order_id = 1
            print("the limit orders is :")
            print(placed_order_id)

            print("Sending the message")
            # os.system("say 'Sending the message'")

            chat_message = 'Placing the Limit Order for client id :'+ str(user_id) + "\n Time :"+ str(final_signals_df.loc[row_index,"Datetime"])  + "\n  Strategy : "+ str(final_signals_df.loc[row_index,"Strategy"]) + "\n Script :"  + str(final_signals_df.loc[row_index,"current_script"]) + "\n Buy Price :" + str(final_signals_df.loc[row_index,"Strike_Buy_Price"]) + "\n Premium Target :" + str(final_signals_df.loc[row_index,"premium_Target"]) + "\n Premium Stoploss :" + str(final_signals_df.loc[row_index,"premium_StopLoss"]) + "\n Lotsize :" + str(final_signals_df.loc[row_index,"lotsize"]) + "\n Probability :" + str(final_signals_df.loc[row_index,"Probability"])
        else:
            placed_order_id = 1
            print("the limit orders is :")
            print(placed_order_id)
            chat_message = '************** Skipping the Limit Order for client id :'+ str(user_id) + "\n Time :"+ str(final_signals_df.loc[row_index,"Datetime"])  + "\n  Strategy : "+ str(final_signals_df.loc[row_index,"Strategy"]) + "\n Script :"  + str(final_signals_df.loc[row_index,"current_script"]) + "\n Buy Price :" + str(final_signals_df.loc[row_index,"Strike_Buy_Price"]) + "\n Premium Target :" + str(final_signals_df.loc[row_index,"premium_Target"]) + "\n Premium Stoploss :" + str(final_signals_df.loc[row_index,"premium_StopLoss"]) + "\n Lotsize :" + str(final_signals_df.loc[row_index,"lotsize"]) + "\n Probability :" + str(final_signals_df.loc[row_index,"Probability"])

        for cht in chat_id:
          message = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + str(cht) + '&parse_mode=Markdown&text=' + str(chat_message)
          # print(message)
          # print(cht)
          send = requests.post(message)
          # print(send)

        final_signals_df.loc[row_index,"order_id"] = placed_order_id
        final_signals_df.loc[row_index,"order_place"] = 1
        final_signals_df.loc[row_index,"conclusion"] = "Pending"

        my_new_order = pd.DataFrame(final_signals_df.loc[row_index,])
        my_new_order = my_new_order.transpose()
        my_new_order.to_csv('~/Downloads/Reddy_Stocks_Application/data/Nifty_Indices_Python_Trading_Communication_Alerts.csv', mode='a',sep=',', header=False, index=False)

    elif user_id == "S812966":
        if final_signals_df.loc[row_index,"Strategy"] == "Options_Chain_Volume":
            order_params = {
            "variety":"NORMAL",
            "tradingsymbol":str(final_signals_df.loc[row_index,"current_script"]),
            "symboltoken":str(final_signals_df.loc[row_index,"token"]),
            "transactiontype":"BUY",
            "exchange":"NFO",
            "ordertype":"LIMIT",
            "producttype":"CARRYFORWARD",
            "duration":"DAY",
            "price":str(final_signals_df.loc[row_index,"Strike_Buy_Price"]),
            "squareoff":str(final_signals_df.loc[row_index,"premium_Target"]),
            "stoploss":str(final_signals_df.loc[row_index,"premium_StopLoss"]),
            "quantity":str(np.float64(final_signals_df.loc[row_index,"lotsize"]))
            }   

            time.sleep(0.8)
            placed_order_id = obj.placeOrder(order_params)
            # placed_order_id = 1
            print("the limit order is :")
            print(placed_order_id)

            # os.system("say 'Sending the message'")
            # print("Sending the message")

            # chat_message = 'Placing the Limit Order using Python for client id :'+ str(user_id) + "\n Time :"+ str(final_signals_df.loc[row_index,"Datetime"])  + "\n  Strategy : "+ str(final_signals_df.loc[row_index,"Strategy"]) + "\n Script :"  + str(final_signals_df.loc[row_index,"current_script"]) + "\n Buy Price :" + str(final_signals_df.loc[row_index,"Strike_Buy_Price"]) + "\n Premium Target :" + str(final_signals_df.loc[row_index,"premium_Target"]) + "\n Premium Stoploss :" + str(final_signals_df.loc[row_index,"premium_StopLoss"]) + "\n Lotsize :" + str(final_signals_df.loc[row_index,"lotsize"])

            # print(chat_message)

            for cht in chat_id:
              message = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + str(cht) + '&parse_mode=Markdown&text=' + str(chat_message)
              # print(message)
              # print(cht)
              send = requests.post(message)
              # print(send)

            final_signals_df.loc[row_index,"order_id"] = placed_order_id
            final_signals_df.loc[row_index,"order_place"] = 1
            final_signals_df.loc[row_index,"conclusion"] = "Pending"

            my_new_order = pd.DataFrame(final_signals_df.loc[row_index,])
            my_new_order = my_new_order.transpose()
            my_new_order.to_csv('~/Downloads/Reddy_Stocks_Application/data/Nifty_Indices_Python_Trading_Communication_Alerts.csv', mode='a',sep=',', header=False, index=False)
        else:
            placed_order_id = 1

            final_signals_df.loc[row_index,"order_id"] = placed_order_id
            final_signals_df.loc[row_index,"order_place"] = 1
            final_signals_df.loc[row_index,"conclusion"] = "Pending"

            my_new_order = pd.DataFrame(final_signals_df.loc[row_index,])
            my_new_order = my_new_order.transpose()
            my_new_order.to_csv('~/Downloads/Reddy_Stocks_Application/data/Nifty_Indices_Python_Trading_Communication_Alerts.csv', mode='a',sep=',', header=False, index=False)

            print("Skipping order")


    else:
        print("Checking the orders for others")
        order_params = {
            "variety":"NORMAL",
            "tradingsymbol":str(final_signals_df.loc[row_index,"current_script"]),
            "symboltoken":str(final_signals_df.loc[row_index,"token"]),
            "transactiontype":"BUY",
            "exchange":"NFO",
            "ordertype":"LIMIT",
            "producttype":"CARRYFORWARD",
            "duration":"DAY",
            "price":str(final_signals_df.loc[row_index,"Strike_Buy_Price"]),
            "squareoff":str(final_signals_df.loc[row_index,"premium_Target"]),
            "stoploss":str(final_signals_df.loc[row_index,"premium_StopLoss"]),
            "quantity":str(np.float64(final_signals_df.loc[row_index,"lotsize"]))
        }
        # print(final_signals_df.loc[row_index,])
        # print(order_params)
        time.sleep(0.8)
        placed_order_id = obj.placeOrder(order_params)
        # placed_order_id = 1
        print("the limit orders is :")
        print(placed_order_id)

        # print("Sending the message")
        # os.system("say 'Sending the message'")

        # chat_message = 'Placing the Limit Order using Python for client id :'+ str(user_id) + "\n Time :"+ str(final_signals_df.loc[row_index,"Datetime"])  + "\n  Strategy : "+ str(final_signals_df.loc[row_index,"Strategy"]) + "\n Script :"  + str(final_signals_df.loc[row_index,"current_script"]) + "\n Buy Price :" + str(final_signals_df.loc[row_index,"Strike_Buy_Price"]) + "\n Premium Target :" + str(final_signals_df.loc[row_index,"premium_Target"]) + "\n Premium Stoploss :" + str(final_signals_df.loc[row_index,"premium_StopLoss"]) + "\n Lotsize :" + str(final_signals_df.loc[row_index,"lotsize"])
        # print(chat_message)

        # for cht in chat_id:
        #   message = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + str(cht) + '&parse_mode=Markdown&text=' + str(chat_message)
        #   # print(message)
        #   # print(cht)
        #   send = requests.post(message)
        #   # print(send)

        final_signals_df.loc[row_index,"order_id"] = placed_order_id
        final_signals_df.loc[row_index,"order_place"] = 1
        final_signals_df.loc[row_index,"conclusion"] = "Pending"

        my_new_order = pd.DataFrame(final_signals_df.loc[row_index,])
        my_new_order = my_new_order.transpose()
        my_new_order.to_csv('~/Downloads/Reddy_Stocks_Application/data/Nifty_Indices_Python_Trading_Communication_Alerts.csv', mode='a',sep=',', header=False, index=False)

angel_script = pd.read_csv("~/Downloads/angel_script.csv",index_col =False)

nse_data = pd.DataFrame([["BANKNIFTY","%5ENSEBANK","BANKNIFTY-EQ"],["Nifty50","%5ENSEI","Nifty50-EQ"]],columns=["Symbol","Yahoo_Symbol","TradingSymbol"])

strategies = ["sweths_violation","cowboy","reds_rocket","reds_brahmos","blackout","gap_up","gap_down","volume_breakout","options_chain_volume_breakout","abc_5_cand","abc_3_cand"]


bot_token = '1931575614:AAFhtU1xieFDqC9WAAzw15G4KdB8rdzrif4'
# chat_id = ["535162272","714628563", "1808943433","844935609","996359001","846794885","1623124565","1088161376","1612368682", "507042774","473977639","488310125","373868886","1594535460","960024014","1080210611","1710009819","1542490708","971366033","997884717","1677198821","910359081","323226633"]
chat_id = ["535162272"]

increment = 0
nifty_support = 17531
nifty_resistance = 17531
bank_nifty_support = 40777
bank_nifty_resistance = 41446


Signal_df = pd.DataFrame(columns = ["Strategy","Stock","Signal","Datetime","Value"])


engine = create_engine("mysql+pymysql://root:Mahadev_143@localhost/titania_trading")
print(engine)


con = mysql.connect(user='root', password='Mahadev_143', database='titania_trading')
cursor = con.cursor()



for idx in range(0,len(nse_data)):
    stock = nse_data.loc[idx,"Yahoo_Symbol"]
    print(stock)
    if stock == "%5ENSEI":
        opt_sql = "select distinct * from Stocks_data_5_minutes where instrumenttype = 'OPTIDX' and Stock = 'Nifty' order by Datetime asc"
        
        fut_sql = "select distinct * from Stocks_data_5_minutes where instrumenttype = 'FUTIDX' and Stock = 'Nifty' order by Datetime asc"
#         print(sql)
        live_data = pd.read_sql(opt_sql,con=engine)
        live_data.reset_index(level=0, inplace=True,drop = True)
        
        index_live_data = pd.read_sql(fut_sql,con=engine)
        index_live_data.reset_index(level=0, inplace=True,drop = True)
        
        latest_data = live_data.tail(1)
        latest_data.reset_index(level=0, inplace=True,drop = True)
        
        current_close = latest_data.loc[0,'Close']
        
        print(current_close)
        print("support :"+ str(abs(current_close - nifty_support)/nifty_support))
        print("resistance :"+ str(abs(current_close - nifty_resistance)/nifty_resistance))
    
        if abs(current_close - nifty_support)/nifty_support >= 0.001 and abs(current_close - nifty_resistance)/nifty_resistance >= 0.001:
            
            technical_sql = "select * from technical_indicator_5_minutes where stock ='Nifty' order by Datetime desc limit 1"
            technical_data = pd.read_sql(technical_sql,con=engine)
            
            for stra in range(0,len(strategies)):
                now = datetime.now(timezone("Asia/Kolkata")) 
                current_time = now.strftime("%H:%M:%S")
                if strategies[stra] == "sweths_violation":
                    sweths_violation(stock,live_data)
                elif strategies[stra] == "cowboy":
                    cowboy(stock,live_data)
                elif strategies[stra] == "reds_rocket":
                    reds_rocket(stock,live_data)
                elif strategies[stra] == "reds_brahmos":
                    reds_brahmos(stock,live_data)
                elif strategies[stra] == "blackout":
                    blackout(stock,live_data)
                elif strategies[stra] == "gap_up":
                    gap_up(stock,live_data)
                elif strategies[stra] == "gap_down":
                    gap_down(stock,live_data)
                elif strategies[stra] == "abc_5_cand":
                    abc_5_cand(stock,live_data)
                elif strategies[stra] == "abc_3_cand":
                    abc_3_cand(stock,live_data)
                elif strategies[stra] == "volume_breakout":
                    volume_breakout(stock,index_live_data)
                elif strategies[stra] == "options_chain_volume_breakout":
                    options_chain_volume_breakout(stock)
                else:
                    pass
        else:
            print("Nifty is at Support/Resistance Zone")
        
        
    elif stock == "%5ENSEBANK":
        opt_sql = "select distinct * from Stocks_data_5_minutes where instrumenttype = 'OPTIDX' and Stock = 'BankNifty' order by Datetime asc"
        fut_sql = "select distinct * from Stocks_data_5_minutes where instrumenttype = 'FUTIDX' and Stock = 'BankNifty' order by Datetime asc"
#         print(sql)
        
        live_data = pd.read_sql(opt_sql,con=engine)
        live_data.reset_index(level=0, inplace=True,drop = True)
        
        index_live_data = pd.read_sql(fut_sql,con=engine)
        index_live_data.reset_index(level=0, inplace=True,drop = True)
        
        latest_data = live_data.tail(1)
        latest_data.reset_index(level=0, inplace=True,drop = True)
        
        current_close = latest_data.loc[0,'Close']
        
        print(current_close)
        print("support :"+ str(abs(current_close - nifty_support)/nifty_support))
        print("resistance :"+ str(abs(current_close - nifty_resistance)/nifty_resistance))
        
        if abs(current_close - bank_nifty_support)/bank_nifty_support >= 0.001 and abs(current_close - bank_nifty_resistance)/bank_nifty_resistance>= 0.001:
            
            
            
            for stra in range(0,len(strategies)):
                now = datetime.now(timezone("Asia/Kolkata"))
                current_time = now.strftime("%H:%M:%S")
                if strategies[stra] == "sweths_violation":
                    sweths_violation(stock,live_data)
                elif strategies[stra] == "cowboy":
                    cowboy(stock,live_data)
                elif strategies[stra] == "reds_rocket":
                    reds_rocket(stock,live_data)
                elif strategies[stra] == "reds_brahmos":
                    reds_brahmos(stock,live_data)
                elif strategies[stra] == "blackout":
                    blackout(stock,live_data)
                elif strategies[stra] == "gap_up":
                    gap_up(stock,live_data)
                elif strategies[stra] == "gap_down":
                    gap_down(stock,live_data)
                elif strategies[stra] == "abc_5_cand":
                    abc_5_cand(stock,live_data)
                elif strategies[stra] == "abc_3_cand":
                    abc_3_cand(stock,live_data)
                elif strategies[stra] == "volume_breakout":
                    volume_breakout(stock,index_live_data)
                elif strategies[stra] == "options_chain_volume_breakout":
                    options_chain_volume_breakout(stock)
                else:
                    pass
        else:
            print("Bank Nifty is at Support/Resistance Zone")
        

bnf_technical_sql = "select * from technical_indicator_5_minutes where stock ='BankNifty' \
and cast(Execution_Date as date) = cast(Datetime as date)\
and cast(Execution_Date as date) = (select max(cast(Execution_Date as date)) from technical_indicator_5_minutes) \
order by Datetime desc"
print(bnf_technical_sql)
bnf_technical_data = pd.read_sql(bnf_technical_sql,con=engine)

nifty_technical_sql = "select * from technical_indicator_5_minutes where stock ='Nifty' \
and cast(Execution_Date as date) = cast(Datetime as date)\
and cast(Execution_Date as date) = (select max(cast(Execution_Date as date)) from technical_indicator_5_minutes) \
order by Datetime desc"
print(nifty_technical_sql)
nifty_technical_data = pd.read_sql(nifty_technical_sql,con=engine)

# bnf_technical_indicator_pcr = "select * from technical_indicator_pcr where Stock = 'BankNifty' order by Datetime desc"
# print(bnf_technical_indicator_pcr)
# bnf_technical_data_pcr = pd.read_sql(bnf_technical_indicator_pcr,con=engine)

# nifty_technical_indicator_pcr = "select * from technical_indicator_pcr where Stock = 'Nifty' order by Datetime desc"
# print(nifty_technical_indicator_pcr)
# nifty_technical_data_pcr = pd.read_sql(nifty_technical_indicator_pcr,con=engine)

if not Signal_df.empty:
    print(Signal_df)
    Signal_df['Datetime'] = pd.to_datetime(Signal_df['Datetime'])
    Signal_df['Datetime'] = pd.to_datetime(Signal_df['Datetime'], errors='coerce')
    Signal_df['Datetime'] = Signal_df['Datetime'].dt.round('5min')
    for idx in range(0,len(Signal_df)):
        print(Signal_df.loc[idx,"Datetime"])
        technical_df = pd.DataFrame()
        if Signal_df.loc[idx,"Stock"] == "%5ENSEBANK":
            print(Signal_df.loc[idx,"Datetime"])
            # print(bnf_technical_data)
            technical_df = bnf_technical_data[bnf_technical_data["Datetime"] == Signal_df.loc[idx,"Datetime"]]
            # technical_df_pcr = bnf_technical_data_pcr[bnf_technical_data_pcr["Datetime"] == Signal_df.loc[idx,"Datetime"]]
        elif Signal_df.loc[idx,"Stock"] == "%5ENSEI":
            print(Signal_df.loc[idx,"Datetime"])
            # print(nifty_technical_data)
            technical_df = nifty_technical_data[nifty_technical_data["Datetime"] == Signal_df.loc[idx,"Datetime"]]
            # technical_df_pcr = nifty_technical_data_pcr[nifty_technical_data_pcr["Datetime"] == Signal_df.loc[idx,"Datetime"]]
            
        technical_df.reset_index(level=0, inplace=True,drop = True)
        # technical_df_pcr.reset_index(level=0, inplace=True,drop = True)
        print(technical_df)
        # print(technical_df_pcr)
        Signal_df.loc[idx,"SMA_Call"] = technical_df.loc[0,"SMA_Call"]
        Signal_df.loc[idx,"RSI_Call"] = technical_df.loc[0,"RSI_Call"]
        Signal_df.loc[idx,"MACD_Call"] = technical_df.loc[0,"MACD_Call"]
        Signal_df.loc[idx,"Pivot_Call"] = technical_df.loc[0,"Pivot_Call"]
        Signal_df.loc[idx,"PCR_Call"] = technical_df.loc[0,"PCR_Call"]
        Signal_df.loc[idx,"BB_Call"] = technical_df.loc[0,"BB_Call"]
        Signal_df.loc[idx,"VWAP_Call"] = technical_df.loc[0,"VWAP_Call"]
        Signal_df.loc[idx,"SuperTrend_Call"] = technical_df.loc[0,"SuperTrend_Call"]
        Signal_df.loc[idx,"buy_probability"] = technical_df.loc[0,"buy_probability"]
        Signal_df.loc[idx,"sell_probability"] = technical_df.loc[0,"sell_probability"]
        
        
        final_prob = 0
        if(Signal_df.loc[idx,'Signal'] == 'Buy'):
            if(Signal_df.loc[idx,'SMA_Call'] == 'Buy'):
                final_prob = final_prob + 12.5
            if(Signal_df.loc[idx,'RSI_Call'] == 'Buy'):
                final_prob = final_prob + 12.5
            if(Signal_df.loc[idx,'MACD_Call'] == 'Buy'):
                final_prob = final_prob + 12.5
            if(Signal_df.loc[idx,'Pivot_Call'] == 'Buy'):
                final_prob = final_prob + 12.5
            if(Signal_df.loc[idx,'BB_Call'] == 'Buy'):
                final_prob = final_prob + 12.5
            if(Signal_df.loc[idx,'VWAP_Call'] == 'Buy'):
                final_prob = final_prob + 12.5
            if(Signal_df.loc[idx,'SuperTrend_Call'] == 'Buy'):
                final_prob = final_prob + 12.5
                
            if(Signal_df.loc[idx,'PCR_Call'] == 'Buy'):
                final_prob = final_prob + 12.5
                
        else:
            if(Signal_df.loc[idx,'SMA_Call'] == 'Sell'):
                final_prob = final_prob + 12.5
            if(Signal_df.loc[idx,'RSI_Call'] == 'Sell'):
                final_prob = final_prob + 12.5
            if(Signal_df.loc[idx,'MACD_Call'] == 'Sell'):
                final_prob = final_prob + 12.5
            if(Signal_df.loc[idx,'Pivot_Call'] == 'Sell'):
                final_prob = final_prob + 12.5
            if(Signal_df.loc[idx,'BB_Call'] == 'Sell'):
                final_prob = final_prob + 12.5
            if(Signal_df.loc[idx,'VWAP_Call'] == 'Sell'):
                final_prob = final_prob + 12.5
            if(Signal_df.loc[idx,'SuperTrend_Call'] == 'Sell'):
                final_prob = final_prob + 12.5
            if(Signal_df.loc[idx,'PCR_Call'] == 'Sell'):
                final_prob = final_prob + 12.5
                
        
        Signal_df.loc[idx,'Probability'] = final_prob

print(Signal_df)

signals_path = "/Users/apple/Downloads/Orders_Data/Signals_Df/"+ datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+".csv"

pysqldf = lambda q: sqldf(q, globals())

if not Signal_df.empty:

    if os.path.exists(signals_path):
        signals_df_check = pd.read_csv(signals_path)

        # Signal_df['Datetime'] = pd.to_datetime(Signal_df['Datetime'], infer_datetime_format=True, utc=True )
        # Signal_df['Datetime'] = Signal_df['Datetime'].dt.tz_convert('Asia/Kolkata')

        # signals_df_check['Datetime'] = pd.to_datetime(signals_df_check['Datetime'], infer_datetime_format=True, utc=True )
        # signals_df_check['Datetime'] = signals_df_check['Datetime'].dt.tz_convert('Asia/Kolkata')


        if os.path.isfile("/Users/apple/Downloads/Orders_Data/Signals_Df/"+ datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+"_testing"+".csv"):
            my_signals_test = pd.read_csv("/Users/apple/Downloads/Orders_Data/Signals_Df/"+ datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+"_testing"+".csv")

            # my_signals_test['Datetime'] = pd.to_datetime(my_signals_test['Datetime'], infer_datetime_format=True, utc=True )
            # my_signals_test['Datetime'] = my_signals_test['Datetime'].dt.tz_convert('Asia/Kolkata')

            my_signals_test = my_signals_test[["Datetime","Signal","Stock","Strategy","Value","SMA_Call","RSI_Call","MACD_Call","Pivot_Call","BB_Call","VWAP_Call","SuperTrend_Call","PCR_Call","Probability"]]

            combined_signals_df = pd.concat([my_signals_test,Signal_df]).drop_duplicates().reset_index(drop=True)
        else:
            my_signals_test = signals_df_check[["Datetime","Signal","Stock","Strategy","Value","SMA_Call","RSI_Call","MACD_Call","Pivot_Call","BB_Call","VWAP_Call","SuperTrend_Call","PCR_Call","Probability"]]

            combined_signals_df = pd.concat([my_signals_test,Signal_df]).drop_duplicates().reset_index(drop=True)


        Signal_df['Value'] = Signal_df['Value'].apply(lambda x: float(x)).round(decimals = 2)

        # print("Signal_df")
        # print(Signal_df)
        # print("my_signals_test")
        # print(my_signals_test)

        # print("combined_signals_df")
        # print(combined_signals_df)

        Signal_df = pysqldf("select t1.Strategy,t1.Stock,t1.Signal,t1.Datetime,round(COALESCE(t2.Value,t1.Value),2) as Value,COALESCE(t2.SMA_Call,t1.SMA_Call) as SMA_Call,COALESCE(t2.RSI_Call,t1.RSI_Call) as RSI_Call,COALESCE(t2.MACD_Call,t1.MACD_Call) as MACD_Call,COALESCE(t2.Pivot_Call,t1.Pivot_Call) as Pivot_Call,COALESCE(t2.BB_Call,t1.BB_Call) as BB_Call,COALESCE(t2.VWAP_Call,t1.VWAP_Call) as VWAP_Call,COALESCE(t2.SuperTrend_Call,t1.SuperTrend_Call) as SuperTrend_Call,COALESCE(t2.PCR_Call,t1.PCR_Call) as PCR_Call,round(COALESCE(t2.Probability,t1.Probability),2) as Probability from Signal_df t1 left join signals_df_check t2 on t1.Strategy = t2.Strategy and t1.Stock = t2.Stock and t1.Datetime = t2.Datetime")

        # Signal_df['Datetime'] = pd.to_datetime(Signal_df['Datetime']).dt.tz_localize('Asia/Kolkata')

        # combined_signals_df['Datetime'] = pd.to_datetime(combined_signals_df['Datetime']).dt.tz_convert('Asia/Kolkata')

        combined_signals_df = combined_signals_df[["Datetime","Signal","Stock","Strategy","Value","SMA_Call","RSI_Call","MACD_Call","Pivot_Call","BB_Call","VWAP_Call","SuperTrend_Call","PCR_Call","Probability"]]
        combined_signals_df.to_csv("/Users/apple/Downloads/Orders_Data/Signals_Df/"+ datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+"_testing"+".csv",sep=',',header = True,index=False)

        # print(Signal_df)

        Signal_df.to_csv(signals_path,sep=',',header = True)

        # print("file exists")
    else:
        # print("File not exists")
        Signal_df.to_csv(signals_path,sep=',',header = True)



print(Signal_df)


if not Signal_df.empty:
    Signal_df = Signal_df.sort_values(by=['Datetime'], ascending=True)
    Signal_df.reset_index(inplace = True, drop = True)
    
    Signal_df['Value'] = Signal_df['Value'].apply(lambda x: float(x)).round(decimals = 2)
    
    stop_loss = 1
    target = 1
    
    Capital = 10000
    
    Signal_df['StopLoss'] = np.where(Signal_df['Signal'] == "Buy",Signal_df['Value']-((stop_loss*Signal_df['Value'])/100),(stop_loss*Signal_df['Value'])/100+Signal_df['Value'])
    
    Signal_df["Target"] = np.where(Signal_df['Signal'] == "Buy",Signal_df['Value']+((target*Signal_df['Value'])/100),Signal_df['Value'] - (target*Signal_df['Value'])/100)
    
#     Signal_df['StopLoss']=Signal_df['Signal'].apply(lambda x: Signal_df['Value']-((stop_loss*Signal_df['Value'])/100) if x == "Buy" else (stop_loss*Signal_df['Value'])/100+Signal_df['Value'])
    
#     Signal_df["StopLoss"] = Signal_df['Value'].astype(int)-((stop_loss*Signal_df['Value'].astype(int))/100) if Signal_df['Signal'] == "Buy" else (stop_loss*Signal_df['Value'].astype(int))/100+Signal_df['Value'].astype(int)
    
#     Signal_df["Target"] = Signal_df['Value'].astype(int)+((target*Signal_df['Value'].astype(int))/100) if Signal_df['Signal'] == "Buy" else Signal_df['Value'].astype(int) - (target*Signal_df['Value'].astype(int))/100
    
    Signal_df['Qty'] = abs((20/100)*Capital/(Signal_df['Target'].astype(int) - Signal_df['StopLoss'].astype(int))).round(decimals = 0)
    
    Signal_df['Spot_Price'] = 0
    # Signal_df['expiry'] = "2022-01-20"
    Signal_df['expiry'] = expiry_date_char
    Signal_df['Strike_Buy_Price'] = 0
    Signal_df['premium_StopLoss'] = 0
    Signal_df['premium_Target'] = 0
    Signal_df['lotsize'] = 0
    Signal_df['premium_Qty'] = 0
    Signal_df['historic_profit'] = 0
    Signal_df['current_script'] = ""
    Signal_df['token'] = 0
    
    
    Signal_df['exec_rnk'] = Signal_df['Datetime'].rank(ascending=True)
    Signal_df['order_place'] = 0
    Signal_df['order_id'] = 0
    Signal_df['target_order_id'] = 0

    Signal_df['stop_loss_order_id'] = 0
    Signal_df['robo_order_id'] = 0
    Signal_df['cancel_order_id'] = 0
    Signal_df['final_order_id'] = 0
    Signal_df['conclusion'] = ""
  
    Signal_df['execution_time'] = now = datetime.now(timezone("Asia/Kolkata"))
    Signal_df['target_hit']  = ""
    Signal_df['avg_buy_price']  = ""
    Signal_df['avg_sell_price']  = ""
    Signal_df['avg_qty']  = ""
    Signal_df['adjusted_target']  = ""
    Signal_df['adjusted_stoploss']  = ""


if not Signal_df.empty:
    Signal_df.reset_index(inplace = True, drop = True)


    obj=SmartConnect(api_key="LPUVlRxd")
    totp = pyotp.TOTP("ILBHGZB6KNXHZALKHZJN2A7PPI")
    print("pyotp",totp.now())
    attempts = 5
    while attempts > 0:
        attempts = attempts-1
        data = obj.generateSession("J95213","start@123", totp.now())
        if data['status']:
            break
        # tt.sleep(2)

    print(data)

    # obj=SmartConnect(api_key="LPUVlRxd")
    # # obj = SmartConnect(api_key = "bflJXkmS")

    # time.sleep(0.5)

    # data = obj.generateSession("J95213","start@123")

    for i in range(0,len(Signal_df)):
        side_dir = "CE" if Signal_df.loc[i,"Signal"] == "Buy" else "PE"
        
        if Signal_df.loc[i,"Stock"] == "%5ENSEBANK":
             #### Get the spot price value at the money
            Signal_df.loc[i,"Spot_Price"] = ((Signal_df.loc[i,"Value"] + (100 - Signal_df.loc[i,"Value"] % 100)) if Signal_df.loc[i,"Value"] % 100 > 50 else (Signal_df.loc[i,"Value"] - Signal_df.loc[i,"Value"] % 100)).round(decimals=0)

            #### Get the two steps in the money
            Signal_df.loc[i,"Spot_Price"] = (Signal_df.loc[i,"Spot_Price"] - 200 if side_dir == "CE" else Signal_df.loc[i,"Spot_Price"] + 200)

            # Signal_df['Spot_Price'] = Signal_df['Spot_Price'].apply(lambda x: float(x)).round(decimals = 0)
            
            # print(str(Signal_df.loc[i,"Spot_Price"]))
            # print(side_dir)
            # lookup_symbol = "BANKNIFTY" + "20JAN22" + str(Signal_df.loc[i,"Spot_Price"])[:len(str(Signal_df.loc[i,"Spot_Price"])) - 2] + side_dir
            lookup_symbol = "BANKNIFTY" + str(expiry_date_month) + str(Signal_df.loc[i,"Spot_Price"]) + side_dir

        else:
            #### Get the spot price value at the money
            Signal_df.loc[i,"Spot_Price"] = ((Signal_df.loc[i,"Value"] + (50 - Signal_df.loc[i,"Value"] % 50)) if Signal_df.loc[i,"Value"] % 50 > 25 else (Signal_df.loc[i,"Value"] - Signal_df.loc[i,"Value"] % 50)).round(decimals=0)

            #### Get the two steps in the money
            Signal_df.loc[i,"Spot_Price"] = Signal_df.loc[i,"Spot_Price"] - 100 if side_dir == "CE" else Signal_df.loc[i,"Spot_Price"] + 100


            ##### Reference sheet : https://docs.google.com/spreadsheets/d/1xG_hKRnOupbtPQb6ptAeh0coLuxg7FmhUno__vQ-wuA/edit#gid=0

            if Signal_df.loc[i,"Strategy"] == "Cowboy":
                ## In the money
                Signal_df.loc[i,"Spot_Price"] = Signal_df.loc[i,"Spot_Price"] - 100 if side_dir == "CE" else Signal_df.loc[i,"Spot_Price"] + 100
            elif Signal_df.loc[i,"Strategy"] == "Sweths Violation":

                ## Out of Money
                Signal_df.loc[i,"Spot_Price"] = Signal_df.loc[i,"Spot_Price"] + 100 if side_dir == "CE" else Signal_df.loc[i,"Spot_Price"] - 100
            elif Signal_df.loc[i,"Strategy"] == "Reds Rocket":

                ## At the money
                Signal_df.loc[i,"Spot_Price"] = Signal_df.loc[i,"Spot_Price"] if side_dir == "CE" else Signal_df.loc[i,"Spot_Price"]
            elif Signal_df.loc[i,"Strategy"] == "Reds Brahmos":

                ## Out of Money
                Signal_df.loc[i,"Spot_Price"] = Signal_df.loc[i,"Spot_Price"] + 100 if side_dir == "CE" else Signal_df.loc[i,"Spot_Price"] - 100
            elif Signal_df.loc[i,"Strategy"] == "Gap_up":

                ## Out of Money
                Signal_df.loc[i,"Spot_Price"] = Signal_df.loc[i,"Spot_Price"] + 100 if side_dir == "CE" else Signal_df.loc[i,"Spot_Price"] - 100
            elif Signal_df.loc[i,"Strategy"] == "Blackout":

                ## At the money
                Signal_df.loc[i,"Spot_Price"] = Signal_df.loc[i,"Spot_Price"] if side_dir == "CE" else Signal_df.loc[i,"Spot_Price"]
            else :

                ## Go with ATM
                Signal_df.loc[i,"Spot_Price"] = (Signal_df.loc[i,"Spot_Price"] if side_dir == "CE" else Signal_df.loc[i,"Spot_Price"])



            # lookup_symbol = "NIFTY" + "20JAN22" + str(Signal_df.loc[i,"Spot_Price"])[:len(str(Signal_df.loc[i,"Spot_Price"])) - 2] + side_dir
            lookup_symbol = "NIFTY" + str(expiry_date_month) + str(Signal_df.loc[i,"Spot_Price"]) + side_dir

         
        
#         Signal_df[i,"expiry"] = "2021-12-16"
        # print("lookup symbok")
        print(lookup_symbol)
        current_script = angel_script[angel_script["symbol"] == lookup_symbol]
        
        
        current_script.reset_index(inplace = True, drop = True)

        print("current_script")
        print(current_script)

        
        token = current_script.loc[0,"token"]

        print(token)
        
        hist = {"exchange":"NFO",
        "symboltoken":token,
        "interval":"ONE_MINUTE",
        "fromdate":now.strftime("%Y-%m-%d")+ " 09:15",
        "todate":now.strftime("%Y-%m-%d")+ " 15:30"
       }
        # print(hist)
        resp = obj.getCandleData(hist)
        # print(resp)
        time.sleep(0.5)
        # print(resp)
        hist_df = pd.DataFrame.from_dict(resp['data']) 
        hist_df.columns = ['Datetime','Open', 'High','Low', 'Close','Volume']
        
        hist_df['Datetime'] = pd.to_datetime(hist_df['Datetime'], format='%Y-%m-%d %H:%M:%S')

        print(Signal_df.loc[i,'Datetime'])
        print(hist_df.tail(1))
        
        current_data = hist_df.loc[hist_df['Datetime'] == Signal_df.loc[i,'Datetime']]
        
        current_data.reset_index(inplace = True, drop = True)

        print(current_data)
        
        ltp = current_data.loc[0,"Close"]
        
        Signal_df.loc[i,"Strike_Buy_Price"] = ltp
        
        stop_loss = 10
        
        target = 10
        
        risk_on_capital = 0.05
        
        capital = 10000
        
        lot_size = Capital * risk_on_capital / ltp
        
        Signal_df.loc[i,"premium_StopLoss"] = round(Signal_df.loc[i,"Strike_Buy_Price"]-((stop_loss*Signal_df.loc[i,"Strike_Buy_Price"])/100),2)
        Signal_df.loc[i,"premium_Target"] = round(Signal_df.loc[i,"Strike_Buy_Price"]+((target*Signal_df.loc[i,"Strike_Buy_Price"])/100),2)
        
        if Signal_df.loc[i,"Stock"] == "%5ENSEBANK":
            
            if(Signal_df.loc[i,"Strategy"] == "Cowboy"):
                bk_test_data = pd.read_csv("~/Downloads/Backtesting_Aplication/data/Cowboy_BankNifty_Backtest.csv")
            elif(Signal_df.loc[i,"Strategy"] == "Sweths Violation"):
                bk_test_data = pd.read_csv("~/Downloads/Backtesting_Aplication/data/Sweths_Violation_BankNifty_Backtest.csv")
            elif(Signal_df.loc[i,"Strategy"] == "Reds Rocket"):
                bk_test_data = pd.read_csv("~/Downloads/Backtesting_Aplication/data/Reds_Rocket_BankNifty_Backtest.csv")
            elif(Signal_df.loc[i,"Strategy"] == "Reds Brahmos"):
                bk_test_data = pd.read_csv("~/Downloads/Backtesting_Aplication/data/Reds_Brahmos_BankNifty_Backtest.csv")
            elif(Signal_df.loc[i,"Strategy"] == "Gap_up"):
                bk_test_data = pd.read_csv("~/Downloads/Backtesting_Aplication/data/Gapup_BankNifty_Backtest.csv")
            else:
                bk_test_data = pd.read_csv("~/Downloads/Backtesting_Aplication/data/Gapup_BankNifty_Backtest.csv")        
                
             
            
            #### After backtesting Nifty Move this to below
            bk_test_data['StartTime'] = pd.to_datetime(bk_test_data['StartTime'], errors='coerce')
            bk_test_data['month'] = bk_test_data['StartTime'].dt.month
            bk_test_data['year'] = bk_test_data['StartTime'].dt.year
            bk_test_data['hour'] = bk_test_data['StartTime'].dt.hour
            bk_test_data['wday'] = bk_test_data['StartTime'].dt.day_name()


            bk_test_data = pd.DataFrame(bk_test_data)

            final_bk_data = bk_test_data.groupby(["hour", "wday"]).agg({"price_diff": "mean"}).reset_index()

#             print(final_bk_data)



            cr_date = datetime.strptime(Signal_df.loc[i,"Datetime"], '%Y-%m-%d %H:%M:%S.%f')
            print(cr_date)
            current_hour = cr_date.hour 
            current_week = cr_date.strftime("%A")

            print(current_hour)
            print(current_week)

            final_bk_data = final_bk_data.loc[(final_bk_data['hour'] == current_hour) & (final_bk_data['wday'] == current_week)]

#             print(final_bk_data)

            if not final_bk_data.empty:
                final_bk_data.reset_index(inplace = True, drop = True)

                if final_bk_data.loc[0,'price_diff'] > 0:
                    Signal_df.loc[i,"lotsize"] = 25*round(Signal_df.loc[i,"Qty"],0)
                    Signal_df.loc[i,"premium_Qty"] = round((abs((20/100)*Capital/( Signal_df.loc[i,"premium_Target"] - Signal_df.loc[i,"premium_StopLoss"]))/25),0)
                    Signal_df.loc[i,"historic_profit"] = 1
                else:
                    Signal_df.loc[i,"lotsize"] = 25
                    Signal_df.loc[i,"premium_Qty"] = round((abs((20/100)*Capital/( Signal_df.loc[i,"premium_Target"] - Signal_df.loc[i,"premium_StopLoss"]))/25),0)
                    Signal_df.loc[i,"historic_profit"] = -1
            else:
                Signal_df.loc[i,"lotsize"] = 25
                Signal_df.loc[i,"premium_Qty"] = round((abs((20/100)*Capital/( Signal_df.loc[i,"premium_Target"] - Signal_df.loc[i,"premium_StopLoss"]))/25),0)
                Signal_df.loc[i,"historic_profit"] = -1
        
        elif Signal_df.loc[i,"Stock"] == "%5ENSEI":
            if(Signal_df.loc[i,"Strategy"] == "Cowboy"):
                bk_test_data = pd.read_csv("~/Downloads/Backtesting_Aplication/data/Cowboy_Nifty_Backtest.csv")
            elif(Signal_df.loc[i,"Strategy"] == "Sweths Violation"):
                bk_test_data = pd.read_csv("~/Downloads/Backtesting_Aplication/data/Sweths_Violation_Nifty_Backtest.csv")
            elif(Signal_df.loc[i,"Strategy"] == "Reds Rocket"):
                bk_test_data = pd.read_csv("~/Downloads/Backtesting_Aplication/data/Reds_Rocket_Nifty_Backtest.csv")
            elif(Signal_df.loc[i,"Strategy"] == "Reds Brahmos"):
                bk_test_data = pd.read_csv("~/Downloads/Backtesting_Aplication/data/Reds_Brahmos_Nifty_Backtest.csv")
            elif(Signal_df.loc[i,"Strategy"] == "Gap_up"):
                bk_test_data = pd.read_csv("~/Downloads/Backtesting_Aplication/data/Gapup_Nifty_Backtest.csv")
            else:
                bk_test_data = pd.read_csv("~/Downloads/Backtesting_Aplication/data/Gapup_Nifty_Backtest.csv") 

             #### After backtesting Nifty Move this to below
            bk_test_data['StartTime'] = pd.to_datetime(bk_test_data['StartTime'], errors='coerce')
            bk_test_data['month'] = bk_test_data['StartTime'].dt.month
            bk_test_data['year'] = bk_test_data['StartTime'].dt.year
            bk_test_data['hour'] = bk_test_data['StartTime'].dt.hour
            bk_test_data['wday'] = bk_test_data['StartTime'].dt.day_name()


            bk_test_data = pd.DataFrame(bk_test_data)

            final_bk_data = bk_test_data.groupby(["hour", "wday"]).agg({"price_diff": "mean"}).reset_index()

#             print(final_bk_data)


            cr_date = datetime.strptime(Signal_df.loc[i,"Datetime"], '%Y-%m-%d %H:%M:%S.%f')
            current_hour = cr_date.hour 
            current_week = cr_date.strftime("%A")

#             print(current_hour)
#             print(current_week)
            # print(final_bk_data['wday'])

            final_bk_data = final_bk_data.loc[(final_bk_data['hour'] == current_hour) & (final_bk_data['wday'] == current_week)]

#             print(final_bk_data)

            if not final_bk_data.empty:
                final_bk_data.reset_index(inplace = True, drop = True)

                if final_bk_data.loc[0,'price_diff'] > 0:
                    Signal_df.loc[i,"lotsize"] = 50*round(Signal_df.loc[i,"Qty"],0)
                    Signal_df.loc[i,"premium_Qty"] = round((abs((20/100)*Capital/( Signal_df.loc[i,"premium_Target"] - Signal_df.loc[i,"premium_StopLoss"]))/50),0)
                    Signal_df.loc[i,"historic_profit"] = 1
                else:
                    Signal_df.loc[i,"lotsize"] = 50
                    Signal_df.loc[i,"premium_Qty"] = round((abs((20/100)*Capital/( Signal_df.loc[i,"premium_Target"] - Signal_df.loc[i,"premium_StopLoss"]))/50),0)
                    Signal_df.loc[i,"historic_profit"] = -1
            else:
                Signal_df.loc[i,"lotsize"] = 50
                Signal_df.loc[i,"premium_Qty"] = round((abs((20/100)*Capital/( Signal_df.loc[i,"premium_Target"] - Signal_df.loc[i,"premium_StopLoss"]))/50),0)
                Signal_df.loc[i,"historic_profit"] = -1


        else:
            ###### Complete the Backtest for Nifty
            print("Back test pending")
            
            Signal_df.loc[i,"lotsize"] = 50
            Signal_df.loc[i,"premium_Qty"] = round((abs((20/100)*Capital/( Signal_df.loc[i,"premium_Target"] - Signal_df.loc[i,"premium_StopLoss"]))/50),0)
            Signal_df.loc[i,"historic_profit"] = -1
            
        
        Signal_df.loc[i,"current_script"] = lookup_symbol
        Signal_df.loc[i,"token"] = token
        Signal_df.loc[i,"conclusion"] = "New Order"


        signals_path = "/Users/apple/Downloads/Reddy_Stocks_Application/data/"+ datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d')+"_Raw_Signals_Data.csv"

    if os.path.isfile(signals_path):
        print("Signal path exists")
        all_signals_df = pd.read_csv(signals_path)
        all_signals_df = all_signals_df.loc[:, ~all_signals_df.columns.str.contains('^Unnamed')]
        Signal_df = Signal_df.loc[:, ~Signal_df.columns.str.contains('^Unnamed')]
        all_signals_df = all_signals_df.append(Signal_df)

        all_signals_df = all_signals_df.groupby(['Strategy','Stock','Signal',   'Datetime', 'Value',    'SMA_Call', 'RSI_Call', 'MACD_Call',    'Pivot_Call',   'BB_Call','VWAP_Call','SuperTrend_Call',  'PCR_Call', 'Probability',  'StopLoss', 'Target',   'Qty',  'Spot_Price',   'expiry',   'Strike_Buy_Price', 'premium_StopLoss', 'premium_Target',   'lotsize',  'premium_Qty',  'historic_profit',  'current_script',   'token',    'exec_rnk', 'order_place',  'order_id', 'target_order_id',  'stop_loss_order_id',   'robo_order_id',    'cancel_order_id',  'final_order_id',   'conclusion',    'target_hit',   'avg_buy_price',    'avg_sell_price',   'avg_qty',  'adjusted_target',  'adjusted_stoploss'])['execution_time'].min().reset_index()

        # all_signals_df.drop_duplicates(subset=all_signals_df.columns.difference(['execution_time']))
        all_signals_df.to_csv("/Users/apple/Downloads/Reddy_Stocks_Application/data/"+ datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d')+'_Raw_Signals_Data.csv', index=False)
    else:
        print("Signal path doesn't exist")
        Signal_df = Signal_df.loc[:, ~Signal_df.columns.str.contains('^Unnamed')]
        print(Signal_df.columns)
        print(Signal_df)
        Signal_df.to_csv("/Users/apple/Downloads/Reddy_Stocks_Application/data/"+ datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d')+'_Raw_Signals_Data.csv', index=False)


merged_data = pd.DataFrame()
order_alerts = pd.DataFrame()

sql = "select * from titania_trading.client_details where client_id in ('J95213','S1604557','G304915','K256027')"
print(sql)
client_data = pd.read_sql(sql,con=engine)

for i in range(0,len(client_data)):
    print("Running for  {}".format(str(client_data.loc[i,'client_name'])))
    try:
        user_id = client_data.loc[i,"client_id"]
        obj=SmartConnect(api_key=client_data.loc[i,"client_api_key"])
        totp = pyotp.TOTP(client_data.loc[i,"totp_code"])
        print("pyotp",totp.now())
        attempts = 5
        while attempts > 0:
            attempts = attempts-1
            data = obj.generateSession(client_data.loc[i,"client_id"],client_data.loc[i,"client_password"], totp.now())
            if data['status']:
                break

        print(data)

        order_path = "/Users/apple/Downloads/Orders_Data/" + client_data.loc[i,'client_id'] + "/Options_Order/Python_"+ str(datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+".csv")

        if os.path.isfile(order_path):
            print("order path exists")
            temp_signals_df =pd.read_csv(order_path)
            temp_signals_df['execution_time'] = pd.to_datetime(temp_signals_df['execution_time'])
            temp_signals_df = temp_signals_df[temp_signals_df['Datetime'].notna()]
        else:
            temp_signals_df = pd.DataFrame(columns = ["SNo", "Strategy","Stock","Datetime","Value","SMA_Call","RSI_Call","MACD_Call","Pivot_Call","BB_Call","VWAP_Call","SuperTrend_Call","PCR_Call","Probability","current_script","Signal", "StopLoss","Target","Qty", "Spot_Price", "expiry","Strike_Buy_Price","premium_StopLoss", "premium_Target", "lotsize","premium_Qty","historic_profit", "token", "exec_rnk","order_id","target_order_id","stop_loss_order_id","cancel_order_id","final_order_id","robo_order_id","conclusion","execution_time","target_hit","avg_buy_price","avg_sell_price","avg_qty","adjusted_target","adjusted_stoploss","order_place"])

        if not Signal_df.empty:
            if not temp_signals_df.empty:
                temp_signals_df['rnk'] = temp_signals_df.groupby(['Strategy','Stock','Datetime'])['execution_time'].rank(ascending=False)
                temp_signals_df = pd.DataFrame(temp_signals_df)
                temp_signals_df = temp_signals_df.loc[temp_signals_df['rnk']==1]
                temp_signals_df.reset_index(inplace = True, drop = True)
            else:
                temp_signals_df['rnk'] = 1

            temp_signals_df['execution_time'] = pd.to_datetime(temp_signals_df['execution_time'])

            Signal_df = Signal_df[~Signal_df.index.duplicated()]
            temp_signals_df = temp_signals_df[~temp_signals_df.index.duplicated()]
            temp_signals_df = temp_signals_df.iloc[: , 1:]

            Signal_df[['Strategy', 'Stock', 'current_script','SMA_Call','RSI_Call','MACD_Call','Pivot_Call','BB_Call','VWAP_Call','SuperTrend_Call','PCR_Call']] = Signal_df[['Strategy', 'Stock', 'current_script','SMA_Call','RSI_Call','MACD_Call','Pivot_Call','BB_Call','VWAP_Call','SuperTrend_Call','PCR_Call']].astype(str)
            temp_signals_df[['Strategy', 'Stock', 'current_script','SMA_Call','RSI_Call','MACD_Call','Pivot_Call','BB_Call','VWAP_Call','SuperTrend_Call','PCR_Call']] = temp_signals_df[['Strategy', 'Stock', 'current_script','SMA_Call','RSI_Call','MACD_Call','Pivot_Call','BB_Call','VWAP_Call','SuperTrend_Call','PCR_Call']].astype(str)
            temp_signals_df[['historic_profit', 'order_id', 'target_order_id','stop_loss_order_id', 'cancel_order_id', 'final_order_id','robo_order_id','rnk',]] = temp_signals_df[['historic_profit', 'order_id', 'target_order_id','stop_loss_order_id', 'cancel_order_id', 'final_order_id','robo_order_id','rnk']].astype(str)
            temp_signals_df[['Value','Probability', 'StopLoss', 'Target','Qty','Spot_Price','Strike_Buy_Price','premium_StopLoss','premium_Target','lotsize','premium_Qty','exec_rnk']] = temp_signals_df[['Value','Probability', 'StopLoss', 'Target','Qty','Spot_Price','Strike_Buy_Price','premium_StopLoss','premium_Target','lotsize','premium_Qty','exec_rnk']].astype(float)

            Signal_df["rnk"] = 1

            Signal_df = Signal_df[["Strategy","Stock","Datetime","Value","SMA_Call","RSI_Call","MACD_Call","Pivot_Call","BB_Call","VWAP_Call","SuperTrend_Call","PCR_Call","Probability","current_script","Signal", "StopLoss","Target","Qty", "Spot_Price", "expiry","Strike_Buy_Price","premium_StopLoss", "premium_Target", "lotsize","premium_Qty","historic_profit", "token", "exec_rnk","order_id","target_order_id","stop_loss_order_id","cancel_order_id","final_order_id","robo_order_id","conclusion","execution_time","target_hit","avg_buy_price","avg_sell_price","avg_qty","adjusted_target","adjusted_stoploss","rnk","order_place"]]

            merged_data = pysqldf('''select COALESCE(tsd.Strategy,sd.Strategy) as Strategy,
                                  COALESCE(tsd.Stock,sd.Stock) as Stock,
                                  COALESCE(tsd.Signal,sd.Signal) as Signal,
                                  COALESCE(sd.Datetime,tsd.Datetime) as Datetime,
                                  COALESCE(tsd.Value,sd.Value) as Value,
                                  COALESCE(tsd.SMA_Call,sd.SMA_Call) as SMA_Call,
                                  COALESCE(tsd.RSI_Call,sd.RSI_Call) as RSI_Call,
                                  COALESCE(tsd.MACD_Call,sd.MACD_Call) as MACD_Call,
                                  COALESCE(sd.Pivot_Call,tsd.Pivot_Call) as Pivot_Call,
                                  COALESCE(tsd.BB_Call,sd.BB_Call) as BB_Call,
                                  COALESCE(tsd.VWAP_Call,sd.VWAP_Call) as VWAP_Call,
                                  COALESCE(tsd.SuperTrend_Call,sd.SuperTrend_Call) as SuperTrend_Call,
                                  
                                  COALESCE(sd.PCR_Call,tsd.PCR_Call) as PCR_Call,
                                  COALESCE(tsd.Probability,sd.Probability) as Probability,
                                  COALESCE(tsd.StopLoss,sd.StopLoss) as StopLoss,
                                  COALESCE(tsd.Target,sd.Target) as Target,
                                  COALESCE(tsd.Qty,sd.Qty) as Qty,
                                  COALESCE(tsd.Spot_Price,sd.Spot_Price) as Spot_Price,
                                  COALESCE(tsd.expiry,sd.expiry) as expiry,
                                  COALESCE(sd.Strike_Buy_Price,tsd.Strike_Buy_Price) as Strike_Buy_Price,
                                  COALESCE(sd.premium_StopLoss,tsd.premium_StopLoss) as premium_StopLoss,
                                  COALESCE(sd.premium_Target,tsd.premium_Target) as premium_Target,
                                  COALESCE(sd.lotsize,tsd.lotsize) as lotsize,
                                  COALESCE(sd.premium_Qty,tsd.premium_Qty) as premium_Qty,
                                  COALESCE(sd.historic_profit,tsd.historic_profit) as historic_profit,
                                  COALESCE(tsd.token,sd.token) as token,
                                  COALESCE(tsd.current_script,sd.current_script) as current_script,
                                  COALESCE(tsd.exec_rnk,sd.exec_rnk) as exec_rnk,
                                  COALESCE(cast(tsd.order_place as numeric),cast(sd.order_place as numeric) )as order_place,
                                  COALESCE(cast(tsd.order_id as numeric),cast(sd.order_id as numeric)) as order_id,
                                  COALESCE(cast(tsd.target_order_id as numeric),cast(sd.target_order_id as numeric)) as target_order_id,
                                  COALESCE(cast(tsd.stop_loss_order_id as numeric),cast(sd.stop_loss_order_id as numeric)) as stop_loss_order_id,
                                  COALESCE(cast(tsd.cancel_order_id as numeric),cast(sd.cancel_order_id as numeric)) as cancel_order_id,
                                  COALESCE(cast(tsd.final_order_id as numeric),cast(sd.final_order_id as numeric)) as final_order_id,
                                  COALESCE(cast(tsd.robo_order_id as numeric),cast(sd.robo_order_id as numeric)) as robo_order_id,
                                  
                                  COALESCE(tsd.conclusion,sd.conclusion) as conclusion,
                                  COALESCE(tsd.execution_time,sd.execution_time) as execution_time,
                                  COALESCE(tsd.target_hit,sd.target_hit) as target_hit,
                                  COALESCE(tsd.avg_buy_price,sd.avg_buy_price) as avg_buy_price,
                                  COALESCE(tsd.avg_sell_price,sd.avg_sell_price) as avg_sell_price,
                                  COALESCE(tsd.avg_qty,sd.avg_qty) as avg_qty,
                                  COALESCE(tsd.adjusted_target,sd.adjusted_target) as adjusted_target,
                                  COALESCE(tsd.adjusted_stoploss,sd.adjusted_stoploss) as adjusted_stoploss

                from Signal_df sd
                left join (select * from temp_signals_df where cast(rnk as numeric) =1) tsd on sd.Strategy = tsd.Strategy and sd.Stock = tsd.Stock and sd.current_script = tsd.current_script and cast(sd.Datetime as datetime) = cast(tsd.Datetime as datetime)''')

            merged_data['Datetime'] = pd.to_datetime(merged_data['Datetime'])
            merged_data['Datetime'] = pd.to_datetime(merged_data['Datetime'], format='%Y-%m-%d %H:%M:%S')
            merged_data.to_csv("~/Downloads/Orders_Data/" + str(user_id) +"/merged_data_testing.csv", sep=',', header=True)

            order_resp = obj.orderBook()
            order_data = pd.DataFrame.from_dict(order_resp['data'])

            if not order_data.empty:
                order_data["orderid"] = order_data["orderid"].astype(str)

            for idx in range(0,len(merged_data)):
                order_alerts = order_alerts.append(merged_data.loc[idx,], ignore_index = True)
                ind_time = datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")
                ind_time = datetime.strptime(ind_time, '%Y-%m-%d %H:%M:%S')

                time_delta = ind_time - merged_data.loc[idx,'Datetime']
                time_delta_mins = time_delta.total_seconds()/60

                print(time_delta_mins)

                if time_delta_mins <= 30:
                	current_time = merged_data.loc[idx,'Datetime'].strftime('%H:%M:%S')

                	if current_time <= "15:00:00":
                		if merged_data.loc[idx,'order_id'] == 0:
                			print("Placing the limit order")
                			order_alerts = order_alerts.append(merged_data.loc[idx,], ignore_index = True)
                			place_limit_order(merged_data,idx,user_id,obj)

                else:
                	prev_order = merged_data.iloc[idx].to_frame().T
                	prev_order.reset_index(inplace = True, drop = True)
                	prev_order['order_id'] = prev_order['order_id'].replace(np.nan, 0)
                	prev_order_id = prev_order.loc[0,"order_id"]
                	prev_target_order_id = prev_order.loc[0,"target_order_id"]
                	prev_final_order_id = prev_order.loc[0,"final_order_id"]

                	if float(prev_order_id) > 1:
                		print("before previous order check")
                		print(prev_order_id)

                		if not order_data.empty:
                			last_order = order_data.loc[order_data["orderid"] == str(prev_order_id)]

                			ind_time = datetime.now(timezone("Asia/Kolkata"))

                			print(type(merged_data.loc[idx,'Datetime']))
                			print(merged_data.loc[idx,'Datetime'])
                			print(ind_time)
                			print(datetime.timestamp(merged_data.loc[idx,'Datetime']))
                			print(type(ind_time))

                			naive = ind_time.replace(tzinfo=None)

                			time_delta = naive - merged_data.loc[idx,'Datetime']
                			print("time_delta_mins is : ")
                			time_delta_mins = time_delta.total_seconds()/60

                			print(time_delta_mins)

                			if time_delta_mins >= 30:
                				if not last_order.empty:
                					last_order.reset_index(inplace = True, drop = True)

                					if last_order.loc[0,"status"] == "open":
                						prev_order_id_details = merged_data.loc[idx,"order_id"]
                						print("before cancelling order")
                						print("cancelling the limit order as the time passed : ",str(prev_order_id_details))

                						time.sleep(0.5)

                						cancel_id = obj.cancelOrder(str(prev_order_id_details),variety="NORMAL")

                						merged_data.loc[(i),"cancel_order_id"] = cancel_id
                						merged_data.loc[(i),"final_order_id"] = cancel_id
                						merged_data.loc[(i),"conclusion"] = "Cancelled as Limit not achieved"




    except Exception as e:
        print("Exception occured {}".format(e))
    finally:
        if not Signal_df.empty:
            if not merged_data.empty:
                merged_data['execution_time'] = datetime.now(timezone("Asia/Kolkata"))
                file_path = "/Users/apple/Downloads/Orders_Data/" + str(user_id) + "/Options_Order/Python_" + datetime.now().strftime("%Y-%m-%d")+".csv"
                second_file_path = "/Users/apple/Downloads/Reddy_Stocks_Application/data/Nifty_Indices_Python_Trading_Logs.csv"

                if os.path.exists(file_path):
                    merged_data.to_csv(file_path, mode='a',sep=',', header=False)
                else:
                    merged_data.to_csv(file_path,sep=',')

                merged_data.to_csv(second_file_path, mode='a',sep=',', header=True)




end_time = datetime.now(timezone("Asia/Kolkata"))

print(end_time)

print('Duration: {}'.format(end_time - start_time))
