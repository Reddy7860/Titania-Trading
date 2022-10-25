import pandas as pd
import os
from datetime import datetime,timedelta
from smartapi import SmartConnect
from pandasql import sqldf
import math
from pandas.io.json import json_normalize
import json
import numpy as np
import yfinance as yf
import pandas_ta as ta
import time
import datetime as dt
from pytz import timezone 

import numpy as np
from matplotlib import pyplot as plt
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.arima.model import ARIMA
from pmdarima.arima import auto_arima
from pandas.plotting import register_matplotlib_converters
from sklearn.metrics import mean_squared_error, mean_absolute_error
register_matplotlib_converters()

from pymongo import MongoClient
from pymongo.server_api import ServerApi
import pymongo


import mysql.connector as mysql
import pymysql
from sqlalchemy.engine import result
import sqlalchemy
from sqlalchemy import create_engine, MetaData,\
Table, Column, Numeric, Integer, VARCHAR, update, delete

from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://root:Mahadev_143@localhost/titania_trading")
print(engine)


con = mysql.connect(user='root', password='Mahadev_143', database='titania_trading')
cursor = con.cursor()


server_api = ServerApi('1')

client = MongoClient("mongodb+srv://Titania:Mahadev@cluster0.zq3w2cn.mongodb.net/titania_trading?ssl=true&ssl_cert_reqs=CERT_NONE", server_api=server_api)

db = client.titania_trading

db = client["titania_trading"]

list_of_collections = db.list_collection_names()

print(list_of_collections)


def calculate_classic_pivots(data):
    pivot_data = data.tail(1)
    pivot_data.reset_index(level=0, inplace=True)
    pivot_point = (pivot_data.loc[0,'High'] + pivot_data.loc[0,'Low'] + pivot_data.loc[0,'Close'])/3

    pivot_bc = (pivot_data.loc[0,'High'] + pivot_data.loc[0,'Low'])/2
    pivot_tc = 2* pivot_point - pivot_bc

    print(pivot_bc)
    print(pivot_tc)
    
    classic_support_1 = round((2*pivot_point) - pivot_data.loc[0,'High'],2)
    
    classic_resistance_1 = round((2*pivot_point) - pivot_data.loc[0,'Low'],2)
        
    classic_support_2 = round(pivot_point - (classic_resistance_1 - classic_support_1),2)

    classic_resistance_2 = round((pivot_point - classic_support_1 ) + classic_resistance_1,2)

    classic_resistance_3 = round((pivot_point - classic_support_2 ) + classic_resistance_2,2)

    classic_support_3 = round(pivot_point - (classic_resistance_2 - classic_support_2),2)
    
    
    price_difference = (pivot_data.loc[0,'High'] - pivot_data.loc[0,'Low'])

    fibonnaci_resistance_1 = round((38.2*price_difference/100) + pivot_point,2)

    fibonnaci_resistance_2 = round((61.8*price_difference/100) + pivot_point,2)

    fibonnaci_resistance_3 = round((100*price_difference/100) + pivot_point,2)

    fibonnaci_support_1 = round(pivot_point - (38.2*price_difference/100),2)

    fibonnaci_support_2 = round(pivot_point - (61.8*price_difference/100),2)

    fibonnaci_support_3 = round(pivot_point - (100*price_difference/100),2)
    
    
    
    final_data.loc[0,"pivot_point"] = round(pivot_point,2)
    final_data.loc[0,"pivot_bc"] = round(pivot_bc,2)
    final_data.loc[0,"pivot_tc"] = round(pivot_tc,2)

    final_data.loc[0,"classical_support_1"] = classic_support_1
    
    final_data.loc[0,"classical_resistance_1"] = classic_resistance_1
        
    final_data.loc[0,"classical_support_2"] = classic_support_2

    final_data.loc[0,"classical_resistance_2"] = classic_resistance_2

    final_data.loc[0,"classical_resistance_3"] = classic_resistance_3

    final_data.loc[0,"classical_support_3"] = classic_support_3
    
    
    price_difference = (pivot_data.loc[0,'High'] - pivot_data.loc[0,'Low'])

    final_data.loc[0,"fibonnaci_resistance_1"] = round((38.2*price_difference/100) + pivot_point,2)

    final_data.loc[0,"fibonnaci_resistance_2"] = round((61.8*price_difference/100) + pivot_point,2)

    final_data.loc[0,"fibonnaci_resistance_3"] = round((100*price_difference/100) + pivot_point,2)

    final_data.loc[0,"fibonnaci_support_1"] = round(pivot_point - (38.2*price_difference/100),2)

    final_data.loc[0,"fibonnaci_support_2"] = round(pivot_point - (61.8*price_difference/100),2)

    final_data.loc[0,"fibonnaci_support_3"] = round(pivot_point - (100*price_difference/100),2)
    
#     print(pivot_point)
#     print(fibonnaci_resistance_1)
#     print(fibonnaci_resistance_2)
#     print(fibonnaci_resistance_3)
#     print(fibonnaci_support_1)
#     print(fibonnaci_support_2)
#     print(fibonnaci_support_3)
    

def arima_forecast(stock_data):
    df_close = stock_data[['Date','Close']]
    df_close['Date'] = pd.to_datetime(df_close['Date'])
    df_close = df_close.set_index('Date')
    df_close = df_close.dropna()
#     decomposition = seasonal_decompose(df_close, model='multiplicative', period = 30) 
#     model = ARIMA(df_close, order=(2,1,2))
    # results = model.fit()
    # plt.plot(df_close_shift)
    # plt.plot(results.fittedvalues, color='red')

    result = seasonal_decompose(df_close, model='multiplicative', period = 30)
    
    model_autoARIMA = auto_arima(df_close, start_p=0, start_q=0,
                      test='adf',       # use adftest to find optimal 'd'
                      max_p=3, max_q=3, # maximum p and q
                      m=1,              # frequency of series
                      d=None,           # let model determine 'd'
                      seasonal=False,   # No Seasonality
                      start_P=0, 
                      D=0, 
                      trace=True,
                      error_action='ignore',  
                      suppress_warnings=True, 
                      stepwise=True)
    model = ARIMA(df_close, order=model_autoARIMA.order)  
    fitted = model.fit()  
    # print(fitted.summary())

    forecast = fitted.get_forecast()
    yhat = forecast.predicted_mean
    yhat_conf_int = forecast.conf_int(alpha=0.05)
    yhat_conf_int2 = forecast.conf_int(alpha=0.2)
    
    yhat_conf_int.reset_index(level=0, inplace=True)
    yhat_conf_int2.reset_index(level=0, inplace=True)
    arima_pivot_point = fitted.forecast().iloc[0]
    
    final_data.loc[0,"arima_pivot_point"] = round(arima_pivot_point,2)
    final_data.loc[0,"arima_resistance_1"] = round(yhat_conf_int2.iloc[0,2],2)
    final_data.loc[0,"arima_resistance_2"] = round(yhat_conf_int.iloc[0,2],2)
    final_data.loc[0,"arima_support_1"] = round(yhat_conf_int2.iloc[0,1],2)
    final_data.loc[0,"arima_support_2"] = round(yhat_conf_int.iloc[0,1],2)
#     print(round(arima_pivot_point,2))
#     print(round(yhat_conf_int.iloc[0,1],2))
#     print(round(yhat_conf_int.iloc[0,2],2))
#     print(round(yhat_conf_int2.iloc[0,1],2))
#     print(round(yhat_conf_int2.iloc[0,2],2))
    
    
sql_data = pd.DataFrame()

final_data = pd.DataFrame()

nifty_data = yf.download(tickers='%5ENSEI', period="5d", interval="1d")
nifty_data = pd.DataFrame(nifty_data)
nifty_data.reset_index(level=0, inplace=True)
# nifty_data = nifty_data[nifty_data['Date'] < datetime.now().strftime("%Y-%m-%d")]

print(nifty_data)

calculate_classic_pivots(nifty_data)

stock_data = yf.download(tickers='%5ENSEI', interval="1d",start = "2019-05-10")
stock_data = pd.DataFrame(stock_data)
stock_data.reset_index(level=0, inplace=True)
stock_data


arima_forecast(stock_data)

final_data.loc[0,"Stock"] = "Nifty"
final_data.loc[0,"Execution_date"] = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d')

final_data = final_data[['Stock','Execution_date','pivot_point','pivot_bc','pivot_tc', 'classical_support_1', 'classical_resistance_1','classical_support_2', 'classical_resistance_2', 'classical_resistance_3','classical_support_3', 'fibonnaci_resistance_1', 'fibonnaci_resistance_2', 'fibonnaci_resistance_3', 'fibonnaci_support_1', 'fibonnaci_support_2','fibonnaci_support_3', 'arima_pivot_point', 'arima_resistance_1','arima_resistance_2', 'arima_support_1', 'arima_support_2']]

sql_data = sql_data.append(final_data)

final_data = pd.DataFrame()

nifty_data = yf.download(tickers='%5ENSEBANK', period="5d", interval="1d")
nifty_data = pd.DataFrame(nifty_data)
nifty_data.reset_index(level=0, inplace=True)
# nifty_data = nifty_data[nifty_data['Date'] < datetime.now().strftime("%Y-%m-%d")]

print(nifty_data)

calculate_classic_pivots(nifty_data)

stock_data = yf.download(tickers='%5ENSEBANK', interval="1d",start = "2019-05-10")
stock_data = pd.DataFrame(stock_data)
stock_data.reset_index(level=0, inplace=True)
stock_data


arima_forecast(stock_data)

final_data.loc[0,"Stock"] = "BankNifty"
final_data.loc[0,"Execution_date"] = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d')

final_data = final_data[['Stock','Execution_date','pivot_point','pivot_bc','pivot_tc', 'classical_support_1', 'classical_resistance_1','classical_support_2', 'classical_resistance_2', 'classical_resistance_3','classical_support_3', 'fibonnaci_resistance_1', 'fibonnaci_resistance_2', 'fibonnaci_resistance_3', 'fibonnaci_support_1', 'fibonnaci_support_2','fibonnaci_support_3', 'arima_pivot_point', 'arima_resistance_1','arima_resistance_2', 'arima_support_1', 'arima_support_2']]

sql_data = sql_data.append(final_data)

sql_data.reset_index(level=0, inplace=True)

print(sql_data)



sql = """insert into titania_trading.support_and_resistance (Stock,Execution_date,pivot_point,pivot_bc,pivot_tc, classical_support_1, classical_resistance_1,classical_support_2, classical_resistance_2, classical_resistance_3,classical_support_3, fibonnaci_resistance_1, fibonnaci_resistance_2, fibonnaci_resistance_3, fibonnaci_support_1, fibonnaci_support_2,fibonnaci_support_3, arima_pivot_point, arima_resistance_1,arima_resistance_2, arima_support_1, arima_support_2)
         values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s) 
    """

if len(sql_data) > 0 :
    sql_data["pivot_point"] = sql_data["pivot_point"].astype(float)
    sql_data["pivot_bc"] = sql_data["pivot_bc"].astype(float)
    sql_data["pivot_tc"] = sql_data["pivot_tc"].astype(float)
    sql_data["classical_support_1"] = sql_data["classical_support_1"].astype(float)
    sql_data["classical_resistance_1"] = sql_data["classical_resistance_1"].astype(float)
    sql_data["classical_support_2"] = sql_data["classical_support_2"].astype(float)
    sql_data["classical_resistance_2"] = sql_data["classical_resistance_2"].astype(float)
    sql_data["classical_support_3"] = sql_data["classical_support_3"].astype(float)
    sql_data["classical_resistance_3"] = sql_data["classical_resistance_3"].astype(float)
    sql_data["fibonnaci_resistance_1"] = sql_data["fibonnaci_resistance_1"].astype(float)
    sql_data["fibonnaci_resistance_2"] = sql_data["fibonnaci_resistance_2"].astype(float)
    sql_data["fibonnaci_resistance_3"] = sql_data["fibonnaci_resistance_3"].astype(float)
    sql_data["fibonnaci_support_1"] = sql_data["fibonnaci_support_1"].astype(float)
    sql_data["fibonnaci_support_2"] = sql_data["fibonnaci_support_2"].astype(float)
    sql_data["fibonnaci_support_3"] = sql_data["fibonnaci_support_3"].astype(float)
    sql_data["arima_resistance_1"] = sql_data["arima_resistance_1"].astype(float)
    sql_data["arima_resistance_2"] = sql_data["arima_resistance_2"].astype(float)
    sql_data["arima_support_1"] = sql_data["arima_support_1"].astype(float)
    sql_data["arima_support_2"] = sql_data["arima_support_2"].astype(float)

    collection = db["support_and_resistance"]
    x = collection.insert_many(sql_data.to_dict('records'))
    print(x.inserted_ids)





for idx in range(0,len(sql_data)):
    print(idx)

    cursor.execute(sql,(sql_data.loc[idx,"Stock"],
                        sql_data.loc[idx,"Execution_date"],
                        float(sql_data.loc[idx,"pivot_point"]),
                        float(sql_data.loc[idx,"pivot_bc"]),
                        float(sql_data.loc[idx,"pivot_tc"]),
                        float(sql_data.loc[idx,"classical_support_1"]),
                        float(sql_data.loc[idx,"classical_resistance_1"]),
                        float(sql_data.loc[idx,"classical_support_2"]),
                        float(sql_data.loc[idx,"classical_resistance_2"]),
                        float(sql_data.loc[idx,"classical_support_3"]),
                        float(sql_data.loc[idx,"classical_resistance_3"]),
                        float(sql_data.loc[idx,"fibonnaci_resistance_1"]),
                        float(sql_data.loc[idx,"fibonnaci_resistance_2"]),
                        float(sql_data.loc[idx,"fibonnaci_resistance_3"]),
                        float(sql_data.loc[idx,"fibonnaci_support_1"]),
                        float(sql_data.loc[idx,"fibonnaci_support_2"]),
                        float(sql_data.loc[idx,"fibonnaci_support_3"]),
                        float(sql_data.loc[idx,"arima_pivot_point"]),
                        float(sql_data.loc[idx,"arima_resistance_1"]),
                        float(sql_data.loc[idx,"arima_resistance_2"]),
                        float(sql_data.loc[idx,"arima_support_1"]),
                        float(sql_data.loc[idx,"arima_support_2"])
                        ))
    con.commit()

