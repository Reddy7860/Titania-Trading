import pandas as pd
from pytz import timezone 
from datetime import datetime,timedelta

import mysql.connector as mysql
import pymysql
# import MySQLdb
# import pymysql

from smartapi import SmartConnect
import pyotp

from sqlalchemy.engine import result
import sqlalchemy
from sqlalchemy import create_engine, MetaData,\
Table, Column, Numeric, Integer, VARCHAR, update, delete

from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://root:Mahadev_143@localhost/titania_trading")
print(engine)


con = mysql.connect(user='root', password='Mahadev_143', database='titania_trading')
cursor = con.cursor()


sql = "select * from titania_trading.client_details where client_id in ('J95213','S1604557','G304915','K256027')"
print(sql)
df = pd.read_sql(sql,con=engine)

def del_and_append_data(todays_data,ind_time,client_id,table_name):
    sql = "select * from "+str(table_name)+" where Execution_Date = '" + str(ind_time) + "' and client_id = '" + str(client_id) + "'"
    print(sql)
    df = pd.read_sql(sql,con=engine)

    ## There is already todays data
    if len(df) > 0:
        sql_Delete_query = "delete from "+str(table_name)+" where Execution_Date = '" + str(ind_time) + "' and client_id = '" + str(client_id) + "'" 
        cursor.execute(sql_Delete_query)
        con.commit()
#         cursor.close()
#         con.close()

    todays_data.to_sql(name=table_name,con=engine, if_exists='append', index=False)


sql = "select * from titania_trading.client_details where client_id in ('J95213','S1604557','G304915','K256027')"
print(sql)
client_data = pd.read_sql(sql,con=engine)

for i in range(0,len(client_data)):
    print("Running for  {}".format(str(client_data.loc[i,'client_name'])))
    con = mysql.connect(user='root', password='Mahadev_143', database='titania_trading')
    cursor = con.cursor()
    ind_time = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d')
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
    client_id = client_data.loc[i,"client_id"]
    print(client_id)
    filename = 'Python_' + datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+'.csv'
    try:
        data = pd.read_csv('/Users/apple/Downloads/Orders_Data/'+str(client_id)+'/Options_Order/'+str(filename))
        data['Client_id'] = client_id
        
        data['Execution_Date'] = ind_time
        data = data[['Client_id','Execution_Date','Strategy', 'Stock', 'Signal', 'Datetime', 'Value',
                       'SMA_Call', 'RSI_Call', 'MACD_Call', 'Pivot_Call', 'BB_Call',
                       'VWAP_Call', 'SuperTrend_Call', 'PCR_Call', 'Probability', 'StopLoss',
                       'Target', 'Qty', 'Spot_Price', 'expiry', 'Strike_Buy_Price',
                       'premium_StopLoss', 'premium_Target', 'lotsize', 'premium_Qty',
                       'historic_profit', 'token', 'current_script', 'exec_rnk', 'order_place',
                       'order_id', 'conclusion', 'execution_time']]
        data.columns = ['Client_id','Execution_Date','Strategy', 'Stock', 'Current_Signal', 'Datetime', 'Value',
                       'SMA_Call', 'RSI_Call', 'MACD_Call', 'Pivot_Call', 'BB_Call',
                       'VWAP_Call', 'SuperTrend_Call', 'PCR_Call', 'Probability', 'StopLoss',
                       'Target', 'Qty', 'Spot_Price', 'expiry', 'Strike_Buy_Price',
                       'premium_StopLoss', 'premium_Target', 'lotsize', 'premium_Qty',
                       'historic_profit', 'token', 'current_script', 'exec_rnk', 'order_place',
                       'order_id',  'conclusion', 'execution_time']
        
        del_and_append_data(data,ind_time,client_id,"algo_orders_data")
    except Exception as e:
        print("No data found")
    
    # user_id = df.loc[idx,"client_id"]
    # print(user_id)
    # obj = SmartConnect(api_key = df.loc[idx,"client_api_key"])
    # print(df.loc[idx,"client_api_key"])
    # print(df.loc[idx,'client_password'])
    # data = obj.generateSession(user_id,df.loc[idx,'client_password'])

    my_positons_data = obj.position()

    current_position_data = pd.DataFrame(my_positons_data['data'])
    
    print(current_position_data.columns)
    
    if len(current_position_data)>0:
        current_position_data['Client_id'] = client_id
        current_position_data['Execution_Date'] = ind_time
        
        current_position_data = current_position_data[['Client_id','Execution_Date','symboltoken', 'symbolname', 'instrumenttype', 'priceden', 'pricenum',
       'genden', 'gennum', 'precision', 'multiplier', 'boardlotsize',
       'exchange', 'producttype', 'tradingsymbol', 'symbolgroup',
       'strikeprice', 'optiontype', 'expirydate', 'lotsize', 'cfbuyqty',
       'cfsellqty', 'cfbuyamount', 'cfsellamount', 'buyavgprice',
       'sellavgprice', 'avgnetprice', 'netvalue', 'netqty', 'totalbuyvalue',
       'totalsellvalue', 'cfbuyavgprice', 'cfsellavgprice', 'totalbuyavgprice',
       'totalsellavgprice', 'netprice', 'buyqty', 'sellqty', 'buyamount',
       'sellamount', 'pnl', 'realised', 'unrealised', 'ltp', 'close']]
        
        current_position_data.columns = ['Client_id','Execution_Date','symboltoken', 'symbolname', 'instrumenttype', 'priceden', 'pricenum',
       'genden', 'gennum', 'precisionNum', 'multiplier', 'boardlotsize',
       'exchange', 'producttype', 'tradingsymbol', 'symbolgroup',
       'strikeprice', 'optiontype', 'expirydate', 'lotsize', 'cfbuyqty',
       'cfsellqty', 'cfbuyamount', 'cfsellamount', 'buyavgprice',
       'sellavgprice', 'avgnetprice', 'netvalue', 'netqty', 'totalbuyvalue',
       'totalsellvalue', 'cfbuyavgprice', 'cfsellavgprice', 'totalbuyavgprice',
       'totalsellavgprice', 'netprice', 'buyqty', 'sellqty', 'buyamount',
       'sellamount', 'pnl', 'realised', 'unrealised', 'ltp', 'close']
        
        del_and_append_data(current_position_data,ind_time,client_id,"position_data")
    
#     print(current_position_data)


    my_orders = obj.orderBook()
    order_data = pd.DataFrame(my_orders['data'])
    
    print(order_data.columns)
    
    if len(order_data)>0:
        order_data['Client_id'] = client_id
        order_data['Execution_Date'] = ind_time
        order_data = order_data[['Client_id','Execution_Date','variety', 'ordertype', 'producttype', 'duration', 'price',
       'triggerprice', 'quantity', 'disclosedquantity', 'squareoff',
       'stoploss', 'trailingstoploss', 'tradingsymbol', 'transactiontype',
       'exchange', 'symboltoken', 'ordertag', 'instrumenttype', 'strikeprice',
       'optiontype', 'expirydate', 'lotsize', 'cancelsize', 'averageprice',
       'filledshares', 'unfilledshares', 'orderid', 'text', 'status',
       'orderstatus', 'updatetime', 'exchtime', 'exchorderupdatetime',
       'fillid', 'filltime', 'parentorderid']]
        
        del_and_append_data(order_data,ind_time,client_id,"order_data")