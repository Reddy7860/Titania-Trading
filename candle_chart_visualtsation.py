import yfinance as yf
import pandas as pd
import plotly.graph_objects as go
from pymongo import MongoClient
from pymongo.server_api import ServerApi


server_api = ServerApi('1')

client = MongoClient("mongodb+srv://Titania:Mahadev@cluster0.zq3w2cn.mongodb.net/titania_trading?ssl=true&ssl_cert_reqs=CERT_NONE", server_api=server_api)
db = client["titania_trading"]

Stocks_data_5_minutes = db["Stocks_data_5_minutes"].find({'Stock':"Nifty",'instrumenttype':"FUTIDX"}).sort([('Datetime', 1)])
Stocks_data_5_minutes =  pd.DataFrame(list(Stocks_data_5_minutes))


support_and_resistance = db["support_and_resistance"].find({'Stock':"Nifty"}).sort([('Execution_date', 1)])
support_and_resistance =  pd.DataFrame(list(support_and_resistance))


Stocks_data_5_minutes = Stocks_data_5_minutes[['Datetime','Open','High','Low','Close','Volume','Execution_Date']]

support_and_resistance = support_and_resistance[['Stock','Execution_date','pivot_point','arima_resistance_2','arima_resistance_1','arima_pivot_point','arima_support_2','arima_support_1']]

print(Stocks_data_5_minutes.columns)
print(support_and_resistance.columns)

modified_stocks_5_data = Stocks_data_5_minutes.merge(support_and_resistance, left_on='Execution_Date', right_on='Execution_date',how="left")

fig = go.Figure(data=[go.Candlestick(x=modified_stocks_5_data['Datetime'],
                open=modified_stocks_5_data['Open'], high=modified_stocks_5_data['High'],
                low=modified_stocks_5_data['Low'], close=modified_stocks_5_data['Close'])
                     ])

fig.layout = dict(xaxis=dict(type="category"))

fig.add_trace(
    go.Scatter(mode = "lines",
        x=modified_stocks_5_data['Datetime'],
        y=modified_stocks_5_data['arima_pivot_point']
    ))

fig.add_trace(
    go.Scatter(mode = "lines",
        x=modified_stocks_5_data['Datetime'],
        y=modified_stocks_5_data['arima_resistance_1']
    ))

fig.add_trace(
    go.Scatter(mode = "lines",
        x=modified_stocks_5_data['Datetime'],
        y=modified_stocks_5_data['arima_support_1']
    ))

fig.update_layout(xaxis_rangeslider_visible=False)
fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])]) 


fig.show()
