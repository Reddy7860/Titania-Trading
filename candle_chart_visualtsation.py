import yfinance as yf
import pandas as pd
import plotly.graph_objects as go
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from datetime import timedelta


server_api = ServerApi('1')

client = MongoClient("mongodb+srv://Titania:Mahadev@cluster0.zq3w2cn.mongodb.net/titania_trading?ssl=true&ssl_cert_reqs=CERT_NONE", server_api=server_api)
db = client["titania_trading"]

Stocks_data_5_minutes = db["Stocks_data_5_minutes"].find({'Stock':"BankNifty",'instrumenttype':"FUTIDX"}).sort([('Datetime', 1)])
Stocks_data_5_minutes =  pd.DataFrame(list(Stocks_data_5_minutes))

Stocks_data_1_minutes = db["Stocks_data_1_minutes"].find({'Stock':"BankNifty",'instrumenttype':"FUTIDX"}).sort([('Datetime', 1)])
Stocks_data_1_minutes =  pd.DataFrame(list(Stocks_data_1_minutes))

support_and_resistance = db["support_and_resistance"].find({'Stock':"BankNifty"}).sort([('Execution_date', 1)])
support_and_resistance =  pd.DataFrame(list(support_and_resistance))

Stocks_data_5_minutes = Stocks_data_5_minutes[['Datetime','Open','High','Low','Close','Volume','Execution_Date']]
Stocks_data_1_minutes = Stocks_data_1_minutes[['Datetime','Open','High','Low','Close','Volume','Execution_Date']]

support_and_resistance = support_and_resistance[['Stock','Execution_date','pivot_point','arima_resistance_2','arima_resistance_1','arima_pivot_point','arima_support_2','arima_support_1']]

print(Stocks_data_5_minutes.columns)
print(support_and_resistance.columns)


futures_options_signals['Combined_Rnk'] = futures_options_signals['fut_volume_rank'] +  futures_options_signals['call_volume_rank'] +  futures_options_signals['put_volume_rank']
futures_options_signals = futures_options_signals.sort_values(['Combined_Rnk'])
futures_options_signals.reset_index(inplace=True,drop=True)

highest_change = futures_options_signals.loc[0,'Datetime']
Stocks_data_1_minutes['Datetime'] = Stocks_data_1_minutes['Datetime'] + timedelta(hours=5,minutes=30)
highest_entry = Stocks_data_1_minutes.loc[Stocks_data_1_minutes['Datetime'] == highest_change,]
highest_entry.reset_index(inplace=True,drop=True)

oi_low = highest_entry.loc[0,"Low"]
oi_high = highest_entry.loc[0,"High"]

modified_stocks_5_data = Stocks_data_5_minutes.merge(support_and_resistance, left_on='Execution_Date', right_on='Execution_date',how="left")
modified_stocks_5_data['Datetime'] = modified_stocks_5_data['Datetime'] + timedelta(hours=5,minutes=30)
modified_stocks_5_data['oi_low'] = oi_low
modified_stocks_5_data['oi_high'] = oi_high


algo_orders_place_data = db["algo_orders_place_data"].find({'Stock':"%5ENSEBANK",'client_id':"J95213","execution_date":"2022-12-20"})
algo_orders_place_data =  pd.DataFrame(list(algo_orders_place_data))

modified_stocks_5_data['Execution_Date'] = pd.to_datetime(modified_stocks_5_data['Execution_Date'])

algo_orders_place_data = algo_orders_place_data[['Strategy', 'Stock', 'Signal','Datetime', 'Value','buy_probability', 'sell_probability','current_script','Strike_Buy_Price','premium_Target','premium_StopLoss','historic_profit','conclusion']]

modified_stocks_5_data = modified_stocks_5_data.merge(algo_orders_place_data, left_on='Datetime', right_on='Datetime',how="left")


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

modified_stocks_5_data



fig = go.Figure(data=[go.Candlestick(x=modified_stocks_5_data['Datetime'],
                open=modified_stocks_5_data['Open'], high=modified_stocks_5_data['High'],
                low=modified_stocks_5_data['Low'], close=modified_stocks_5_data['Close'])
                     ])

fig.add_trace(
    go.Scatter(mode = "lines",
        x=modified_stocks_5_data['Datetime'],
        y=modified_stocks_5_data['arima_pivot_point'],
        name='Arima Pivot Point'
    ))

fig.add_trace(
    go.Scatter(mode = "lines",
        x=modified_stocks_5_data['Datetime'],
        y=modified_stocks_5_data['arima_resistance_1'],
        name = 'Arima Resistance'
    ))

fig.add_trace(
    go.Scatter(mode = "lines",
        x=modified_stocks_5_data['Datetime'],
        y=modified_stocks_5_data['arima_support_1'],
        name = 'Arima Support'
    ))

fig.add_trace(
    go.Scatter(mode = "lines",
        x=modified_stocks_5_data['Datetime'],
        y=modified_stocks_5_data['oi_low'],
        name = 'Highest OI Low'
    ))

fig.add_trace(
    go.Scatter(mode = "lines",
        x=modified_stocks_5_data['Datetime'],
        y=modified_stocks_5_data['oi_high'],
        name = 'Highest OI High'
    ))

# fig.add_trace(
#     go.Scatter(
#         x=modified_stocks_5_data['Datetime'],
#         y=modified_stocks_5_data["Strategy_x"],
#         mode="markers+text",
#         marker=dict(symbol='triangle-down-open', size = 12),
# #         text = 'important',
# #         textposition = 'middle right'

#     )
# )

fig.add_trace(
    go.Scatter(
        x=algo_orders_place_data['Datetime'],
        y=algo_orders_place_data['Value'],
        mode="markers",
        marker=dict(symbol='star-triangle-down'),
        marker_size=12,
        hovertemplate = ('<i>Value</i>: %{y:.2f}'+\
                         '<br><b>Height</b>: %customdata[0]<br>'+\
                      '<br><b>Thickness 1</b>: %{customdata[1]}<br>'+\
                      '<br><b>Thickness 2</b>: %{customdata[2]}<br>'+\
                          '<br><b>Time</b>: %{x}<br>')

    )
)

fig.layout = dict(xaxis=dict(type="category"))

fig.update_layout(xaxis_rangeslider_visible=False)
fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])]) 

fig.update_layout(
    autosize=False,
    width=1000,
    height=800,)


fig.show()










#### Technical Chart visualisation

technical_indicator_5_minutes = db["technical_indicator_5_minutes"].find({'Stock':"Nifty"})
technical_indicator_5_minutes =  pd.DataFrame(list(technical_indicator_5_minutes))

fig = go.Figure()
fig.add_trace(go.Scatter(x=technical_indicator_5_minutes['Datetime'], y=technical_indicator_5_minutes['buy_probability'],
                    mode='lines+markers',
                    name='Buy Probability'))
fig.add_trace(go.Scatter(x=technical_indicator_5_minutes['Datetime'], y=technical_indicator_5_minutes['sell_probability'],
                    mode='lines+markers',
                    name='Sell Probability'))


fig.show()

