import pandas as pd
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import warnings
from datetime import datetime,timedelta
from pytz import timezone
from itertools import compress
import talib
import numpy as np
import gzip
import itertools

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings('ignore')


start_time = datetime.now(timezone("Asia/Kolkata"))
print("Script execution started")
print(start_time)

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

server_api = ServerApi('1')

client = MongoClient("mongodb+srv://Titania:Mahadev@cluster0.zq3w2cn.mongodb.net/titania_trading?ssl=true&ssl_cert_reqs=CERT_NONE", server_api=server_api)

# db = client.titania_trading

db = client["titania_trading"]

candle_rankings = {
    
        "CDL3LINESTRIKE_Bull": 1,
        "CDL3LINESTRIKE_Bear": 2,
        "CDL3BLACKCROWS_Bull": 3,
        "CDL3BLACKCROWS_Bear": 3,
        "CDLEVENINGSTAR_Bull": 4,
        "CDLEVENINGSTAR_Bear": 4,
        "CDLTASUKIGAP_Bull": 5,
        "CDLTASUKIGAP_Bear": 5,
        "CDLINVERTEDHAMMER_Bull": 6,
        "CDLINVERTEDHAMMER_Bear": 6,
        "CDLMATCHINGLOW_Bull": 7,
        "CDLMATCHINGLOW_Bear": 7,
        "CDLABANDONEDBABY_Bull": 8,
        "CDLABANDONEDBABY_Bear": 8,
        "CDLBREAKAWAY_Bull": 10,
        "CDLBREAKAWAY_Bear": 10,
        "CDLMORNINGSTAR_Bull": 12,
        "CDLMORNINGSTAR_Bear": 12,
        "CDLPIERCING_Bull": 13,
        "CDLPIERCING_Bear": 13,
        "CDLSTICKSANDWICH_Bull": 14,
        "CDLSTICKSANDWICH_Bear": 14,
        "CDLTHRUSTING_Bull": 15,
        "CDLTHRUSTING_Bear": 15,
        "CDLINNECK_Bull": 17,
        "CDLINNECK_Bear": 17,
        "CDL3INSIDE_Bull": 20,
        "CDL3INSIDE_Bear": 56,
        "CDLHOMINGPIGEON_Bull": 21,
        "CDLHOMINGPIGEON_Bear": 21,
        "CDLDARKCLOUDCOVER_Bull": 22,
        "CDLDARKCLOUDCOVER_Bear": 22,
        "CDLIDENTICAL3CROWS_Bull": 24,
        "CDLIDENTICAL3CROWS_Bear": 24,
        "CDLMORNINGDOJISTAR_Bull": 25,
        "CDLMORNINGDOJISTAR_Bear": 25,
        "CDLXSIDEGAP3METHODS_Bull": 27,
        "CDLXSIDEGAP3METHODS_Bear": 26,
        "CDLTRISTAR_Bull": 28,
        "CDLTRISTAR_Bear": 76,
        "CDLGAPSIDESIDEWHITE_Bull": 46,
        "CDLGAPSIDESIDEWHITE_Bear": 29,
        "CDLEVENINGDOJISTAR_Bull": 30,
        "CDLEVENINGDOJISTAR_Bear": 30,
        "CDL3WHITESOLDIERS_Bull": 32,
        "CDL3WHITESOLDIERS_Bear": 32,
        "CDLONNECK_Bull": 33,
        "CDLONNECK_Bear": 33,
        "CDL3OUTSIDE_Bull": 34,
        "CDL3OUTSIDE_Bear": 39,
        "CDLRICKSHAWMAN_Bull": 35,
        "CDLRICKSHAWMAN_Bear": 35,
        "CDLSEPARATINGLINES_Bull": 36,
        "CDLSEPARATINGLINES_Bear": 40,
        "CDLLONGLEGGEDDOJI_Bull": 37,
        "CDLLONGLEGGEDDOJI_Bear": 37,
        "CDLHARAMI_Bull": 38,
        "CDLHARAMI_Bear": 72,
        "CDLLADDERBOTTOM_Bull": 41,
        "CDLLADDERBOTTOM_Bear": 41,
        "CDLCLOSINGMARUBOZU_Bull": 70,
        "CDLCLOSINGMARUBOZU_Bear": 43,
        "CDLTAKURI_Bull": 47,
        "CDLTAKURI_Bear": 47,
        "CDLDOJISTAR_Bull": 49,
        "CDLDOJISTAR_Bear": 51,
        "CDLHARAMICROSS_Bull": 50,
        "CDLHARAMICROSS_Bear": 80,
        "CDLADVANCEBLOCK_Bull": 54,
        "CDLADVANCEBLOCK_Bear": 54,
        "CDLSHOOTINGSTAR_Bull": 55,
        "CDLSHOOTINGSTAR_Bear": 55,
        "CDLMARUBOZU_Bull": 71,
        "CDLMARUBOZU_Bear": 57,
        "CDLUNIQUE3RIVER_Bull": 60,
        "CDLUNIQUE3RIVER_Bear": 60,
        "CDL2CROWS_Bull": 61,
        "CDL2CROWS_Bear": 61,
        "CDLBELTHOLD_Bull": 62,
        "CDLBELTHOLD_Bear": 63,
        "CDLHAMMER_Bull": 65,
        "CDLHAMMER_Bear": 65,
        "CDLHIGHWAVE_Bull": 67,
        "CDLHIGHWAVE_Bear": 67,
        "CDLSPINNINGTOP_Bull": 69,
        "CDLSPINNINGTOP_Bear": 73,
        "CDLUPSIDEGAP2CROWS_Bull": 74,
        "CDLUPSIDEGAP2CROWS_Bear": 74,
        "CDLGRAVESTONEDOJI_Bull": 77,
        "CDLGRAVESTONEDOJI_Bear": 77,
        "CDLHIKKAKEMOD_Bull": 82,
        "CDLHIKKAKEMOD_Bear": 81,
        "CDLHIKKAKE_Bull": 85,
        "CDLHIKKAKE_Bear": 83,
        "CDLENGULFING_Bull": 84,
        "CDLENGULFING_Bear": 91,
        "CDLMATHOLD_Bull": 86,
        "CDLMATHOLD_Bear": 86,
        "CDLHANGINGMAN_Bull": 87,
        "CDLHANGINGMAN_Bear": 87,
        "CDLRISEFALL3METHODS_Bull": 94,
        "CDLRISEFALL3METHODS_Bear": 89,
        "CDLKICKING_Bull": 96,
        "CDLKICKING_Bear": 102,
        "CDLDRAGONFLYDOJI_Bull": 98,
        "CDLDRAGONFLYDOJI_Bear": 98,
        "CDLCONCEALBABYSWALL_Bull": 101,
        "CDLCONCEALBABYSWALL_Bear": 101,
        "CDL3STARSINSOUTH_Bull": 103,
        "CDL3STARSINSOUTH_Bear": 103,
        "CDLDOJI_Bull": 104,
        "CDLDOJI_Bear": 104
        # ,
    
    
    
    
        # "CDLLONGLINE_Bull":106,
        # "CDLLONGLINE_Bear":106,
        # "CDLSHORTLINE_Bull":108,
        # "CDLSHORTLINE_Bear":108,
        # "CDLSTALLEDPATTERN_Bull":110,
        # "CDLSTALLEDPATTERN_Bear":110
    
    }

candle_names = talib.get_function_groups()['Pattern Recognition']

# patterns not found in the patternsite.com
exclude_items = ('CDLCOUNTERATTACK',
                 'CDLLONGLINE',
                 'CDLSHORTLINE',
                 'CDLSTALLEDPATTERN',
                 'CDLKICKINGBYLENGTH')

candle_names = [candle for candle in candle_names if candle not in exclude_items]

nse_data = pd.DataFrame([["BankNifty","%5ENSEBANK"],["Nifty","%5ENSEI"]],columns=["Symbol","Yahoo_Symbol"])



for idx in range(0,len(nse_data)):
	current_symbol = nse_data.loc[idx,"Symbol"]
	print(current_symbol)
	# technical_data
	Stocks_data_5_minutes = db["Stocks_data_5_minutes"]
	Candle_stick_pattern_5_minutes = db["Candle_stick_pattern_5_minutes"]

	Stocks_data_5_minutes = Stocks_data_5_minutes.find({"Stock":str(current_symbol),"instrumenttype":"OPTIDX"}).sort([('Datetime', 1)])

	Stocks_data_5_minutes =  pd.DataFrame(list(Stocks_data_5_minutes))

	Stocks_data_5_minutes['Datetime'] = Stocks_data_5_minutes['Datetime'] + timedelta(hours=5,minutes=30)

	Stocks_data_5_minutes = Stocks_data_5_minutes[['Stock', 'Datetime', 'Open', 'High', 'Low', 'Close', 'Volume','instrumenttype', 'Execution_Date']]

	# print(Stocks_data_5_minutes.columns)

	# extract OHLC 
	op = Stocks_data_5_minutes['Open']
	hi = Stocks_data_5_minutes['High']
	lo = Stocks_data_5_minutes['Low']
	cl = Stocks_data_5_minutes['Close']
	# create columns for each pattern
	for candle in candle_names:
	    # below is same as;
	    # df["CDL3LINESTRIKE"] = talib.CDL3LINESTRIKE(op, hi, lo, cl)
	    Stocks_data_5_minutes[candle] = getattr(talib, candle)(op, hi, lo, cl)

	# print(Stocks_data_5_minutes)

	df = Stocks_data_5_minutes

	df['candlestick_pattern'] = np.nan
	df['candlestick_match_count'] = np.nan
	for index, row in df.iterrows():

	    # no pattern found
	    if len(row[candle_names]) - sum(row[candle_names] == 0) == 0:
	        df.loc[index,'candlestick_pattern'] = "NO_PATTERN"
	        df.loc[index, 'candlestick_match_count'] = 0
	    # single pattern found
	    elif len(row[candle_names]) - sum(row[candle_names] == 0) == 1:
	        # bull pattern 100 or 200
	        if any(row[candle_names].values > 0):
	            pattern = list(compress(row[candle_names].keys(), row[candle_names].values != 0))[0] + '_Bull'
	            df.loc[index, 'candlestick_pattern'] = pattern
	            df.loc[index, 'candlestick_match_count'] = 1
	        # bear pattern -100 or -200
	        else:
	            pattern = list(compress(row[candle_names].keys(), row[candle_names].values != 0))[0] + '_Bear'
	            df.loc[index, 'candlestick_pattern'] = pattern
	            df.loc[index, 'candlestick_match_count'] = 1
	    # multiple patterns matched -- select best performance
	    else:
	        # filter out pattern names from bool list of values
	        patterns = list(compress(row[candle_names].keys(), row[candle_names].values != 0))
	        container = []
	        for pattern in patterns:
	            if row[pattern] > 0:
	                container.append(pattern + '_Bull')
	            else:
	                container.append(pattern + '_Bear')
	        rank_list = [candle_rankings[p] for p in container]
	        if len(rank_list) == len(container):
	            rank_index_best = rank_list.index(min(rank_list))
	            df.loc[index, 'candlestick_pattern'] = container[rank_index_best]
	            df.loc[index, 'candlestick_match_count'] = len(container)
	# clean up candle columns
	df.drop(candle_names, axis = 1, inplace = True)

	print(df)

	x = Candle_stick_pattern_5_minutes.delete_many({"Stock":str(current_symbol)})

	print(x.deleted_count, " documents deleted.")

	Candle_stick_pattern_5_minutes.insert_many(df.to_dict('records'))

	


end_time = datetime.now(timezone("Asia/Kolkata"))

print(end_time)

print('Duration: {}'.format(end_time - start_time))