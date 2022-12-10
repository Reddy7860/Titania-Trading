import pandas as pd
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import warnings
warnings.filterwarnings("ignore")


## Authenicate the Collections :
server_api = ServerApi('1')
client = MongoClient("mongodb+srv://Titania:Mahadev@cluster0.zq3w2cn.mongodb.net/titania_trading?ssl=true&ssl_cert_reqs=CERT_NONE", server_api=server_api)
db = client.titania_trading

## Fetch the Global Market Data
## Index filters : 'Nasdaq','Dow Jones','S & P 500','FTSE','NIKKI','Hang Seng'
global_result = db["global_markets"].find({'index':'Nasdaq'}).sort('Date',-1).limit(100)
global_result = pd.DataFrame(global_result)


## Fetch FII and DII Data
fii_dii_result = db["fii_dii_data"].find({}).sort('Date',-1).limit(100)
fii_dii_result = pd.DataFrame(fii_dii_result)
fii_dii_result = fii_dii_result.fillna(0)
fii_dii_result['FutureIndexLong'] = fii_dii_result['FutureIndexLong'].astype(int)
fii_dii_result['FutureIndexShort'] = fii_dii_result['FutureIndexShort'].astype(int)
fii_dii_result['FutureStockLong'] = fii_dii_result['FutureStockLong'].astype(int)
fii_dii_result['FutureStockShort'] = fii_dii_result['FutureStockShort'].astype(int)

fii_dii_result['FutureIndexLong_pct'] = (fii_dii_result['FutureIndexLong']/(fii_dii_result['FutureIndexLong'] + fii_dii_result['FutureIndexShort']))*100.0
fii_dii_result['FutureIndexShort_pct'] = (fii_dii_result['FutureIndexShort']/(fii_dii_result['FutureIndexLong'] + fii_dii_result['FutureIndexShort']))*100.0
fii_dii_result['FutureStockLong_pct'] = (fii_dii_result['FutureStockLong']/(fii_dii_result['FutureStockLong'] + fii_dii_result['FutureStockShort']))*100.0
fii_dii_result['FutureStockShort_pct'] = (fii_dii_result['FutureStockShort']/(fii_dii_result['FutureStockLong'] + fii_dii_result['FutureStockShort']))*100.0

final_fii_dii_result = fii_dii_result.loc[fii_dii_result['ClientType'] == 'FII',]






