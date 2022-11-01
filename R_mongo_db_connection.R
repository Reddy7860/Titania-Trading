library(mongolite)

connection_string = "mongodb+srv://Titania:Mahadev@cluster0.zq3w2cn.mongodb.net/titania_trading"

final_orders_raw_data_collection = mongo(collection="final_orders_raw_data", db="titania_trading", url=connection_string)

query_output = final_orders_raw_data_collection$find('{"execution_date":Sys.Date()}',sort = '{"Datetime" : -1}')

