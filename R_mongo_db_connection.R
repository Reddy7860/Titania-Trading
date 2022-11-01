library(mongolite)

connection_string = "mongodb+srv://Titania:Mahadev@cluster0.zq3w2cn.mongodb.net/titania_trading"

final_orders_raw_data_collection = mongo(collection="final_orders_raw_data", db="titania_trading", url=connection_string)

temp_query = paste0('{"execution_date": { "$gte" :  "',as.character(Sys.Date()), '" }}',"")

query_output = final_orders_raw_data_collection$find(query = temp_query,sort = '{"Datetime" : 1}')

algo_orders_place_data_collection = mongo(collection="algo_orders_place_data", db="titania_trading", url=connection_string)
temp_query = paste0('{"execution_date": { "$gte" :  "',as.character(Sys.Date()), '" }',',"client_id":"J95213" }')
query_output = algo_orders_place_data_collection$find(query = temp_query,sort = '{"Datetime" : 1}')
