from subprocess import call
import datetime
from datetime import datetime,timedelta
from pytz import timezone 

start_time = datetime.now(timezone("Asia/Kolkata"))
print("Script execution started")
print(start_time)


server = 'ubuntu@ec2-52-66-247-124.ap-south-1.compute.amazonaws.com'

filename = 'Python_' + datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+'.csv'
# print(filename)


cmd = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +':/home/ubuntu/Python_Automation/data/Orders_Data/J95213/Options_Order/'+filename +' /Users/apple/Downloads/Orders_Data/J95213/Options_Order/'+filename
cmd2 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +':/home/ubuntu/Python_Automation/data/Orders_Data/A987129/Options_Order/'+filename +' /Users/apple/Downloads/Orders_Data/A987129/Options_Order/'+filename
cmd22 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +':/home/ubuntu/Python_Automation/data/Orders_Data/S1604557/Options_Order/'+filename +' /Users/apple/Downloads/Orders_Data/S1604557/Options_Order/'+filename
cmd23 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +':/home/ubuntu/Python_Automation/data/Orders_Data/G304915/Options_Order/'+filename +' /Users/apple/Downloads/Orders_Data/G304915/Options_Order/'+filename
cmd24 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +':/home/ubuntu/Python_Automation/data/Orders_Data/K256027/Options_Order/'+filename +' /Users/apple/Downloads/Orders_Data/K256027/Options_Order/'+filename
cmd25 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +':/home/ubuntu/Python_Automation/data/Signals_Df/'+datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+'_testing.csv'+ ' /Users/apple/Downloads/Orders_Data/Signals_Df/'+datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+'_testing.csv'
cmd4 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +":/home/ubuntu/Python_Automation/data/Signals_Df/"+ datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+".csv"+ ' /Users/apple/Downloads/Orders_Data/Signals_Df/'+datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+'.csv'
cmd5 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +":/home/ubuntu/Python_Automation/data/Orders_Data/J95213/Options_Order/" + (datetime.now(timezone("Asia/Kolkata"))).strftime("%Y-%m-%d")+'/broker_orders.csv'+' /Users/apple/Downloads/Orders_Data/J95213/Options_Order/'+datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+'_broker_orders.csv'
cmd6 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +":/home/ubuntu/Python_Automation/data/Orders_Data/J95213/Options_Order/" + (datetime.now(timezone("Asia/Kolkata"))).strftime("%Y-%m-%d")+'/broker_positions.csv'+' /Users/apple/Downloads/Orders_Data/J95213/Options_Order/'+datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+'_broker_positions.csv'
cmd7 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +":/home/ubuntu/Python_Automation/data/Orders_Data/J95213/Options_Order/" + (datetime.now(timezone("Asia/Kolkata"))).strftime("%Y-%m-%d")+'/orders_summary.csv'+' /Users/apple/Downloads/Orders_Data/J95213/Options_Order/'+datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+'_orders_summary.csv'
cmd8 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +":/home/ubuntu/Python_Automation/data/" + (datetime.now(timezone("Asia/Kolkata"))).strftime("%Y-%m-%d")+'_Monthly_options_data.csv'+' /Users/apple/Downloads/EC2_Edit/' + (datetime.now(timezone("Asia/Kolkata"))).strftime("%Y-%m-%d")+'_Monthly_options_data.csv'

cmd9 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +':/home/ubuntu/Python_Automation/data/Orders_Data/G304915/Options_Order/'+filename +' /Users/apple/Downloads/Orders_Data/G304915/Options_Order/'+filename
cmd10 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +":/home/ubuntu/Python_Automation/data/Orders_Data/G304915/Options_Order/" + (datetime.now(timezone("Asia/Kolkata"))).strftime("%Y-%m-%d")+'/broker_orders.csv'+' /Users/apple/Downloads/Orders_Data/G304915/Options_Order/'+datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+'_broker_orders.csv'
cmd11 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +":/home/ubuntu/Python_Automation/data/Orders_Data/G304915/Options_Order/" + (datetime.now(timezone("Asia/Kolkata"))).strftime("%Y-%m-%d")+'/broker_positions.csv'+' /Users/apple/Downloads/Orders_Data/G304915/Options_Order/'+datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+'_broker_positions.csv'
cmd12 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +":/home/ubuntu/Python_Automation/data/Orders_Data/G304915/Options_Order/" + (datetime.now(timezone("Asia/Kolkata"))).strftime("%Y-%m-%d")+'/orders_summary.csv'+' /Users/apple/Downloads/Orders_Data/G304915/Options_Order/'+datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+'_orders_summary.csv'

cmd13 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +':/home/ubuntu/Python_Automation/data/Orders_Data/S1604557/Options_Order/'+filename +' /Users/apple/Downloads/Orders_Data/S1604557/Options_Order/'+filename
cmd14 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +":/home/ubuntu/Python_Automation/data/Orders_Data/S1604557/Options_Order/" + (datetime.now(timezone("Asia/Kolkata"))).strftime("%Y-%m-%d")+'/broker_orders.csv'+' /Users/apple/Downloads/Orders_Data/S1604557/Options_Order/'+datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+'_broker_orders.csv'
cmd15 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +":/home/ubuntu/Python_Automation/data/Orders_Data/S1604557/Options_Order/" + (datetime.now(timezone("Asia/Kolkata"))).strftime("%Y-%m-%d")+'/broker_positions.csv'+' /Users/apple/Downloads/Orders_Data/S1604557/Options_Order/'+datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+'_broker_positions.csv'
cmd16 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +":/home/ubuntu/Python_Automation/data/Orders_Data/S1604557/Options_Order/" + (datetime.now(timezone("Asia/Kolkata"))).strftime("%Y-%m-%d")+'/orders_summary.csv'+' /Users/apple/Downloads/Orders_Data/S1604557/Options_Order/'+datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+'_orders_summary.csv'

cmd17 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +':/home/ubuntu/Python_Automation/data/Orders_Data/K256027/Options_Order/'+filename +' /Users/apple/Downloads/Orders_Data/K256027/Options_Order/'+filename
cmd18 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +":/home/ubuntu/Python_Automation/data/Orders_Data/K256027/Options_Order/" + (datetime.now(timezone("Asia/Kolkata"))).strftime("%Y-%m-%d")+'/broker_orders.csv'+' /Users/apple/Downloads/Orders_Data/K256027/Options_Order/'+datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+'_broker_orders.csv'
cmd19 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +":/home/ubuntu/Python_Automation/data/Orders_Data/K256027/Options_Order/" + (datetime.now(timezone("Asia/Kolkata"))).strftime("%Y-%m-%d")+'/broker_positions.csv'+' /Users/apple/Downloads/Orders_Data/K256027/Options_Order/'+datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+'_broker_positions.csv'
cmd20 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +":/home/ubuntu/Python_Automation/data/Orders_Data/K256027/Options_Order/" + (datetime.now(timezone("Asia/Kolkata"))).strftime("%Y-%m-%d")+'/orders_summary.csv'+' /Users/apple/Downloads/Orders_Data/K256027/Options_Order/'+datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")+'_orders_summary.csv'

cmd21 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(server) +":/home/ubuntu/Python_Automation/data/"+ (datetime.now(timezone("Asia/Kolkata"))).strftime("%Y-%m-%d")+'_Raw_Signals_Data.csv /Users/apple/Downloads/Reddy_Stocks_Application/data/'

# print(cmd)

call(cmd.split())
call(cmd2.split())
call(cmd3.split())
call(cmd4.split())
call(cmd5.split())
call(cmd6.split())
call(cmd7.split())
call(cmd8.split())

call(cmd9.split())
call(cmd10.split())
call(cmd11.split())
call(cmd12.split())
call(cmd13.split())
call(cmd14.split())
call(cmd15.split())
call(cmd16.split())
call(cmd17.split())

call(cmd18.split())
call(cmd19.split())
call(cmd20.split())
call(cmd21.split())

call(cmd22.split())
call(cmd23.split())
call(cmd24.split())
call(cmd25.split())