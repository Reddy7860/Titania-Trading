from subprocess import call
import datetime
from datetime import datetime,timedelta
from pytz import timezone 

start_time = datetime.now(timezone("Asia/Kolkata"))
print("Script execution started")
print(start_time)

server = 'ubuntu@ec2-52-66-247-124.ap-south-1.compute.amazonaws.com'

# path1 = '/Users/apple/Desktop/Python_Stocks_Automation/Options_data/Nifty/' + (datetime.now()+ timedelta(days =-1)).strftime('%Y-%m-%d')+'_Futures_Options_Signals.csv'
path1 = '/Users/apple/Desktop/Python_Stocks_Automation/Options_data/Nifty/' + datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d')+'_Futures_Options_Signals.csv'
path2 = '/Users/apple/Desktop/Python_Stocks_Automation/Options_data/Nifty/' + datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d')+'_Options_Signals.csv'
path3 = '/Users/apple/Desktop/Python_Stocks_Automation/Options_data/BankNifty/' + datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d')+'_Futures_Options_Signals.csv'
path4 = '/Users/apple/Desktop/Python_Stocks_Automation/Options_data/BankNifty/' + datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d')+'_Options_Signals.csv'

cmd1 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(path1) + ' '+ str(server) +':/home/ubuntu/Options_Chain/Nifty/'
cmd2 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(path2) + ' '+ str(server) +':/home/ubuntu/Options_Chain/Nifty/'
cmd3 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(path3) + ' '+ str(server) +':/home/ubuntu/Options_Chain/BankNifty/'
cmd4 = 'scp -i /Users/apple/Downloads/flask_app.pem '+ str(path4) + ' '+ str(server) +':/home/ubuntu/Options_Chain/BankNifty/'

call(cmd1.split())
call(cmd2.split())
call(cmd3.split())
call(cmd4.split())

end_time = datetime.now(timezone("Asia/Kolkata"))

print(end_time)

print('Duration: {}'.format(end_time - start_time))