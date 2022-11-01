import logging
from datetime import datetime
from pytz import timezone
  
def main():
    # Configure the logging system
    logging.basicConfig(filename ='/Users/apple/Desktop/Python_Stocks_Automation/app_'+ str(datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%d")) +'.log',
                        level = logging.ERROR)
      
    # Variables (to make the calls that follow work)
    hostname = 'www.python.org'
    item = 'spam'
    filename = 'data.csv'
    mode = 'r'
      
    # Example logging calls (insert into your program)
    logging.critical('Host %s unknown', hostname)
    logging.info('Hi Titania')
    logging.error("Couldn't find %r", item)
    logging.warning('Feature is deprecated')
    logging.info('Opening file %r, mode = %r', filename, mode)
    logging.debug('Got here')
      
if __name__ == '__main__':
	main()