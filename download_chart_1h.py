import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import os
import glob
import pandas as pd
import csv
from csv import writer

#========================
#Filename format: BTCUSDT-1h-yyyy-mm-dd.zip
#Binance only provide 1h data till 2021-March-1st
#========================


#========================
#Functions
#========================
date_format = "%Y-%m-%d"
current_datetime = datetime.utcnow()
current_date = date.today()
binance_earliest_date = date(2021,3,1)
delta = current_date - binance_earliest_date
#print (delta.days) #TO CALCULATE DAYS FROM TODAY TILL 1st March 2021

print ("Files will be downloaded and extracted to current directory folder \"Downloaded_Data\".\n")

curr_dir = os.getcwd()

def dl_extract_hourly_data():
	headerList = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'QAV', 'No. of trades', 'Taker buy base', 'Taker buy quote', 'Ignore']
	
	#Remove all files previously downloaded so it don't get appended into combined csv
	print ('\nClearing directory of previous download files...\n')
	try:
		for filename in glob.glob(curr_dir + '\\Downloaded_Data\\BTCUSDT-1h*'):
			os.remove(filename)

		os.remove(curr_dir + '\\Downloaded_Data\\Combined.csv')
		print ('\nCleared...\n')
	except:
		pass

	for x in range(3):

		date_time_desc = current_datetime - timedelta(days = x+1)
		date_time_desc_formatted = datetime.strftime(date_time_desc, "%Y-%m-%d")
		filename_zip = 'BTCUSDT-1h-' + date_time_desc_formatted + '.zip'
		filename_csv = 'BTCUSDT-1h-' + date_time_desc_formatted + '.csv'
		url = 'https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1h/' + filename_zip
		#r = requests.get(url, allow_redirects=True)

		print ("Download and extracting daily hourly data for " + date_time_desc_formatted)
		
		with urlopen(url) as zipresp:
			with ZipFile(BytesIO(zipresp.read())) as zfile:
				zfile.extractall(curr_dir + '\\Downloaded_Data')
		
		os.chdir(curr_dir + '\\Downloaded_Data')

		#Read file and specify header, then append header the next. Else headerList will overwrite data in first row.
		file = pd.read_csv(filename_csv, header=None)
		file.to_csv(filename_csv, header=headerList, index=False)
	
'''
Each Kline hourly CSV data starts from 12am, to 11pm of that day.

Binance Klines Header CSV:
- Open time
- Open
- High
- Low
- Close
- Volume
- Close time
- Quote asset volume
- Number of trades
- Taker buy base asset volume
- Taker buy quote asset volume
- Ignore
'''

'''
1) Append all CSV data into one sheet.
2) Sort column A (Unix Epoch timestamp) -> Earliest to latest (Ascending)
3) Column A into custom format (Date + time)
5) Collect data from first moons, every cycle = 14 days? (Double check TV)
4) Insert extra column
	Column Name: New Moon/Full Moon
	Values: Sell/Buy
5) Based on Moon cycle, fill in Sell/Buy on each new cycle.
6) Calculate total profits if an individual were to buy/sell x hours after each cycle. Determine average.
'''
#====
#1
#====
def concat_csv():
	print ("\nDownload Completed. Combining all CSVs into \"Combined_csv.csv\"")
	os.chdir(curr_dir + '\\Downloaded_Data')
	
	extension = 'csv'
	all_filenames = glob.glob("*.csv")
	#Combine all files in the list
	
	df = pd.concat(map(pd.read_csv, all_filenames), ignore_index=True)
	df.to_csv("Combined.csv", index=False)
	
#====================
#Functions
#====================
dl_extract_hourly_data()
concat_csv()