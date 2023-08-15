from queryportal.subgraphinterface import SubgraphInterface
from datetime import datetime, timedelta
import polars as pl
import pandas as pd
import requests
import time
import os


from globals import trades_analyzed, mainnet_matching_trades
from thegraph import get_trades_from_graph
from cex_price import binance_prices

pl.Config.set_fmt_str_lengths(200)


# Run loop every 15 mins
# Get CoW trades from Gnosis + Mainnet 
# Process Mainnet dataframe -> produce table 
# Process Gnosis dataframe -> Produce table 

while(True):


	######## GET COW TRADES in last 15 mins from subgraph ########

	# Call the graph - get dataframes 
	cow_mainnet_df = get_trades_from_graph('https://api.thegraph.com/subgraphs/name/cowprotocol/cow')
	#cow_gnosis_df = get_trades_from_graph('https://api.thegraph.com/subgraphs/name/cowprotocol/cow-gc')

	'''	
	# Gnosis Chain CoW 
	if cow_gnosis_df != None:
		cow_gnosis_df = binance_prices(cow_gnosis_df)
		# Write mainnet dataframe into data store: 
		if not os.path.isfile('mainnet_cow_binance_data.csv'):
			cow_gnosis_df.to_csv('gnosis_cow_binance_data.csv', mode='w', header=True, index=False)
		else:
			cow_gnosis_df.to_csv('gnosis_cow_binance_data.csv', mode='a', header=False, index=False)

		gnosis_matching_trades = gnosis_matching_trades + len(cow_gnosis_df)

	elif cow_gnosis_df == None:
		print('No CoW Gnosis Chain trades in the last 15mins')
		
	'''

	#print(cow_mainnet_df)

	# Call binance_price function that takes df as input and adds CoW price for each row:
	cow_mainnet_df = binance_prices(cow_mainnet_df)

	# Prepare both dataframes for reading by front end. 


	# Write mainnet dataframe into data store: 
	if not os.path.isfile('mainnet_cow_binance_data.csv'):
		cow_mainnet_df.to_csv('mainnet_cow_binance_data.csv', mode='w', header=True, index=False)
	else:
		cow_mainnet_df.to_csv('mainnet_cow_binance_data.csv', mode='a', header=False, index=False)

	mainnet_matching_trades = mainnet_matching_trades + len(cow_mainnet_df)


	'''
	# Write Gnosis Chain dataframe into data store: 
	if not os.path.isfile('GC_cow_binance_data.csv'):
		cow_mainnet_df.to_csv('GC_cow_binance_data.csv', mode='w', header=True, index=False)
	else:
		cow_mainnet_df.to_csv('GC_cow_binance_data.csv', mode='a', header=False, index=False)

	mainnet_matching_trades = mainnet_matching_trades + len(cow_mainnet_df)

	'''
	#To do : return the right required data
	# macro data for this run
	print('total cow trades so far: ', len(trades_analyzed))
	print('total number of matching trades appended to data: ', mainnet_matching_trades)


	# time the loop to run after 14mins of end of previous run. Though if there was a way to actually time it in 15mins intervals
	# that would be more efficient
	time.sleep(840)





