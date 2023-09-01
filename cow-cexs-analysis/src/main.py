from queryportal.subgraphinterface import SubgraphInterface
from datetime import datetime, timedelta
import polars as pl
import pandas as pd
import requests
import time
import os


from globals import trades_analyzed, mainnet_matching_trades, gnosis_matching_trades
from thegraph import get_trades_from_graph
from cex_price import binance_prices

pl.Config.set_fmt_str_lengths(200)


# Write mainnet DataFrame into data store
data_folder_path = '../data/'
mainnet_data_file_path = os.path.join(data_folder_path, 'mainnet_cow_binance_data.csv')
gnosis_data_file_path = os.path.join(data_folder_path, 'gnosis_cow_binance_data.csv')


# Run loop every 15 mins
# Get CoW trades from Gnosis + Mainnet 
# Process Mainnet dataframe -> produce table 
# Process Gnosis dataframe -> Produce table 


#df = pd.read_csv('../data/mainnet_cow_binance_data.csv')
#print(df)


while(True):


	######## GET COW TRADES in last 15 mins from subgraph ########

	# Mainnet CoW 
	print('################start mainnet####################')
	cow_mainnet_df_raw = get_trades_from_graph('https://api.thegraph.com/subgraphs/name/cowprotocol/cow')
	#print('test', type(cow_mainnet_df_raw))
	if not cow_mainnet_df_raw.is_empty():
		cow_mainnet_df = binance_prices(cow_mainnet_df_raw)
		# Write mainnet dataframe into data store:
		if not os.path.isfile(mainnet_data_file_path):
			cow_mainnet_df.to_csv(mainnet_data_file_path, mode='w', header=True, index=False)
		else:
			# Append to the existing data file
			cow_mainnet_df.to_csv(mainnet_data_file_path, mode='a', header=False, index=False)

		mainnet_matching_trades = mainnet_matching_trades + len(cow_mainnet_df)

	elif cow_mainnet_df_raw.is_empty():
		cow_mainnet_df = pl.DataFrame({})
		print('No CoW Mainnet Chain trades in the last 15mins')
		
	
	# Gnosis Chain CoW 
	print('#########start gnosis################')
	cow_gnosis_df_raw = get_trades_from_graph('https://api.thegraph.com/subgraphs/name/cowprotocol/cow-gc')
	if not cow_gnosis_df_raw.is_empty():
		cow_gnosis_df = binance_prices(cow_gnosis_df_raw)
		# Write mainnet dataframe into data store: 
		if not os.path.isfile(gnosis_data_file_path):
			cow_gnosis_df.to_csv(gnosis_data_file_path, mode='w', header=True, index=False)
		else:
			# Append to the existing data file
			cow_gnosis_df.to_csv(gnosis_data_file_path, mode='a', header=False, index=False)

		gnosis_matching_trades = gnosis_matching_trades + len(cow_gnosis_df)

	elif cow_gnosis_df_raw.is_empty():
		cow_gnosis_df = pl.DataFrame({})
		print('No CoW Gnosis Chain trades in the last 15mins')
	


	#To do : return the right required data

	# Total trades Mainnet graph in last 15mins
	# Total trades matched mainet-binance
	# total trades gnosis graph in last 15mins
	# total trades matched gnosis-binance in last 15mins


	# macro data for this run
	print('total cow trades so far: ', len(trades_analyzed))
	print('##### Mainnet #####')
	print('total cow mainnet trades in the last 15mins: ', len(cow_mainnet_df_raw))
	print('total number of mainnet trades obtained binance price for: ', len(cow_mainnet_df))
	print('##### Gnosis #####')
	print('total cow gnosis trades in the last 15mins: ', len(cow_gnosis_df_raw))
	print('total number of gnosis trades obtained binance prices for ', len(cow_gnosis_df))


	# time the loop to run after 14mins of end of previous run. Though if there was a way to actually time it in 15mins intervals
	# that would be more efficient
	time.sleep(840)





