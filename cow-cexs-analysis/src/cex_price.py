import pandas as pd 
import time
from globals import trades_analyzed
from binance import get_all_binance_pairs, query_binance
from utils import percentage_diff


def row_binance(sellTokenSymbol: str, buyTokenSymbol: str, timestamp: int, sellTokenQty:float, buyTokenQty:float):

	"""function to be used on the cow trades dataframe. Takes values from sellTokenSymbol 
	and buyTokenSymbol and timestamp to check the binance price for that trade. 

	Defines market_price as the price of sell token / price of buy token  
	Gets price of sell token and price of buy token from binance seperately

	Checks if token is USDT or a token that exists in Binance. Otherwise returns False 
	and not able to retrieve a price for that trade"""

	# Get all current binance pairs 
	binance_pairs = get_all_binance_pairs()

	sell_pair = f'{sellTokenSymbol}USDT'
	buy_pair = f'{buyTokenSymbol}USDT'
	print('sell_pair: ', sell_pair)
	print('buy_pair: ', buy_pair)
	# retrieve sell token price  

	if sell_pair == 'USDTUSDT':
		sell_token_price = 1 
		sell_token_price_max = 1
		sell_token_price_min = 1 
        
    # check this logic again 
	elif sell_pair == 'DAIUSDT':
		usdtdai, usdtdai_max, usdtdai_min = query_binance('USDTDAI', timestamp, sellTokenQty, False)
		print('usdtdai result', usdtdai)
		if usdtdai != 'value_error' and type(usdtdai)==float and usdtdai!= 0 :
			sell_token_price = 1 / usdtdai
			sell_token_price_max = 1/ usdtdai_min
			sell_token_price_min = 1/usdtdai_max
		else:
			return 'value_error', 'value_error'        
        
    
	elif sell_pair == 'WETHUSDT':
		sell_token_price, sell_token_price_max, sell_token_price_min = query_binance('ETHUSDT', timestamp,sellTokenQty, False)
        
	elif sell_pair == 'WBTCUSDT':
		sell_token_price, sell_token_price_max, sell_token_price_min = query_binance('BTCUSDT', timestamp, sellTokenQty, False)
        
	elif sell_pair in binance_pairs.values:
		sell_token_price, sell_token_price_max, sell_token_price_min = query_binance(sell_pair, timestamp, sellTokenQty, False)
        
	else: 
		return 'sell_unavailable' , 'sell_unavailable'

	# retrieve buy token price 
    
	if buy_pair =='USDTUSDT':
		buy_token_price = 1 
		buy_token_price_max = 1
		buy_token_price_min = 1 
        
        
	elif buy_pair == 'DAIUSDT':
		usdtdai, usdtdai_max, usdtdai_min = query_binance('USDTDAI', timestamp, buyTokenQty, True)
		if usdtdai != 'value_error' and float(usdtdai) == 0 and usdtdai!=0 :
			buy_token_price = 1 / usdtdai
			buy_token_price_max = 1/usdtdai_min
			buy_token_price_min = 1/usdtdai_max
		else:
			return 'value_error', 'value_error'
    
	elif buy_pair == 'WETHUSDT':
		buy_token_price, buy_token_price_max, buy_token_price_min = query_binance('ETHUSDT', timestamp, buyTokenQty, True)
        
	elif buy_pair == 'WBTCUSDT':
		buy_token_price, buy_token_price_max, buy_token_price_min = query_binance('BTCUSDT', timestamp, buyTokenQty, True)
        
	elif buy_pair in binance_pairs.values:
		buy_token_price, buy_token_price_max, buy_token_price_min = query_binance(buy_pair, timestamp, buyTokenQty, True)
        
	else:
		return 'buy_unavailable', 'buy_unavailable'

	# calculate trade pair price 
	if buy_token_price != 'value_error' and buy_token_price != 'no matching timestamp':
		if sell_token_price != 'value_error' and sell_token_price != 'no matching timestamp':
			market_price = buy_token_price / sell_token_price 
			worst_market_price = buy_token_price_max / sell_token_price_min
			print(f'{sellTokenSymbol}/{buyTokenSymbol} binance price:', market_price)
			print('worst_market_price', worst_market_price)
			return market_price , worst_market_price
		else:
			return 'value_error', 'value_error'
	else: 
		return 'value_error', 'value_error'


def binance_prices(input_df): 

	# TODO: Change all pandas to polars 

	# Convert Polars DataFrame to pandas DataFrame
	input_df = input_df.to_pandas()

	# Initiate columns to be written in loop 
	input_df['binance_price'] = 0.000
	input_df['worst_binance_price'] = 0.00 
	input_df = input_df.reset_index(drop=True)


	#input_df.to_csv('final_test.csv', index=False)

	# Loop through each row of the dataframe.
	for i, row in input_df.iterrows():
	    # Retrieve the trades_id, timestamp, sell token symbol, and buy token symbol from the row.
	    trade_id = row[2]
	    print('trade_id', trade_id)
	    timestamp = row[5]
	    sell_token_symbol = row[10]
	    buy_token_symbol = row[13]
	    sell_token_qty = row[14]
	    buy_token_qty = row[15]
	    timestamp_now = int(time.time())
	    
	    # Check first if timestamp is within 1000s of now to avoid panick. If it is old, then return 'old timestamp' 
	    if abs(timestamp - timestamp_now) < 1000: 
	        if trade_id in trades_analyzed:
	        	input_df.iloc[i, 17] = 'repeat' 
	        	input_df.iloc[i,18] = 'repeat'
	        	continue

	        else: 
	            # Use the pair_price_binance function to calculate the binance price and store it in the dataframe.
	            input_df.iloc[i, 17], input_df.iloc[i,18] = row_binance(sell_token_symbol, buy_token_symbol, timestamp, sell_token_qty, buy_token_qty)
	            trades_analyzed.add(trade_id)
	            print('i,17: ', input_df.iloc[i,17])
	            print('i,18: ', input_df.iloc[i,18])

 
	    else:
	        input_df.iloc[i,17] = 'timeout'
	        input_df.iloc[i,18] = 'timeout'

	    

	# Filter out trades that do not have a symbol in the subgraph
	input_df = input_df[input_df['binance_price'] != 'repeat']
	input_df = input_df[input_df['binance_price'] != 'timeout']
	input_df = input_df[input_df['binance_price'] != 'buy_unavailable']
	input_df = input_df[input_df['binance_price'] != 'sell_unavailable']
	input_df = input_df[input_df['binance_price'] != 'value_error']

	# Define a percentage difference function to get percentage difference between binance price and cow price 


	# Create a new column in the dataframe that stores the percentage difference between the sell amount and the sell amount on Binance.
	input_df['percentage_diff'] = percentage_diff(
	    input_df['cow_price'], 
	    input_df['binance_price']
	)


	# Create a new column in the dataframe that stores the percentage difference between the sell amount and the sell amount on Binance.
	input_df['percentage_diff_worst'] = percentage_diff(
	    input_df['cow_price'], 
	    input_df['worst_binance_price']
	)

	# Filter out rows that have a difference higher than 100% as its likely to be a different token alltogether
	# an example is LIT which is Litentry on Binance but Timeless on COW. Unfortunately binance api does not allow
	# one to validate by token address only by string symbol. Its not perfect way to do it but at least filters the obvious ones out
	input_df = input_df[abs(input_df['percentage_diff']) < 100]

	print('input_df', input_df)

	print('adding binance prices to table complete')

	return input_df
