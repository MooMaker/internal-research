import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import warnings
import os 

# Ignore warnings from showing on the app 
warnings.filterwarnings("ignore")


def run_app():

	@st.cache(ttl=900) # set TTL (time to live) to 900 seconds (15minutes)

	# Define the function to read the data and return the latest 50 readings
	def get_latest_data(file_name: str, period:int):
		df = pd.read_csv(f'../data/{file_name}')
		latest_data = df.tail(period)
		latest_data['timestampx'] = pd.to_datetime(latest_data['timestamp'], unit='s')
		latest_data['timestampx'] = latest_data['timestampx'].dt.strftime('%Y-%m-%d %H:%M:%S')
		return latest_data

	def get_macro_metric(file_name:str):
		df = pd.read_csv(f'../data/{file_name}')
		total = len(df)
		cow_better = len(df[df['percentage_diff'] > 0])
		cow_worse = len(df[df['percentage_diff'] < 0])
		return total, cow_worse, cow_better

	def count_by_day(file_name:str):
		df = pd.read_csv(f'../data/{file_name}')
		df['timestampx'] = pd.to_datetime(df['timestamp'], unit='s')
		df['timestampday'] = df['timestampx'].dt.strftime('%Y-%m-%d')
		mini_df = df['timestampday'].value_counts()
		return mini_df

	def count_positive_percentage(file_name:str):
		df = pd.read_csv(f'../data/{file_name}')
		df['timestampx'] = pd.to_datetime(df['timestamp'], unit='s')
		df['timestampday'] = df['timestampx'].dt.strftime('%Y-%m-%d')
		df['percentage_diff'] = pd.to_numeric(df['percentage_diff'], errors='coerce')
		#positive_count = df.groupby('timestampday')['percentage_diff'].gt(0).sum()
		positive_counts = df.groupby('timestampday')['percentage_diff'].apply(lambda x: (x < 0).sum())
		# needs to return with date grouped first
		print('count', positive_counts)
		return positive_counts


	def get_total_cow_trades(file_name:str):
		df = pd.read_csv(f'../data/{file_name}')
		total_cow_trades = len(df)
		return total_cow_trades


	## START OF APP ## 

	# Write details 
	st.title('COW-Binance price compare')
	st.write("Methodology\n\nThe code for this app and script to fetch data are available on: github.com/moomaker/internal-research\n\nThe price is defined as the (sell/buy) hence a higher number is a worse price.\nA positive price difference indicates COW price is better for the user by the percentage difference")
	#st.write('This table shows the latest 50 readings:')
	# Include how variables in the table were calculated. 

	# Fetch latest Data from data store
	mainnet_latest_data_table1 = get_latest_data('../data/mainnet_cow_binance_data.csv', 50)
	gnosis_latest_data_table1 = get_latest_data('../data/gnosis_cow_binance_data.csv', 50)
	mainnet_latest_data_graph = get_latest_data('../data/mainnet_cow_binance_data.csv', 200)
	gnosis_latest_data_graph = get_latest_data('../data/gnosis_cow_binance_data.csv', 200)


	### Mainnet stuff 

	st.write('Mainnet:')

	# Component1: Macro Table / Display a table with 
	binance = count_by_day('../data/mainnet_cow_binance_data.csv')
	raw = count_by_day('../data/cow_raw_cow_data.csv')
	positive = count_positive_percentage('../data/mainnet_cow_binance_data.csv')
	merged_series = pd.concat([raw, binance, positive], axis=1)
	merged_series.columns = [
	    'Total CoW Trades',
	    'Trades with Binance Tokens',
	    'Total Binance Price < Cow Price'
	]
	merged_series['% of Binance Price better than CoW price'] = (
    (merged_series['Total Binance Price < Cow Price'] / merged_series['Total CoW Trades']) * 100
	)
	st.table(merged_series)
	
	# Component2: Display the table with the latest data
	st.table(mainnet_latest_data_table1[['timestamp', 'timestampx','txHash', 'sellToken_symbol', 'buyToken_symbol','cow_price_no_fee', 'binance_price', 'percentage_diff', 'worst_binance_price', 'percentage_diff_worst']])
	
	# Component3: Graph visual of percent diff 
	chart_data = mainnet_latest_data_graph[['timestamp', 'percentage_diff']]   
	chart_data.set_index('timestamp', inplace=True)

	'''
	# Display the number of rows and counts of positive/negative percentage_diff
	total, cow_worse, cow_better = get_macro_metric('mainnet_cow_binance_data.csv')
	st.write('Total number of rows:', total)
	st.write('Number of rows with positive percentage_diff:', cow_better)
	st.write('Number of rows with negative percentage_diff:', cow_worse)
	'''
	
	# Create a button to download the data
	st.download_button(
		label='Download mainnet data',
		data=pd.read_csv('../data/mainnet_cow_binance_data.csv').to_csv(index=False),
		file_name='mainnet_cow_binance_data.csv',
		mime='text/csv'
	)

	# Create bar plot
	fig, ax = plt.subplots()
	chart_data.plot(kind='bar', ax=ax)	
	# Set plot labels
	ax.set_xticklabels([])
	ax.set_xlabel('Timestamp')
	ax.set_ylabel('Percentage Difference')
	# Show plot
	st.pyplot(fig)


	### Gnosis stuff 

	st.write('Gnosis Chain:')

	# Component1: Macro Table / Display a table with 
	gc_binance = count_by_day('../data/gnosis_cow_binance_data.csv')
	gc_raw = count_by_day('../data/cow-gc_raw_cow_data.csv')
	gc_positive = count_positive_percentage('../data/gnosis_cow_binance_data.csv')
	gc_merged_series = pd.concat([gc_raw, gc_binance, gc_positive], axis=1)
	gc_merged_series.columns = [
	    'Total CoW Trades',
	    'Trades with Binance Tokens',
	    'Total Binance Price < Cow Price'
	]
	gc_merged_series['% of Binance Price better than CoW price'] = (
    (gc_merged_series['Total Binance Price < Cow Price'] / merged_series['Total CoW Trades']) * 100
	)
	st.table(gc_merged_series)


	# Display the table with the latest data
	st.table(gnosis_latest_data_table1[['timestamp', 'timestampx','txHash', 'sellToken_symbol', 'buyToken_symbol','cow_price_no_fee', 'binance_price', 'percentage_diff', 'worst_binance_price', 'percentage_diff_worst']])
	
	# Create the chart with the percentage difference
	gnosis_chart_data = gnosis_latest_data_graph[['timestamp', 'percentage_diff']]   
	gnosis_chart_data.set_index('timestamp', inplace=True)


	'''
	# Display the number of rows and counts of positive/negative percentage_diff
	gnosis_total, gnosis_cow_worse, gnosis_cow_better = get_macro_metric('../data/gnosis_cow_binance_data.csv')
	st.write('Total number of rows:', total)
	st.write('Number of rows with positive percentage_diff:', gnosis_cow_better)
	st.write('Number of rows with negative percentage_diff:', gnosis_cow_worse)
	'''

	# Create a button to download the data
	st.download_button(
		label='Download gnosis data',
		data=pd.read_csv('../data/gnosis_cow_binance_data.csv').to_csv(index=False),
		file_name='gnosis_cow_binance_data.csv',
		mime='text/csv'
	)

	# Create bar plot
	fig, ax = plt.subplots()
	gnosis_chart_data.plot(kind='bar', ax=ax)	
	# Set plot labels
	ax.set_xticklabels([])
	ax.set_xlabel('Timestamp')
	ax.set_ylabel('Percentage Difference')
	# Show plot
	st.pyplot(fig)
	

if __name__ == '__main__':
	run_app()