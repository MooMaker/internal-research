import requests
import polars as pl
import pandas as pd
import time


def vwap_one_side(data, qty: int):
    total_qty = 0
    price = None

    for row in data.iterrows():
        row_qty = row[
            1
        ]  # Access the second element (index 1) representing the "qty" column
        row_price = row[
            0
        ]  # Access the first element (index 0) representing the "price" column
        if total_qty + row_qty > qty:
            remaining_qty = qty - total_qty
            price = row_price
            total_qty += remaining_qty
            break
        else:
            total_qty += row_qty

    return price



def get_all_binance_pairs():
    
    host = "https://data.binance.com"
    prefix = "/api/v3/ticker/price"
    r = requests.get(host+prefix)
    data = r.json()
    binance_pairs = pd.DataFrame(data).loc[:,'symbol']
    return binance_pairs


def binance_vwap_estimator(symbol, qty):
    url_vwap = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=5000"
    r_vwap = requests.get(url_vwap)
    data_vwap = r_vwap.json()

    #if len(data_vwap["bids"]) != len(data_vwap["asks"]):
    #    return "value_error"

    bids_data = data_vwap["bids"]
    asks_data = data_vwap["asks"]

    bids_df = pl.DataFrame(
        {
            "price": [float(bid[0]) for bid in bids_data],
            "qty": [float(bid[1]) for bid in bids_data],
        }
    )

    asks_df = pl.DataFrame(
        {
            "price": [float(ask[0]) for ask in asks_data],
            "qty": [float(ask[1]) for ask in asks_data],
        }
    )

    print("Bids DataFrame:")
    print(bids_df)

    print("Asks DataFrame:")
    print(asks_df)

    # Add your logic to calculate price at a given qty here

    # Return the calculated price
    # return bids_df, asks_df

    ## start of new stuff
    bid_price = vwap_one_side(bids_df, qty)
    ask_price = vwap_one_side(asks_df, qty)
    start_bid = bids_df["price"][0]
    start_ask = asks_df["price"][0]

    print("bid_price", bid_price)
    print("ask_price", ask_price)
    print("start_bid", start_bid)
    print("start_ask", start_ask)

    if bid_price is None or ask_price is None:
        print("Unable to calculate deviation due to missing price data")
        return "value_error"

    # Calculating bid deviation % for a given quantity.
    bid_deviation = 100 * ((bid_price - bids_df["price"][0])) / bids_df["price"][0]
    ask_deviation = 100 * ((asks_df["price"][0]) - ask_price) / asks_df["price"][0]

    return bid_price, ask_price, bid_deviation, ask_deviation


def query_binance(symbol: str, timestamp: int, qty: float, is_buy: bool):

    """Queries the binance api to retrieve the price of symbol at a timestamp.
    Timestamp has to be within 1000seconds window ~ 16.66 mins"""

    # check what data this actually returns? is it the midprice??
    # Need to add check that the timestamp is in the last 15mins <1000s from now.

    host = "https://data.binance.com"
    prefix = "/api/v3/klines"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    payload = {"symbol": f"{symbol}", "interval": "1s", "limit": "1000"}
    r = requests.get(host + prefix, params=payload, headers=headers)
    if r.status_code != 200:
        print("API call failed with status code:", r.status_code)
        return "api error"
    data = r.json()
    df = pd.DataFrame(data).filter(items=[0, 4], axis=1)
    df.columns = ["timestamp", "price"]
    df1 = df.loc[df["timestamp"] == timestamp * 1000, "price"]
    if df1.empty:
        print("empty dataframe- no matching timestamp")
        return "no matching timestamp", "no matching timestamp", "no matching timestamp"
    price = float(df1.iloc[0])
    print(f"price for {symbol} is {price}")

    # For the required timestamp, get the last 15 readings before it and return the max
    df2 = df[df["timestamp"] < timestamp * 1000].tail(30)
    max_price_in_last_30_readings = float(df2["price"].max())
    min_price_in_last_30_readings = float(df2["price"].min())
    print("max_price_in_last_15_readings", max_price_in_last_30_readings)
    print("min_price_in_last_15_readings", min_price_in_last_30_readings)

    # adjust price by vwap estimate
    print("qty", qty)
    ob = binance_vwap_estimator(symbol, qty)
    
    if ob != "value_error":
        if is_buy:
            price_adjust = ob[3]
            price_final = (1 - price_adjust / 100) * price
            price_final_max = (1 - price_adjust / 100) * max_price_in_last_30_readings
            price_final_min = (1 - price_adjust / 100) * min_price_in_last_30_readings

        else:
            price_adjust = ob[2]
            price_final = (price_adjust / 100 + 1) * price
            price_final_max = (price_adjust / 100 + 1) * max_price_in_last_30_readings
            price_final_min = (price_adjust / 100 + 1) * min_price_in_last_30_readings
    else:
        return "value_error", "value_error", "value_error"

    print(
        "price_final",
        price_final,
        " price_final_max",
        price_final_max,
        " price_final_min",
        price_final_min,
    )
    return price_final, price_final_max, price_final_min





'''

# Call the function with your desired symbol and qty
symbol = "BTCUSDT"
qty = 100
bid_price, ask_price, bid_deviation, ask_deviation = binance_vwap_estimator(symbol, qty)
print("bid_price", bid_price)
print("asks_price", ask_price)
print("bid_deviation, ", bid_deviation)
print("ask_deviation, ", ask_deviation)

current_timestamp = int(time.time())
timestamp = current_timestamp - 10
print("current_timestamp", current_timestamp)
print(timestamp)
# add timestamp of 15min limit (15 min = 900 seconds)
limit_timestamp = current_timestamp - 900
query_binance(symbol, timestamp, 100, False)


# bid_price = vwap_one_side(bids, 10)
# print('bid_price', bid_price)

'''