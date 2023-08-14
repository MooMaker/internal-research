import requests
import polars as pl


def percentage_diff(col1, col2):
    """
    A function that calculates the percentage difference between two columns.
    """
    return ((col2.sub(col1)).div(col1)).mul(100)



def get_price_at_qty(symbol, qty):
    url_vwap = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=200"
    r_vwap = requests.get(url_vwap)
    data_vwap = r_vwap.json()

    if len(data_vwap["bids"]) != len(data_vwap["asks"]):
        return "value_error"

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
    return bids_df, asks_df


# Call the function with your desired symbol and qty
symbol = "BTCUSDT"
qty = 10
result = get_price_at_qty(symbol, qty)
print("Calculated Price:", result)


def percentage_diff(col1, col2):
    """
    A function that calculates the percentage difference between two columns.
    """
    return ((col2.sub(col1)).div(col1)).mul(100)
