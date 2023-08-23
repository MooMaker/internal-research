import requests
import time
import polars as pl
from queryportal.subgraphinterface import SubgraphInterface


# Mainnet CoW subgraph: 'https://api.thegraph.com/subgraphs/name/cowprotocol/cow'
# GC CoW subgraphh: 'https://api.thegraph.com/subgraphs/name/cowprotocol/cow-gc'


# Load subgraph endpoint for a given subgraph


def get_trades_from_graph(subgraph_link: str):

    # Extract name of subgraph from link. (Last word)
    subgraph_name = subgraph_link.split('/')[-1]
    

    sgi = SubgraphInterface(endpoints=[subgraph_link])

    # Extract name of subgraph from link. (Last word)
    name = subgraph_link.split('/')[-1]

    # Query Params
    current_timestamp = int(time.time())
    # add timestamp of 15min limit (15 min = 900 seconds)
    limit_timestamp = current_timestamp - 900

    # query search filter params
    filter = {
        "timestamp_gte": int(limit_timestamp),
    }

    # query size
    query_size = 1250

    # query columns
    query_paths = [
        "txHash",
        "timestamp",
        "gasPrice",
        "feeAmount",
        "txHash",
        "settlement_id",
        "sellAmount",
        "sellToken_decimals",
        "buyAmount",
        "buyToken_decimals",
        "sellToken_id",
        "buyToken_id",
        "order_id",
        "sellToken_symbol",
        "buyToken_symbol",
    ]

    trades_df = sgi.query_entity(
        query_size=query_size,
        entity="trades",
        name=f"{subgraph_name}",
        filter_dict=filter,
        query_paths=query_paths,
        orderBy="timestamp",
        graphql_query_fmt=True,
    )

    print('test', type(trades_df))

    if type(trades_df) == type(None): 
        print("No trades found. Exiting get_trades_from_graph function.")
        return pl.DataFrame({})


    #elif type(trades_df) == pl.DataFrame:
    #    print('polars')

    if subgraph_link == "https://api.thegraph.com/subgraphs/name/cowprotocol/cow":
        trades_df = trades_df.with_columns(
            pl.when(
                trades_df["buyToken_id"] == "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            )
            .then("ETH")
            .otherwise(trades_df["buyToken_symbol"])
            .alias("buyToken_symbol")
        )
    elif subgraph_link == "https://api.thegraph.com/subgraphs/name/cowprotocol/cow-gc":
        trades_df = trades_df.with_columns(
            pl.when(
                trades_df["buyToken_id"] == "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            )
            .then("DAI")
            .otherwise(trades_df["buyToken_symbol"])
            .alias("buyToken_symbol")
        )

    # normalize around the right decimals since each token amount is in  its own decimal (eg. USDC is 6 decimals)
    trades_df = trades_df.with_columns(
        (pl.col("sellAmount") / 10 ** pl.col("sellToken_decimals")).alias(
            "sell_amount_right_decimal"
        ),
        (pl.col("buyAmount") / 10 ** pl.col("buyToken_decimals")).alias(
            "buy_amount_right_decimal"
        ),
        ((pl.col("sellAmount")-pl.col("feeAmount")) / 10 ** pl.col("sellToken_decimals")).alias(
            "sell_amount_right_decimal_no_fee"
        )

    )


    # Add a column with CoW price 
    trades_df = trades_df.with_columns(
        [
            (
                pl.col("sell_amount_right_decimal") / pl.col("buy_amount_right_decimal")
            ).alias("cow_price")
        ]
    )

    
    # Add a column with CoW price 
    trades_df = trades_df.with_columns(
        [
            (
                pl.col("sell_amount_right_decimal_no_fee") / pl.col("buy_amount_right_decimal")
            ).alias("cow_price_no_fee")
        ]
    )


    #trades_df.write_csv("raw_data.csv")
    print("fetch trades from the graph complete")
    return trades_df


#get_trades_from_graph("https://api.thegraph.com/subgraphs/name/cowprotocol/cow-gc")
