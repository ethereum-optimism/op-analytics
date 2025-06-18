# CoinGecko Price Data Ad Hoc Run

# 1. Load chain metadata and get CoinGecko token IDs
from op_analytics.datapipeline.chains.load import load_chain_metadata
import polars as pl
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()

# Load and inspect metadata
meta = load_chain_metadata()
print("Metadata shape:", meta.shape)
print("\nColumns with CoinGecko data:")
coingecko_data = meta.select(["chain_name", "cgt_coingecko_api"]).filter(pl.col("cgt_coingecko_api").is_not_null())
print(coingecko_data)

# Get token IDs
token_ids = meta.filter(pl.col("cgt_coingecko_api").is_not_null())["cgt_coingecko_api"].unique().to_list()
print("\nNumber of token IDs found:", len(token_ids))
print("First few token IDs:", token_ids[:5])

# 2. Fetch CoinGecko price data for a small subset of tokens
from op_analytics.datasources.coingecko.price_data import CoinGeckoDataSource
import requests

# Create a session with debug logging
session = requests.Session()
data_source = CoinGeckoDataSource(session=session)

# Try with just one token first to debug
test_token = token_ids[0] if token_ids else None
if test_token:
    print(f"\nTesting with single token: {test_token}")
    try:
        prices = data_source.get_token_prices([test_token], days=7)
        print("Price data shape:", prices.shape)
        print("Price data columns:", prices.columns)
        print("\nFirst few rows:")
        print(prices.head())
    except Exception as e:
        print(f"Error fetching prices: {str(e)}")
else:
    print("No token IDs found in metadata!")

# 3. Write the fetched price data to the production BigQuery table
from op_analytics.datasources.coingecko.dataaccess import CoinGecko
from op_analytics.coreutils.partitioned.dailydatawrite import write_to_prod

with write_to_prod():
    CoinGecko.DAILY_PRICES.write(
        dataframe=prices,
        sort_by=["token_id"],
    )

# 4. Optionally, read back from production to verify
view = CoinGecko.DAILY_PRICES.read(min_date=prices["dt"].min())
view.head() 
