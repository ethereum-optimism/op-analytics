# CoinGecko Price Data Ad Hoc Run

# 1. Load chain metadata and get CoinGecko token IDs
from op_analytics.datapipeline.chains.load import load_chain_metadata
import polars as pl

meta = load_chain_metadata()
meta.head()

token_ids = meta.filter(pl.col("cgt_coingecko_api_key").is_not_null())["cgt_coingecko_api_key"].unique().to_list()
token_ids[:5]

# 2. Fetch CoinGecko price data for a small subset of tokens
from op_analytics.datasources.coingecko.price_data import CoinGeckoDataSource

data_source = CoinGeckoDataSource()
prices = data_source.get_token_prices(token_ids[:3], days=7)
prices.head()

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
