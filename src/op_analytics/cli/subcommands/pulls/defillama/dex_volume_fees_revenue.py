from dataclasses import dataclass
from typing import Optional

from .dataaccess import DefiLlama

import polars as pl
import json

from op_analytics.coreutils.bigquery.write import (
    most_recent_dates,
    upsert_partitioned_table,
    upsert_unpartitioned_table,
)
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import dt_fromepoch, now_dt



log = structlog.get_logger()

# ENDPOINT_NAMES = ['Volume','Fees'] # If we eventually want to standardize further - since the endpoints follow the same format

DEX_SUMMARY_ENDPOINT = "https://api.llama.fi/overview/dexs?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=true&dataType=dailyVolume"
DEX_CHAIN_ENDPOINT = "https://api.llama.fi/overview/dexs/{chain_name}?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=false&dataType=dailyVolume"

FEE_SUMMARY_ENDPOINT = "https://api.llama.fi/overview/fees?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=true&dataType=dailyFees"
FEE_CHAIN_ENDPOINT = "https://api.llama.fi/overview/fees/{chain_name}?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=false&dataType=dailyFees"

REVENUE_SUMMARY_ENDPOINT = "https://api.llama.fi/overview/fees?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=true&dataType=dailyRevenue"
REVENUE_CHAIN_ENDPOINT = "https://api.llama.fi/overview/fees/{chain_name}?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=false&dataType=dailyRevenue"

BQ_DATASET = "uploads_api"

TABLE_LAST_N_DAYS = 30  # upsert only the last X days of volume fetched from the api

# High-Level Dataset Pulls
@dataclass
class DefillamaDEXFeesRev:
    """Metadata and Volumes for all DEXs, and Fees & Revenue.

    This is the result we obtain after fetching from the API and extracting the data
    that we need to ingest.
    """

    metadata_df: pl.DataFrame

    total_crypto_df: pl.DataFrame
    total_chain_df: pl.DataFrame

    breakdown_df: pl.DataFrame

def pull_dataframes(SUMMARY_ENDPOINT, CHAIN_ENDPOINT) -> DefillamaDEXFeesRev:
    """
    Pull all datapoints from the SUMMARY_ENDPOINT
    1. Chain List - for breakdown pulls
    2. Total and Chain-Wide
    3. Breakdown by Chain and Protocol
    """
    session = new_session()

    metadata_df = get_data(session, SUMMARY_ENDPOINT)

    # Get Dataframes
    protocol_metadata = get_protocol_metadata(metadata_df)
    total_df = get_total_data_chart(metadata_df)
    volume_dfs = get_total_chain(session, metadata_df, CHAIN_ENDPOINT)
    chain_df = volume_dfs[0]
    breakdown_df = volume_dfs[1]

    return DefillamaDEXFeesRev(
        metadata_df= protocol_metadata,
        total_crypto_df=total_df,
        total_chain_df=chain_df,
        breakdown_df=breakdown_df
    )

def pull_dex_dataframes() -> DefillamaDEXFeesRev:
    return pull_dataframes(DEX_SUMMARY_ENDPOINT, DEX_CHAIN_ENDPOINT)

def pull_fees_dataframes() -> DefillamaDEXFeesRev:
    return pull_dataframes(FEE_SUMMARY_ENDPOINT, FEE_CHAIN_ENDPOINT)

def pull_revenue_dataframes() -> DefillamaDEXFeesRev:
    return pull_dataframes(REVENUE_SUMMARY_ENDPOINT, REVENUE_CHAIN_ENDPOINT)

# Intermediate Functions
def get_total_data_chart(metadata_df) -> pl.DataFrame | None:
    try:
        total_daily_volumes = metadata_df["totalDataChart"]
        
        # Build Dataframe
        total_daily_volumes_df = pl.DataFrame(total_daily_volumes, schema=["dt", "total_volume_usd"], orient="row")
        
        # Format dt column as date
        total_daily_volumes_df = total_daily_volumes_df.with_columns(
            pl.col("dt").map_elements(dt_fromepoch, return_dtype=pl.String).alias("dt")
        )
        return total_daily_volumes_df

    except KeyError:
        print("Warning: No totalDataChart found")
        return None


def get_total_data_chart_breakdown_volumes(metadata_df) -> pl.DataFrame | None:
    try:
        total_daily_bk_volumes = metadata_df["totalDataChartBreakdown"]
        
        # Build Dataframe

        flattened_data: list[dict[str, any]] = []

        for entry in total_daily_bk_volumes:
            dt = entry[0]  # The timestamp
            protocol_data = entry[1]  # The dictionary of protocol volumes

            # Iterate over the protocol data to flatten it
            for protocol, volume in protocol_data.items():
                flattened_data.append({
                    "dt": dt,
                    "protocol": protocol,
                    "total_volume_usd": volume
                })

        # Create DataFrame from the flattened data
        total_daily_bk_volumes_df = pl.DataFrame(flattened_data, schema=["dt", "protocol","total_volume_usd"], orient="row")

        total_daily_bk_volumes_df = total_daily_bk_volumes_df.with_columns(
                pl.col("dt").map_elements(dt_fromepoch, return_dtype=pl.String).alias("dt")
            )

        return total_daily_bk_volumes_df

    except KeyError:
        print("Warning: No totalDataChart found")
        return None

def get_protocol_metadata(metadata_df):

    protocols = metadata_df["protocols"]

    MUST_HAVE_FIELDS = [
        "defillamaId",
        "name",
        "displayName",
        "module",
        "category",
        "logo",
        "chains",
        "protocolType",
        "methodologyURL",
        "methodology",
        "latestFetchIsOk",
        "slug",
        "id",
    ]
    OPTIONAL_FIELDS = [
        "parentProtocol",
        "total24h",
        "total48hto24h",
        "total7d",
        "total14dto7d",
        "total60dto30d",
        "total30d",
        "total1y",
        "totalAllTime",
        "average1y",
        "change_1d",
        "change_7d",
        "change_1m",
        "change_7dover7d",
        "change_30dover30d",
        "breakdown24h"
    ]

    total_metadata: list[dict] = []

    for element in protocols:
        metadata_row: dict[str, Optional[str]] = {}
        for key in MUST_HAVE_FIELDS:
            if key == "methodology":
                # Convert the methodology struct to a JSON string
                metadata_row[key] = json.dumps(element.get(key, {}))
            else:
                metadata_row[key] = element[key]
        for key in OPTIONAL_FIELDS:
            metadata_row[key] = element.get(key)
            
        total_metadata.append(metadata_row)
    
    protocols_df = pl.DataFrame(total_metadata, infer_schema_length = len(total_metadata))
    protocols_df = protocols_df.with_columns(latest_dt=pl.lit(now_dt()))

    return protocols_df

def construct_chain_urls(metadata_df, CHAIN_ENDPOINT) -> dict[str, str]:
    """Build the collection of urls that we will fetch from DefiLlama.

    Args:
        symbols: list of symbols to process. Defaults to None (process all).
    """
    urls = {}
    chains_list = metadata_df["allChains"]
    for chain in chains_list:
        chain_name_fmt = chain.replace(" ","%20")
        urls[chain] = CHAIN_ENDPOINT.format(chain_name=chain_name_fmt)

    if not urls:
        raise ValueError("No valid chains provided.")
    return urls


def get_total_chain(session, metadata_df, CHAIN_ENDPOINT) -> list[pl.DataFrame]:
    dex_chain_urls = construct_chain_urls(metadata_df, CHAIN_ENDPOINT)
    dexs_data = run_concurrently(lambda x: get_data(session, x), dex_chain_urls, max_workers=4)

    chain_level_df = get_chain_level_daily_volumes(dexs_data)
    chain_dex_level_df = get_chain_dex_level_daily_volumes(dexs_data)

    return [chain_level_df, chain_dex_level_df]


def get_chain_level_daily_volumes(dexs_data) -> pl.DataFrame:
    chain_volumes = []

    for key, value in dexs_data.items():
        chain_name = key
        dex_data = value
        try:
            total_chain_volume = get_total_data_chart(dex_data)
            total_chain_volume = total_chain_volume.with_columns(
                pl.lit(chain_name).alias("chain")
            )
            chain_volumes.append(total_chain_volume)
        except KeyError:
            print(f"Error processing {chain_name}. Skipping this chain.")
            continue  # Skip to the next iteration

    if not chain_volumes:
        raise ValueError("No valid chain data found")

    total_chain_volumes_df = pl.concat(chain_volumes)

    return total_chain_volumes_df

def get_chain_dex_level_daily_volumes(dexs_data) -> pl.DataFrame:
    chain_volumes = []

    for key, value in dexs_data.items():
        chain_name = key
        dex_data = value
        try:
            total_chain_volume = get_total_data_chart_breakdown_volumes(dex_data)
            total_chain_volume = total_chain_volume.with_columns(
                pl.lit(chain_name).alias("chain")
            )
            chain_volumes.append(total_chain_volume)
        except KeyError:
            print(f"Error processing {chain_name}. Skipping this chain.")
            continue  # Skip to the next iteration

    if not chain_volumes:
        raise ValueError("No valid chain data found")

    total_chain_volumes_df = pl.concat(chain_volumes)

    return total_chain_volumes_df


# Write Functions

def pull_dex_volume():
    result = pull_dex_dataframes()

    # Write metadata.
    DefiLlama.DEX_METADATA.write(
        dataframe=result.metadata_df.with_columns(dt=pl.lit(now_dt())),
        sort_by=["defillamaId"],
    )

    # # Write Overall DEX Volume.
    # DefiLlama.DEX_TOTAL.write(
    #     dataframe=most_recent_dates(result.total_crypto_df, n_dates=TABLE_LAST_N_DAYS),
    #     sort_by=["dt"],
    # )

    # Write By Chain DEX Volume.
    DefiLlama.DEX_CHAIN.write(
        dataframe=most_recent_dates(result.total_chain_df, n_dates=TABLE_LAST_N_DAYS),
        sort_by=["chain"],
    )

    # Write Breakdown DEX Volume.
    DefiLlama.DEX_BREAKDOWN.write(
        dataframe=most_recent_dates(result.breakdown_df, n_dates=TABLE_LAST_N_DAYS),
        sort_by=["chain", "protocol"],
    )

def pull_fees():
    result = pull_fees_dataframes()

    # Write metadata.
    DefiLlama.FEES_METADATA.write(
        dataframe=result.metadata_df.with_columns(dt=pl.lit(now_dt())),
        sort_by=["defillamaId"],
    )

    # Write Overall Fees.
    DefiLlama.FEES_TOTAL.write(
        dataframe=most_recent_dates(result.total_crypto_df, n_dates=TABLE_LAST_N_DAYS),
        sort_by=["dt"],
    )

    # Write By Chain Fees.
    DefiLlama.FEES_CHAIN.write(
        dataframe=most_recent_dates(result.total_chain_df, n_dates=TABLE_LAST_N_DAYS),
        sort_by=["chain"],
    )

    # Write Breakdown Fees.
    DefiLlama.FEES_BREAKDOWN.write(
        dataframe=most_recent_dates(result.breakdown_df, n_dates=TABLE_LAST_N_DAYS),
        sort_by=["chain", "protocol"],
    )

def pull_revenue():
    result = pull_revenue_dataframes()

    # Write metadata.
    DefiLlama.REVENUE_METADATA.write(
        dataframe=result.metadata_df.with_columns(dt=pl.lit(now_dt())),
        sort_by=["defillamaId"],
    )

    # Write Overall Revenue.
    DefiLlama.REVENUE_TOTAL.write(
        dataframe=most_recent_dates(result.total_crypto_df, n_dates=TABLE_LAST_N_DAYS),
        sort_by=["dt"],
    )

    # Write By Chain Revenue.
    DefiLlama.REVENUE_CHAIN.write(
        dataframe=most_recent_dates(result.total_chain_df, n_dates=TABLE_LAST_N_DAYS),
        sort_by=["chain"],
    )

    # Write Breakdown Revenue.
    DefiLlama.REVENUE_BREAKDOWN.write(
        dataframe=most_recent_dates(result.breakdown_df, n_dates=TABLE_LAST_N_DAYS),
        sort_by=["chain", "protocol"],
    )
