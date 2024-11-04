# -*- coding: utf-8 -*-
import time
from typing import Dict, List, Optional, Tuple
import polars as pl
from op_coreutils.bigquery.write import (
    overwrite_unpartitioned_table,
    overwrite_partition_static,
    upsert_partitioned_table,
)
from datetime import datetime, timedelta, timezone
from op_coreutils.logger import structlog
from op_coreutils.request import new_session
from op_coreutils.time import now_date
from op_coreutils.threads import run_concurrently

log = structlog.get_logger()

SUMMARY_ENDPOINT = "https://stablecoins.llama.fi/stablecoins?includePrices=true"
BREAKDOWN_ENDPOINT = "https://stablecoins.llama.fi/stablecoin/{id}"

BQ_DATASET = "uploads_api"

BREAKDOWN_TABLE = "defillama_daily_stablecoins_breakdown"
METADATA_TABLE = "defillama_stablecoins_metadata"


def get_data(session, url: str) -> Optional[dict]:
    """
    Fetches data from a given URL using the provided session.

    Args:
        session: The HTTP session for making requests.
        url (str): The URL to fetch data from.

    Returns:
        Optional[dict]: The JSON response as a dictionary, or None if an error occurred.
    """
    start = time.time()
    try:
        response = session.get(url, headers={"Content-Type": "application/json"})
        response.raise_for_status()
        resp_json = response.json()
        log.info(f"Fetched from {url}: {time.time() - start:.2f} seconds")
        return resp_json
    except Exception as e:
        log.error(f"Failed to fetch data from {url}: {e}")
        return None


def process_breakdown_stables(
    data: dict, days: int = 30
) -> Tuple[pl.DataFrame, Dict[str, Optional[str]]]:
    """
    Processes breakdown data for stablecoins, filtering for the most recent N days.

    Args:
        data (dict): The breakdown data from the API.
        days (int): The number of days to filter in the processed data.

    Returns:
        Tuple[pl.DataFrame, Dict[str, Optional[str]]]: A Polars DataFrame with breakdown data and metadata dictionary.
    """
    if data is None:
        return pl.DataFrame(), {}

    peg_type: str = data.get("pegType", "")
    balances: Dict[str, dict] = data.get("chainBalances", {})

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
    rows: List[Dict[str, Optional[str]]] = []

    for chain, balance in balances.items():
        tokens = balance.get("tokens", [])
        filtered_tokens = [
            datapoint
            for datapoint in tokens
            if datetime.fromtimestamp(datapoint["date"], tz=timezone.utc) >= cutoff_date
        ]

        for datapoint in filtered_tokens:
            row: Dict[str, Optional[str]] = {
                "chain": chain,
                "dt": datetime.fromtimestamp(
                    datapoint["date"], tz=timezone.utc
                ).strftime("%Y-%m-%d"),
                "circulating": datapoint.get("circulating", {}).get(peg_type),
                "bridged_to": datapoint.get("bridgedTo", {}).get(peg_type),
                "minted": datapoint.get("minted", {}).get(peg_type),
                "unreleased": datapoint.get("unreleased", {}).get(peg_type),
            }
            rows.append(row)

    must_have_metadata_fields = [
        "id",
        "name",
        "address",
        "symbol",
        "url",
        "pegType",
        "pegMechanism",
    ]
    metadata_fields = [
        "description",
        "mintRedeemDescription",
        "onCoinGecko",
        "gecko_id",
        "cmcId",
        "priceSource",
        "twitter",
        "price",
    ]

    metadata: Dict[str, Optional[str]] = {
        key: data.get(key) for key in must_have_metadata_fields + metadata_fields
    }

    result = (
        pl.DataFrame(rows, infer_schema_length=len(rows)).with_columns(
            id=pl.lit(metadata["id"]),
            name=pl.lit(metadata["name"]),
            symbol=pl.lit(metadata["symbol"]),
        )
        if rows
        else pl.DataFrame()
    )

    return result, metadata


def pull_stables(
    stablecoin_ids: Optional[List[str]] = None, days: int = 30
) -> Dict[str, pl.DataFrame]:
    """
    Pulls and processes stablecoin data from DeFiLlama.

    Args:
        stablecoin_ids (Optional[List[str]]): List of stablecoin IDs to process. Defaults to None (process all).
        days (int): Number of days of data to retrieve. Defaults to 30.

    Returns:
        Dict[str, pl.DataFrame]: A dictionary containing metadata and breakdown DataFrames.
    """
    session = new_session()
    summary = get_data(session, SUMMARY_ENDPOINT)
    if summary is None:
        log.error("Failed to fetch summary data.")
        return {"metadata": pl.DataFrame(), "breakdown": pl.DataFrame()}

    all_stablecoin_ids = [asset["id"] for asset in summary.get("peggedAssets", [])]
    if stablecoin_ids is not None:
        # Filter only the stablecoin IDs provided
        stablecoin_ids = [id_ for id_ in stablecoin_ids if id_ in all_stablecoin_ids]
        if not stablecoin_ids:
            log.error("No valid stablecoin IDs provided.")
            return {"metadata": pl.DataFrame(), "breakdown": pl.DataFrame()}
    else:
        stablecoin_ids = all_stablecoin_ids

    urls = {
        stablecoin_id: BREAKDOWN_ENDPOINT.format(id=stablecoin_id)
        for stablecoin_id in stablecoin_ids
    }

    stablecoin_data = run_concurrently(
        lambda x: get_data(session, x), urls, max_workers=4
    )

    breakdown_dfs: List[pl.DataFrame] = []
    metadata_rows: List[Dict[str, Optional[str]]] = []

    for data in stablecoin_data.values():
        if data:
            breakdown_df, metadata = process_breakdown_stables(data, days=days)
            if not breakdown_df.is_empty():
                breakdown_dfs.append(breakdown_df)
            if metadata:
                metadata_rows.append(metadata)
        else:
            log.warning("Received empty data for a stablecoin.")

    breakdown_df = (
        pl.concat(breakdown_dfs, how="diagonal_relaxed")
        if breakdown_dfs
        else pl.DataFrame()
    )
    metadata_df = (
        pl.DataFrame(metadata_rows, infer_schema_length=len(metadata_rows))
        if metadata_rows
        else pl.DataFrame()
    )

    dt = now_date()

    if not metadata_df.is_empty():
        overwrite_unpartitioned_table(
            metadata_df, BQ_DATASET, f"{METADATA_TABLE}_latest"
        )
        overwrite_partition_static(
            metadata_df,
            partition_dt=dt,
            dataset=BQ_DATASET,
            table_name=f"{METADATA_TABLE}_history",
        )

    if not breakdown_df.is_empty():
        dates = breakdown_df["dt"].unique().to_list()
        for date_value in dates:
            date_data = breakdown_df.filter(pl.col("dt") == date_value)
            upsert_partitioned_table(
                date_data,
                dataset=BQ_DATASET,
                table_name=f"{BREAKDOWN_TABLE}_history",
                unique_keys=["dt", "id", "chain"],
                partition_dt=date_value,
            )

    return {"metadata": metadata_df, "breakdown": breakdown_df}
