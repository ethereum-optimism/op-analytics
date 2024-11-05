# -*- coding: utf-8 -*-
from datetime import timedelta
from typing import Dict, List, Optional, Tuple

import polars as pl
from op_coreutils.bigquery.write import (
    overwrite_partition_static,
    overwrite_unpartitioned_table,
    upsert_partitioned_table,
)
from op_coreutils.logger import structlog
from op_coreutils.request import get_data, new_session
from op_coreutils.threads import run_concurrently
from op_coreutils.time import datetime_fromepoch, now_date, now

log = structlog.get_logger()

SUMMARY_ENDPOINT = "https://stablecoins.llama.fi/stablecoins?includePrices=true"
BREAKDOWN_ENDPOINT = "https://stablecoins.llama.fi/stablecoin/{id}"

BQ_DATASET = "uploads_api"

BREAKDOWN_TABLE = "defillama_daily_stablecoins_breakdown"
METADATA_TABLE = "defillama_stablecoins_metadata"


def process_breakdown_stables(
    data: dict, days: int = 30
) -> Tuple[pl.DataFrame, Dict[str, Optional[str]]]:
    """
    Processes breakdown data for stablecoins, filtering for the most recent N days.

    Args:
        data: The breakdown data from the API.
        days: The number of days to filter in the processed data.

    Returns:
        A tuple containing a Polars DataFrame with breakdown data and a metadata dictionary.
    """

    if "pegType" not in data or "chainBalances" not in data:
        raise ValueError("Missing required fields: 'pegType' or 'chainBalances'")

    peg_type: str = data["pegType"]
    balances: Dict[str, dict] = data["chainBalances"]

    cutoff_date = now() - timedelta(days=days)
    rows: List[Dict[str, Optional[str]]] = []

    for chain, balance in balances.items():
        tokens = balance.get("tokens", [])

        for datapoint in tokens:
            if datetime_fromepoch(datapoint["date"]) < cutoff_date:
                continue
            row: Dict[str, Optional[str]] = {
                "chain": chain,
                "dt": datetime_fromepoch(datapoint["date"]).strftime("%Y-%m-%d"),
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

    metadata: Dict[str, Optional[str]] = {}

    # Collect required metadata fields
    for key in must_have_metadata_fields:
        if key not in data:
            raise KeyError(f"Missing required metadata field: '{key}'")
        metadata[key] = data[key]

    # Collect additional optional metadata fields
    for key in metadata_fields:
        metadata[key] = data.get(key)

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
        stablecoin_ids: List of stablecoin IDs to process. Defaults to None (process all).
        days: Number of days of data to retrieve. Defaults to 30.

    Returns:
        A dictionary containing metadata and breakdown DataFrames.
    """
    session = new_session()
    summary = get_data(session, SUMMARY_ENDPOINT)

    if "peggedAssets" not in summary:
        raise KeyError("The 'peggedAssets' key is missing from the summary data.")

    urls = {}
    for stablecoin in summary["peggedAssets"]:
        stablecoin_id = stablecoin["id"]
        if stablecoin_ids is None or stablecoin_id in stablecoin_ids:
            urls[stablecoin_id] = BREAKDOWN_ENDPOINT.format(id=stablecoin_id)
    if not urls:
        raise ValueError("No valid stablecoin IDs provided.")

    stablecoin_data = run_concurrently(
        lambda x: get_data(session, x), urls, max_workers=4
    )

    breakdown_dfs: List[pl.DataFrame] = []
    metadata_rows: List[Dict[str, Optional[str]]] = []

    for data in stablecoin_data.values():
        breakdown_df, metadata = process_breakdown_stables(data, days=days)
        if not breakdown_df.is_empty():
            breakdown_dfs.append(breakdown_df)
            if metadata:
                metadata_rows.append(metadata)

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

    if metadata_df.is_empty():
        raise ValueError(
            "metadata_df is empty. Expected non-empty data to write to BigQuery."
        )
    overwrite_unpartitioned_table(metadata_df, BQ_DATASET, f"{METADATA_TABLE}_latest")
    overwrite_partition_static(
        metadata_df,
        partition_dt=dt,
        dataset=BQ_DATASET,
        table_name=f"{METADATA_TABLE}_history",
    )

    if breakdown_df.is_empty():
        raise ValueError(
            "breakdown_df is empty. Expected non-empty data to write to BigQuery."
        )

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
