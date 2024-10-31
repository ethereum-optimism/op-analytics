import time
import polars as pl
from op_coreutils.bigquery.write import (
    overwrite_partition_static,
    overwrite_partitions_dynamic,
    overwrite_unpartitioned_table,
)
from op_coreutils.logger import structlog
from op_coreutils.request import new_session
from op_coreutils.time import dt_fromepoch, now_date
from op_coreutils.threads import run_concurrently

log = structlog.get_logger()

SUMMARY_ENDPOINT = "https://stablecoins.llama.fi/stablecoins?includePrices=true"
BREAKDOWN_ENDPOINT = "https://stablecoins.llama.fi/stablecoin/{id}"

BQ_DATASET = "uploads_api"

BREAKDOWN_TABLE = "defillama_daily_stablecoins_breakdown"
METADATA_TABLE = "defillama_stablecoins_metadata"


def get_data(session, url):
    start = time.time()
    resp = session.request(
        method="GET",
        url=url,
        headers={"Content-Type": "application/json"},
    ).json()
    log.info(f"Fetched from {url}: {time.time() - start:.2f} seconds")
    return resp


def process_breakdown_stables(data):
    peg_type = data["pegType"]
    balances = data["chainBalances"]

    rows = []
    for chain, balance in balances.items():
        tokens = balance["tokens"]

        for datapoint in tokens:
            row = {}
            row["chain"] = chain
            row["dt"] = dt_fromepoch(datapoint["date"])

            try:
                row["circulating"] = datapoint["circulating"][peg_type]
            except KeyError:
                row["circulating"] = None

            try:
                row["bridged_to"] = datapoint["bridgedTo"][peg_type]
            except KeyError:
                row["bridged_to"] = None

            try:
                row["minted"] = datapoint["minted"][peg_type]
            except KeyError:
                row["minted"] = None

            try:
                row["unreleased"] = datapoint["unreleased"][peg_type]
            except KeyError:
                row["unreleased"] = None

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

    metadata = {}
    for key in must_have_metadata_fields:
        metadata[key] = data[key]

    for key in metadata_fields:
        metadata[key] = data.get(key)

    result = pl.DataFrame(rows, infer_schema_length=len(rows)).with_columns(
        id=pl.lit(metadata["id"]),
        name=pl.lit(metadata["name"]),
        symbol=pl.lit(metadata["symbol"]),
    )

    return result, metadata


def pull_stables():
    """Pull stablecoin data from DeFiLlama.

    - Fetch the stablecoins summary endpoint.
    - For each stablecoin, fetch the detailed breakdown data.
    - Write all results to BigQuery.
    """
    session = new_session()
    summary = get_data(session, SUMMARY_ENDPOINT)

    urls = {}
    for stablecoin in summary["peggedAssets"]:
        stablecoin_id = stablecoin["id"]
        urls[stablecoin_id] = BREAKDOWN_ENDPOINT.format(id=stablecoin_id)

    stablecoin_data = run_concurrently(lambda x: get_data(session, x), urls, max_workers=4)

    breakdown_dfs = []
    metadata_rows = []
    for data in stablecoin_data.values():
        breakdown_df, metadata = process_breakdown_stables(data)
        breakdown_dfs.append(breakdown_df)
        metadata_rows.append(metadata)

    breakdown_df = pl.concat(breakdown_dfs, how="diagonal_relaxed")
    metadata_df = pl.DataFrame(metadata_rows, infer_schema_length=len(metadata_rows))

    # Write metadata to BQ
    dt = now_date()
    overwrite_unpartitioned_table(metadata_df, BQ_DATASET, f"{METADATA_TABLE}_latest")
    overwrite_partition_static(metadata_df, dt, BQ_DATASET, f"{METADATA_TABLE}_history")

    # Write breakdown to BQ
    overwrite_partitions_dynamic(breakdown_df, BQ_DATASET, f"{BREAKDOWN_TABLE}_history")

    return {"metadata": metadata_df, "breakdown": breakdown_df}
