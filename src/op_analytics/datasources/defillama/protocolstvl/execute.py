from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.threads import run_concurrently_store_failures
from op_analytics.coreutils.time import date_fromstr, dt_fromepoch, epoch_is_date, now_date, now_dt

from ..dataaccess import DefiLlama

log = structlog.get_logger()

PROTOCOLS_ENDPOINT = "https://api.llama.fi/protocols"
PROTOCOL_DETAILS_ENDPOINT = "https://api.llama.fi/protocol/{slug}"


TVL_TABLE_LAST_N_DAYS = 90


def execute_pull():
    result = pull_protocol_tvl()
    return {
        "metadata_df": dt_summary(result.metadata_df),
        "app_tvl_df": dt_summary(result.app_tvl_df),
        "app_token_tvl_df": dt_summary(result.app_token_tvl_df),
    }


@dataclass
class DefillamaProtocols:
    """Metadata and data for all protocols."""

    metadata_df: pl.DataFrame
    app_tvl_df: pl.DataFrame
    app_token_tvl_df: pl.DataFrame


@dataclass
class SingleProtocolDFs:
    """Records obtained for a single protocol."""

    tvl_df: pl.DataFrame | None
    token_tvl_df: pl.DataFrame | None


def pull_protocol_tvl() -> DefillamaProtocols:
    """
    Pulls and processes protocol data from DeFiLlama.

    Args:
        pull_protocols: list of protocol slugs to process. Defaults to None (process all).
    """

    session = new_session()

    # Fetch the list of protocols and their metadata
    protocols = get_data(session, PROTOCOLS_ENDPOINT)
    metadata_df = extract_protocol_metadata(protocols)

    DefiLlama.PROTOCOLS_METADATA.write(
        dataframe=metadata_df.with_columns(dt=pl.lit(now_dt())),
        sort_by=["protocol_slug"],
    )

    # Create list of slugs to fetch protocol-specific data
    slugs = metadata_df.get_column("protocol_slug").to_list()

    # Fetch and extract protocol details in parallel.
    # The single protocol extraction filters data to only the dates of
    # interest, which helps control memory usage.
    log.info(f"fetching data for {len(slugs)} protocols")
    run_results = run_concurrently_store_failures(
        function=lambda slug: extract_single_protocol(session, slug),
        targets=slugs,
        max_workers=8,
    )

    # Tolerate some failures due to DefiLlama API instabilities.
    if num_failures := len(run_results.failures) < 3:
        log.warning("proceeding despite failures", num_failures=num_failures)
    else:
        raise Exception(f"too many single protocol failures: {num_failures}")

    protocol_data: dict[str, SingleProtocolDFs] = run_results.results

    log.info("done fetching and preprocessing data")

    # Load protocol data into dataframes.
    app_tvl_df = pl.concat(_.tvl_df for _ in protocol_data.values() if _.tvl_df is not None)
    app_token_tvl_df = pl.concat(
        _.token_tvl_df for _ in protocol_data.values() if _.token_tvl_df is not None
    )

    # Write tvl.
    DefiLlama.PROTOCOLS_TVL.write(
        dataframe=app_tvl_df,
        sort_by=["protocol_slug", "chain"],
    )

    # Write token tvl.
    DefiLlama.PROTOCOLS_TOKEN_TVL.write(
        dataframe=app_token_tvl_df,
        sort_by=["protocol_slug", "chain", "token"],
    )

    return DefillamaProtocols(
        metadata_df=metadata_df,
        app_tvl_df=app_tvl_df,
        app_token_tvl_df=app_token_tvl_df,
    )


def table_cutoff_date() -> date:
    return now_date() - timedelta(TVL_TABLE_LAST_N_DAYS)


def extract_parent(protocol: dict[str, Any]) -> str | None:
    if parent_protcol := protocol.get("parentProtocol"):
        assert isinstance(parent_protcol, str)
        return parent_protcol.replace("parent#", "")
    else:
        return protocol.get("slug")


def extract_protocol_metadata(protocols: list[dict[str, Any]]) -> pl.DataFrame:
    """Extract metadata from the protocols API response.

    Args:
        protocols: List of protocol dictionaries from the API response.

    Returns:
        Polars DataFrame containing metadata.
    """
    metadata_records = [
        {
            "protocol_name": protocol.get("name"),
            "protocol_slug": protocol.get("slug"),
            "protocol_category": protocol.get("category"),
            "parent_protocol": extract_parent(protocol),
            "wrong_liquidity": protocol.get("wrongLiquidity"),
            "misrepresented_tokens": protocol.get("misrepresentedTokens"),
        }
        for protocol in protocols
        if protocol.get("category")
    ]
    return pl.DataFrame(metadata_records)


@dataclass(frozen=True, slots=True)
class PrimaryKey:
    protocol_slug: str
    chain: str
    dt: str
    token: str


def extract_single_protocol(session, slug) -> SingleProtocolDFs:
    """Fetch and extract for a single protocol.

    Calls the DefiLLama endpoint and extracts daily 'tvl' and 'tokensInUsd' for the
    given slug.
    """

    # Fetch data
    url = PROTOCOL_DETAILS_ENDPOINT.format(slug=slug)
    data = get_data(session, url, retry_attempts=5)

    # Initialize extracted records.
    tvl_records = []

    token_tvl_records: dict[PrimaryKey, list[dict]] = defaultdict(list)

    # Each app entry can have tvl data in multiple chains. Loop through each chain
    chain_tvls = data.get("chainTvls", {})

    cutoff_date = table_cutoff_date()
    for chain, chain_data in chain_tvls.items():
        # Extract total app tvl
        tvl_entries = chain_data.get("tvl", [])
        for tvl_entry in tvl_entries:
            dateval = dt_fromepoch(tvl_entry["date"])

            if date_fromstr(dateval) < cutoff_date:
                continue

            if not epoch_is_date(tvl_entry["date"]):
                continue

            tvl_records.append(
                {
                    "protocol_slug": slug,
                    "chain": chain,
                    "dt": dateval,
                    "total_app_tvl": float(tvl_entry.get("totalLiquidityUSD")),
                }
            )

        # Extract token tvl for each app
        tokens_entries = chain_data.get("tokens", [])
        for tokens_entry in tokens_entries:
            dateval = dt_fromepoch(tokens_entry["date"])

            if date_fromstr(dateval) < cutoff_date:
                continue

            if not epoch_is_date(tokens_entry["date"]):
                continue

            token_tvls = tokens_entry.get("tokens", [])
            for token in token_tvls:
                pkey = PrimaryKey(
                    protocol_slug=slug,
                    chain=chain,
                    dt=dateval,
                    token=token,
                )
                token_tvl_records[pkey].append({"app_token_tvl": float(token_tvls[token])})

        # Extract token usd tvl for each app
        tokens_usd_entries = chain_data.get("tokensInUsd", [])
        for tokens_usd_entry in tokens_usd_entries:
            dateval = dt_fromepoch(tokens_usd_entry["date"])

            if date_fromstr(dateval) < cutoff_date:
                continue

            if not epoch_is_date(tokens_usd_entry["date"]):
                continue

            token_usd_tvls = tokens_usd_entry.get("tokens", [])
            for token in token_usd_tvls:
                pkey = PrimaryKey(
                    protocol_slug=slug,
                    chain=chain,
                    dt=dateval,
                    token=token,
                )
                token_tvl_records[pkey].append({"app_token_tvl_usd": float(token_usd_tvls[token])})

    if len(tvl_records) == 0:
        tvl_df = None
    else:
        tvl_df = pl.DataFrame(
            tvl_records,
            schema={
                "protocol_slug": pl.String(),
                "chain": pl.String(),
                "dt": pl.String(),
                "total_app_tvl": pl.Float64(),
            },
        )

    if not token_tvl_records:
        token_tvl_df = None
    else:
        token_tvl_df = pl.DataFrame(
            join_datapoints(token_tvl_records),
            schema={
                "protocol_slug": pl.String(),
                "chain": pl.String(),
                "dt": pl.String(),
                "token": pl.String(),
                "app_token_tvl": pl.Float64(),
                "app_token_tvl_usd": pl.Float64(),
            },
        )

    return SingleProtocolDFs(
        tvl_df=tvl_df,
        token_tvl_df=token_tvl_df,
    )


def join_datapoints(token_tvl_records: dict[PrimaryKey, list[dict]]):
    """Helper function.

    Yields the separate TVL and TVL_USD values as a single row (join) so
    they can be assembled in a a single dataframe for each PrimaryKey.
    """
    for pkey, datapoints in token_tvl_records.items():
        row = asdict(pkey)
        for datapoint in datapoints:
            row |= datapoint
        yield row
