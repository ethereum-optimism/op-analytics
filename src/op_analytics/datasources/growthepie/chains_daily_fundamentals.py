from dataclasses import dataclass

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary, last_n_days
from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.time import now_dt

from .dataaccess import GrowThePie

log = structlog.get_logger()

URL_BASE = "https://api.growthepie.xyz/"
FUNDAMENTALS_ENDPOINT = "v1/fundamentals_full.json"
METADATA_ENDPOINT = "v1/master.json"


@dataclass
class GrowthepieFundamentalSummary:
    """Summary of daily chain fundamentals from GrowThePie.

    This is the result we obtain after fetching from the API and extracting the data
    that we need to ingest.
    """

    metadata_df: pl.DataFrame
    summary_df: pl.DataFrame

    @classmethod
    def fetch(cls):
        """Fetch GrowThePie daily chain fundamentals summary."""
        session = new_session()
        current_dt: str = now_dt()

        summary_raw_data = get_data(session, f"{URL_BASE}{FUNDAMENTALS_ENDPOINT}")
        summary_df = pl.DataFrame(
            summary_raw_data,
            schema={
                "metric_key": pl.String,
                "origin_key": pl.String,
                "value": pl.Float64,
                "date": pl.String,
            },
        ).rename({"date": "dt"})
        summary_df_truncated = last_n_days(
            summary_df,
            n_dates=7,
            reference_dt=current_dt,
            date_column_type_is_str=True,
        )

        metadata_raw_data = get_data(session, f"{URL_BASE}{METADATA_ENDPOINT}")
        chains_meta = metadata_raw_data["chains"]

        chains_meta_processed = process_metadata_pull(chains_meta)
        metadata_df = pl.DataFrame(
            chains_meta_processed,
            schema=[
                ("name", pl.String),
                ("url_key", pl.String),
                ("chain_type", pl.String),
                ("caip2", pl.String),
                ("evm_chain_id", pl.Int64),
                ("deployment", pl.String),
                ("name_short", pl.String),
                ("description", pl.String),
                ("da_layer", pl.String),
                ("symbol", pl.String),
                ("bucket", pl.String),
                ("ecosystem", pl.List(pl.String)),
                (
                    "colors",
                    pl.Struct(
                        {
                            "light": pl.List(pl.String),
                            "dark": pl.List(pl.String),
                            "darkTextOnBackground": pl.Boolean,
                        }
                    ),
                ),
                ("logo", pl.Struct({"body": pl.String, "width": pl.Int64, "height": pl.Int64})),
                ("technology", pl.String),
                ("purpose", pl.String),
                ("launch_date", pl.String),
                ("enable_contracts", pl.Boolean),
                ("l2beat_stage", pl.Struct({"stage": pl.String, "hex": pl.String})),
                ("l2beat_link", pl.String),
                ("l2beat_id", pl.String),
                ("raas", pl.String),
                ("stack", pl.Struct({"label": pl.String, "url": pl.String})),
                ("website", pl.String),
                ("twitter", pl.String),
                ("block_explorer", pl.String),
                (
                    "block_explorers",
                    pl.Struct(
                        {
                            "Etherscan": pl.String,
                            "Blockscout": pl.String,
                            "Arbiscan": pl.String,
                            "BaseScan": pl.String,
                            "Blast Explorer": pl.String,
                            "Immutascan": pl.String,
                            "LineaScan": pl.String,
                            "Loopring Explorer": pl.String,
                            "Optimistic Etherscan": pl.String,
                            "PolygonScan": pl.String,
                            "Rhino Explorer": pl.String,
                            "ScrollScan": pl.String,
                            "StarkScan": pl.String,
                            "Voyager": pl.String,
                            "Taikoscan": pl.String,
                            "Alchemy Explorer": pl.String,
                            "zkSync Explorer": pl.String,
                        }
                    ),
                ),
                ("rhino_listed", pl.Boolean),
                ("rhino_naming", pl.String),
                ("origin_key", pl.String),
            ],
        )

        return GrowthepieFundamentalSummary(
            metadata_df=metadata_df,
            # Use the full dataframe when backfilling:
            # summary_df=summary_df,
            summary_df=summary_df_truncated,
        )


def execute_pull():
    result = pull_growthepie_summary()
    return {
        "metadata_df": dt_summary(result.metadata_df),
        "summary_df": dt_summary(result.summary_df),
    }


def pull_growthepie_summary() -> GrowthepieFundamentalSummary:
    """Fetch and write to GCS."""

    data = GrowthepieFundamentalSummary.fetch()

    GrowThePie.CHAIN_METADATA.write(
        dataframe=data.metadata_df.with_columns(dt=pl.lit(now_dt())),
        sort_by=["name"],
    )

    GrowThePie.FUNDAMENTALS_SUMMARY.write(
        dataframe=data.summary_df,
        sort_by=["origin_key"],
    )


def process_metadata_pull(response_data) -> list[dict[str, str]]:
    """Extract chain metadata from GrowThePie API response."""

    normalized_data = []

    # Iterate over each chain in the 'chains' data
    for chain_meta, attributes in response_data.items():
        # Add the 'chain_meta' as part of the attributes
        attributes["origin_key"] = chain_meta
        normalized_data.append(attributes)

    return normalized_data
