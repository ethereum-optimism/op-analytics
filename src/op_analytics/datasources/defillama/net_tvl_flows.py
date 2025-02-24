from dataclasses import dataclass
from datetime import timedelta

import polars as pl

from op_analytics.coreutils.duckdb_inmem.client import init_client
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.coreutils.time import date_fromstr, now_dt
from op_analytics.datasources.defillama.dataaccess import DefiLlama

log = structlog.get_logger()

FLOW_DAYS = [1, 7, 14, 28, 60, 90, 365]
FLOW_TABLE_LAST_N_DAYS = 90


def execute_pull():
    # Produce the result
    result = DefiLlamaNetFlows.of_date()

    # Write to storage
    DefiLlama.PROTOCOL_TOKEN_NET_FLOWS.write(
        dataframe=result.df_net_flows,
        sort_by=["dt", "chain", "protocol_slug", "token"],
    )

    return {
        "df_net_flows": dt_summary(result.df_net_flows),
    }


@dataclass
class DefiLlamaNetFlows:
    df_net_flows: pl.DataFrame

    @classmethod
    def of_date(cls, current_dt: str | None = None):
        """Process DeFiLlama TVL data to calculate net flows."""
        ctx = init_client()
        client = ctx.client

        current_dt = current_dt or now_dt()
        current_date = date_fromstr(current_dt)

        # Calculate min_date to ensure we have enough historical data for flow calculations
        max_flow_days = max(FLOW_DAYS)
        min_date = current_date - timedelta(days=max_flow_days + FLOW_TABLE_LAST_N_DAYS + 1)
        max_date = current_date - timedelta(days=1)  # Exclude current day as it's incomplete

        tvl_view = DefiLlama.PROTOCOL_TOKEN_TVL_BREAKDOWN.read(
            min_date=min_date,
            max_date=max_date,
        )

        # Get base TVL data
        df_tvl = client.sql(
            f"""
            SELECT
                dt,
                chain,
                protocol_slug,
                token,
                COALESCE(app_token_tvl, 0) as app_token_tvl,
                COALESCE(app_token_tvl_usd, 0) as app_token_tvl_usd
            FROM {tvl_view}
            """
        ).pl()

        # Calculate net flows for each flow day period
        df_flows = calculate_net_flows(df_tvl, FLOW_DAYS)

        return cls(df_net_flows=df_flows)


def calculate_net_flows(df: pl.DataFrame, flow_days: list) -> pl.DataFrame:
    # Sort by grouping keys and dt so that the join behaves as expected.
    df = df.sort(["chain", "protocol_slug", "token", "dt"])

    # Compute the USD conversion rate for each row.
    df = df.with_columns(
        [
            (pl.col("app_token_tvl_usd") / pl.col("app_token_tvl"))
            .fill_null(0)
            .fill_nan(0)
            .alias("usd_conversion_rate")
        ]
    )

    for d in flow_days:
        # Create a temporary DataFrame with dt_prev = dt - d days.
        temp = df.with_columns([(pl.col("dt") - pl.duration(days=d)).alias("dt_prev")])

        # Create a "previous" DataFrame that renames dt to dt_prev and app_token_tvl to prev_app_token_tvl.
        prev = df.select(
            [
                pl.col("dt").alias("dt_prev"),
                "chain",
                "protocol_slug",
                "token",
                pl.col("app_token_tvl").alias("prev_app_token_tvl"),
            ]
        )

        # Join temp with prev on dt_prev and the grouping keys.
        joined = temp.join(
            prev, on=["dt_prev", "chain", "protocol_slug", "token"], how="left"
        ).with_columns([pl.col("prev_app_token_tvl").fill_null(0).fill_nan(0)])

        # Compute the net token flow for this flow period.
        net_flow_col = f"net_token_flow_{d}d"
        joined = joined.with_columns(
            [
                (
                    pl.col("app_token_tvl_usd")
                    - (pl.col("prev_app_token_tvl") * pl.col("usd_conversion_rate"))
                )
                .fill_null(0)
                .fill_nan(0)
                .alias(net_flow_col)
            ]
        )

        # Join the net flow column back into the original df using the unique keys.
        df = df.join(
            joined.select(["dt", "chain", "protocol_slug", "token", net_flow_col]),
            on=["dt", "chain", "protocol_slug", "token"],
            how="left",
        )

    # Drop the temporary conversion rate column.
    df = df.drop("usd_conversion_rate")

    return df
