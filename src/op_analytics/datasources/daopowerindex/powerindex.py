from dataclasses import dataclass

import polars as pl
import requests

from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.request import new_session, get_data
from op_analytics.coreutils.misc import raise_for_schema_mismatch

CPI_ENDPOINT = "https://api.daocpi.com/api/calculate-cpi"


SNAPSHOT_SCHEMA = {
    "dt": pl.String(),
    "cpi_value": pl.Float64(),
    "active_percent": pl.Float64(),
    "inactive_percent": pl.Float64(),
}

COUNCIL_PERCENTAGES_SCHEMA = {
    "dt": pl.String(),
    "council_name": pl.String(),
    "original_percentage": pl.Float64(),
    "redistributed_percentage": pl.Float64(),
}


@dataclass
class ConcentrationOfPowerIndex:
    snapshots_df: pl.DataFrame
    council_percentages_df: pl.DataFrame

    @classmethod
    def fetch(cls, session: requests.Session | None = None) -> "ConcentrationOfPowerIndex":
        session = session or new_session()

        api_key = env_get("DAOCPI_API_KEY")
        response = get_data(session, CPI_ENDPOINT, headers={"x-api-key": api_key})

        results = response["results"]

        snapshots_df = construct_snapshots_df(results)

        council_percentages_df = construct_council_percentages_df(results)

        return cls(
            snapshots_df=snapshots_df,
            council_percentages_df=council_percentages_df,
        )


def construct_snapshots_df(results) -> pl.DataFrame:
    snapshots = []

    for item in results:
        # Get the snapshot date
        snapshot_date = item["date"]

        snapshot = {
            "dt": snapshot_date,
            "cpi_value": item["cpi"],
            "active_percent": float(item["councilPercentages"]["active"]),
            "inactive_percent": float(item["councilPercentages"]["inactive"]),
        }
        snapshots.append(snapshot)

    snapshots_df = pl.DataFrame(snapshots)

    raise_for_schema_mismatch(
        actual_schema=snapshots_df.schema,
        expected_schema=pl.Schema(SNAPSHOT_SCHEMA),
    )

    return snapshots_df


def construct_council_percentages_df(results) -> pl.DataFrame:
    council_percentages = []

    for item in results:
        # Get the snapshot date
        snapshot_date = item["date"]

        # Extract council percentages
        redistributed = item["councilPercentages"]["redistributed"]
        original = item["councilPercentages"]["originalPercentages"]

        # Combine all council names from both dictionaries
        all_councils = set(redistributed.keys()) | set(original.keys())

        for council in all_councils:
            # Get the original percentage
            if council not in original:
                original_pct = None
            else:
                original_pct = float(original[council])

            # Get the redistributed percentage
            if council not in redistributed:
                redistributed_pct = None
            else:
                redistributed_pct = float(redistributed[council])

            council_data = {
                "dt": snapshot_date,
                "council_name": council,
                "original_percentage": original_pct,
                "redistributed_percentage": redistributed_pct,
            }
            council_percentages.append(council_data)

    council_percentages_df = pl.DataFrame(council_percentages)

    raise_for_schema_mismatch(
        actual_schema=council_percentages_df.schema,
        expected_schema=pl.Schema(COUNCIL_PERCENTAGES_SCHEMA),
    )

    return council_percentages_df
