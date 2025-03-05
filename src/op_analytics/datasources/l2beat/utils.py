from dataclasses import dataclass
from typing import Any

import polars as pl

from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


@dataclass(frozen=True)
class L2BeatProject:
    """A single project as referenced by L2Beat."""

    id: str
    slug: str


def apply_schema(
    all_data: dict[L2BeatProject, Any],
    column_schemas: dict[str, type[pl.DataType]],
) -> pl.DataFrame:
    """Convert to dataframe.

    L2Beat API responses have the same structure for different endpoints. This function
    leverages that structure to turn the data feched from multiple projects into a polars
    dataframe.
    """
    percent_success = 100.0 * sum(_["success"] for _ in all_data.values()) / len(all_data)
    if percent_success < 80:
        raise Exception("Failed to get L2Beat data for >80%% of chains")

    # Process the feched data.
    dfs = []
    for project, data in all_data.items():
        if data["success"]:
            chart_data = data["data"]["chart"]
            columns = chart_data["types"]
            values = chart_data["data"]

            # Ensure fetched data conforms to our expected schema.
            assert set(column_schemas.keys()) == set(columns)

            schema = [(col, column_schemas[col]) for col in columns]

            # Pick the last value for each date.
            project_df = (
                pl.DataFrame(values, schema=schema, orient="row")
                .with_columns(
                    id=pl.lit(project.id),
                    slug=pl.lit(project.slug),
                    dt=pl.from_epoch(pl.col("timestamp")).dt.strftime("%Y-%m-%d"),
                )
                .sort("timestamp")
                .group_by("dt", maintain_order=True)
                .last()
            )

            dfs.append(project_df)

    return pl.concat(dfs)
