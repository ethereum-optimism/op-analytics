from dataclasses import dataclass

import polars as pl
import requests

from op_analytics.coreutils.request import new_session, get_data
from op_analytics.coreutils.misc import raise_for_schema_mismatch

from .activity import fetch_activity
from .tvl import fetch_tvl
from .utils import L2BeatProject

SUMMARY_ENDPOINT = "https://l2beat.com/api/scaling/summary"

SUMMARY_SCHEMA = {
    "id": pl.String(),
    "name": pl.String(),
    "slug": pl.String(),
    "type": pl.String(),
    "hostChain": pl.String(),
    "category": pl.String(),
    "providers": pl.List(pl.String()),
    "purposes": pl.List(pl.String()),
    "isArchived": pl.Boolean(),
    "isUpcoming": pl.Boolean(),
    "isUnderReview": pl.Boolean(),
    "stage": pl.String(),
    "shortName": pl.String(),
    "da_badge": pl.String(),
    "vm_badge": pl.String(),
    "tvl_total": pl.Float64(),
    "tvl_ether": pl.Float64(),
    "tvl_stablecoin": pl.Float64(),
    "tvl_associated": pl.Float64(),
    "tvl_associated_tokens": pl.List(pl.String()),
}


@dataclass
class L2BeatProjectsSummary:
    projects: list[L2BeatProject]
    summary_df: pl.DataFrame

    @classmethod
    def fetch(cls, session: requests.Session | None = None) -> "L2BeatProjectsSummary":
        session = session or new_session()
        summary = get_data(session, SUMMARY_ENDPOINT)

        # Collect the list of projects tracked by L2Beat
        projects = []
        for project_data in list(summary["projects"].values()):
            projects.append(L2BeatProject(id=project_data["id"], slug=project_data["slug"]))

        # Parse summary df
        summary_df = parse_summary(summary)

        return cls(
            projects=projects,
            summary_df=summary_df,
        )


@dataclass
class L2BeatProjects:
    df: pl.DataFrame
    projects: list[L2BeatProject]

    tvl_df: pl.DataFrame
    activity_df: pl.DataFrame

    @classmethod
    def fetch(cls, query_range: str = "30d", session: requests.Session | None = None):
        session = session or new_session()

        l2beat_projects = L2BeatProjectsSummary.fetch(session)

        # Fetch TVL
        tvl_df = fetch_tvl(
            projects=l2beat_projects.projects,
            query_range=query_range,
        )

        # Fetch Actvity
        activity_df = fetch_activity(
            projects=l2beat_projects.projects,
            query_range=query_range,
        )

        return cls(
            df=l2beat_projects.summary_df,
            projects=l2beat_projects.projects,
            tvl_df=tvl_df,
            activity_df=activity_df,
        )


def parse_summary(summary):
    # Parse the summary and store as a dataframe.
    # (pedrod - 2025/01/31) L2Beat updated their naming from TVL to TVS.
    # Here we adapt the incoming data to match our existing schema.
    # This is a temporary patch while we work on migrating L2Beat to the
    # DailyData pattern.
    projects_summary = list(summary["projects"].values())

    df = (
        pl.DataFrame(projects_summary)
        .rename({"tvs": "tvl"})
        .with_columns(
            da_badge=pl.col("badges")
            .list.eval(
                pl.when(pl.element().struct["type"] == "DA")
                .then(pl.element().struct["name"])
                .otherwise(None)
            )
            .list.drop_nulls()
            .list.first(),
            vm_badge=pl.col("badges")
            .list.eval(
                pl.when(pl.element().struct["type"] == "VM")
                .then(pl.element().struct["name"])
                .otherwise(None)
            )
            .list.drop_nulls()
            .list.first(),
            # Extract nested values out of the tvl column.
            tvl_total=pl.col("tvl").struct["breakdown"].struct["total"],
            tvl_ether=pl.col("tvl").struct["breakdown"].struct["ether"],
            tvl_stablecoin=pl.col("tvl").struct["breakdown"].struct["stablecoin"],
            tvl_associated=pl.col("tvl").struct["breakdown"].struct["associated"],
            tvl_associated_tokens=pl.col("tvl").struct["associatedTokens"],
        )
        .drop("badges", "risks", "tvl")
    )

    raise_for_schema_mismatch(
        actual_schema=df.schema,
        expected_schema=pl.Schema(SUMMARY_SCHEMA),
    )

    return df
