from dataclasses import dataclass
from typing import Any

import polars as pl
import requests

from op_analytics.coreutils.misc import raise_for_schema_mismatch
from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import dt_fromepoch

from .projects import L2BeatProject, L2BeatProjectsSummary

TVS_BREAKDOWN_SCHEMA: list[tuple[str, type[pl.DataType]]] = [
    ("dt", pl.String),
    ("project_id", pl.Utf8),
    ("project_slug", pl.Utf8),
    ("timestamp", pl.Int64),
    ("asset_id", pl.Utf8),
    ("chain_name", pl.Utf8),
    ("chain_id", pl.Int64),
    ("amount", pl.Float64),
    ("usd_value", pl.Float64),
    ("usd_price", pl.Float64),
    ("is_gas_token", pl.Boolean),
    ("token_address", pl.Utf8),
    ("escrow_address", pl.Utf8),
    ("escrow_name", pl.Utf8),
    ("is_shared_escrow", pl.Boolean),
    ("escrow_url", pl.Utf8),
    ("asset_url", pl.Utf8),
    ("icon_url", pl.Utf8),
    ("symbol", pl.Utf8),
    ("name", pl.Utf8),
    ("supply", pl.Utf8),
    ("category", pl.Utf8),
]


@dataclass
class L2BeatTVSBreakdown:
    df: pl.DataFrame

    @classmethod
    def fetch(
        cls,
        projects: list[L2BeatProject] | None = None,
        session: requests.Session | None = None,
    ) -> "L2BeatTVSBreakdown":
        session = session or new_session()

        # Get the list of projects from L2Beat
        l2beat_projects = projects or L2BeatProjectsSummary.fetch(session).projects

        # Fetch the TVS breakdown for each project
        projects_tvs = run_concurrently(
            function=lambda project: L2BeatProjectTVS.fetch(project, session),
            targets=l2beat_projects,
            max_workers=8,
        )

        rows = []
        for parsed_data in projects_tvs.values():
            rows.extend(parsed_data.data)

        df = clean_dataframe(rows)

        # Check the data coming from the API is just as we expect it to be.
        raise_for_schema_mismatch(
            actual_schema=df.schema,
            expected_schema=pl.Schema(TVS_BREAKDOWN_SCHEMA),
        )

        return cls(df=df)


@dataclass
class L2BeatProjectTVS:
    slug: str
    data: list[dict[str, Any]]

    @classmethod
    def fetch(
        cls,
        project: L2BeatProject,
        session: requests.Session | None = None,
    ) -> "L2BeatProjectTVS":
        session = session or new_session()

        data = get_data(
            session,
            url=f"https://l2beat.com/api/scaling/tvs/{project.slug}/breakdown",
        )

        if not data.get("success"):
            raise Exception(f"Failed to fetch data for project {project.slug}")

        return cls(slug=project.slug, data=parse_tvs(data, project))


def parse_tvs(data: dict[str, Any], project: L2BeatProject) -> list[dict[str, Any]]:
    breakdown = data["data"].get("breakdown", {})
    timestamp = data["data"]["dataTimestamp"]

    rows = []
    for category, assets in breakdown.items():
        for asset in assets:
            # We produce one row per asset per escrow. If there are no escrows we still need to
            # produce a row for the asset, so we set escrows to a list with a single null escrow.
            escrows = asset.get("escrows") or [None]

            for escrow in escrows:
                row = {
                    "dt": dt_fromepoch(timestamp),
                    "project_id": project.id,
                    "project_slug": project.slug,
                    "timestamp": timestamp,
                    "asset_id": asset["assetId"],
                    "chain_name": asset["chain"]["name"],
                    "chain_id": asset["chain"]["id"],
                    "amount": asset["amount"],
                    "usd_value": asset["usdValue"],
                    "usd_price": float(asset["usdPrice"]),
                    "is_gas_token": asset.get("isGasToken"),
                    "token_address": asset.get("tokenAddress"),
                    "escrow_address": escrow.get("escrowAddress") if escrow else None,
                    "escrow_name": escrow.get("name") if escrow else None,
                    "is_shared_escrow": escrow.get("isSharedEscrow") if escrow else None,
                    "escrow_url": escrow.get("url") if escrow else None,
                    "asset_url": asset.get("url"),
                    "icon_url": asset.get("iconUrl"),
                    "symbol": asset["symbol"],
                    "name": asset.get("name"),
                    "supply": asset.get("supply"),
                    "category": category,
                }
                rows.append(row)
    return rows


def clean_dataframe(rows: list[dict[str, Any]]) -> pl.DataFrame:
    return (
        pl.DataFrame(rows)
        .select(_[0] for _ in TVS_BREAKDOWN_SCHEMA)
        .with_columns(
            pl.col("token_address").str.to_lowercase().alias("token_address"),
            pl.col("escrow_address").str.to_lowercase().alias("escrow_address"),
        )
    )
