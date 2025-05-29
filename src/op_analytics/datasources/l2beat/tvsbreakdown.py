from dataclasses import dataclass
from typing import Any

import polars as pl
import requests

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.misc import raise_for_schema_mismatch
from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import dt_fromepoch

from .projects import L2BeatProject, L2BeatProjectsSummary

log = structlog.get_logger()

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
            max_workers=4,
        )

        rows = []
        for parsed_data in projects_tvs.values():
            if parsed_data.data is not None:
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
    data: list[dict[str, Any]] | None

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
            # Some projects are not found in the TVS breakdown API, they return:
            # {
            # "success": false,
            # "error": "Project not found."
            # }
            return cls(slug=project.slug, data=None)

        try:
            parsed_data = parse_tvs(data, project)
        except Exception as e:
            raise Exception(f"Failed to parse TVS data for project {project}") from e

        return cls(slug=project.slug, data=parsed_data)


def parse_tvs(data: dict[str, Any], project: L2BeatProject) -> list[dict[str, Any]]:
    breakdown = data["data"].get("breakdown", {})
    timestamp = data["data"]["dataTimestamp"]

    rows = []
    for category, assets in breakdown.items():
        for asset in assets:
            # Starting around 2025-04-17 the L2Beat API changed the structure of the JSON response
            # for the TVS breakdown. We have to work around those changes here.

            if "assetId" not in asset:
                # New JSON structure
                asset_id = asset["id"]
            else:
                asset_id = asset["assetId"]

            try:
                if "chain" not in asset:
                    chain_id = None
                    chain_name = None
                    # New JSON structure
                    formula = asset["formula"]
                    if "chain" in formula:
                        chain_name = formula["chain"]
                    elif "arguments" in formula:
                        for argument in formula["arguments"]:
                            if "chain" in argument:
                                chain_name = argument["chain"]
                                break
                else:
                    chain_id = asset["chain"]["id"]
                    chain_name = asset["chain"]["name"]

            except Exception:
                log.error(f"Failed to parse chain for asset {asset}")
                chain_id = None
                chain_name = None

            if "tokenAddress" not in asset:
                # New JSON structure
                if "address" in asset and isinstance(asset["address"], dict):
                    try:
                        token_address = asset["address"]["address"]
                    except KeyError:
                        token_address = None
                else:
                    token_address = None
            else:
                token_address = asset["tokenAddress"]

            url = None

            if "url" in asset:
                url = asset["url"]
            elif isinstance(asset.get("address"), dict):
                url = asset["address"].get("url")

            # We produce one row per asset per escrow. If there are no escrows we still need to
            # produce a row for the asset, so we set escrows to a list with a single null escrow.
            if "escrows" in asset:
                escrows = asset["escrows"]
            elif "escrow" in asset:
                if asset["escrow"] == "multiple":
                    escrows = asset["formula"]["arguments"]
                else:
                    escrows = [asset.get("escrow")]
            else:
                escrows = [None]

            def is_valid_escrow(esc: dict[str, Any] | None) -> bool:
                return (
                    isinstance(esc, dict)
                    and esc.get("type") == "balanceOfEscrow"
                    and "escrowAddress" in esc
                )

            def get_escrow_address(esc: dict[str, Any]) -> str | None:
                if "address" in esc:
                    return esc["address"]
                return esc.get("escrowAddress")

            for esc in escrows:
                if not is_valid_escrow(esc):
                    # Create a row with null escrow data
                    row = {
                        "dt": dt_fromepoch(timestamp),
                        "project_id": project.id,
                        "project_slug": project.slug,
                        "timestamp": timestamp,
                        "asset_id": asset_id,
                        "chain_name": chain_name,
                        "chain_id": chain_id,
                        "amount": asset["amount"],
                        "usd_value": float(asset["usdValue"]) if "usdValue" in asset else None,
                        "usd_price": float(asset["usdPrice"]) if "usdPrice" in asset else None,
                        "is_gas_token": asset.get("isGasToken"),
                        "token_address": token_address,
                        "escrow_address": None,
                        "escrow_name": None,
                        "is_shared_escrow": None,
                        "escrow_url": None,
                        "asset_url": url,
                        "icon_url": asset.get("iconUrl"),
                        "symbol": asset["symbol"],
                        "name": asset.get("name"),
                        "supply": asset.get("supply"),
                        "category": category,
                    }
                    rows.append(row)
                    continue

                escrow_address = get_escrow_address(esc)
                if not escrow_address:
                    log.error(f"Unknown escrow structure: {esc}")
                    continue

                row = {
                    "dt": dt_fromepoch(timestamp),
                    "project_id": project.id,
                    "project_slug": project.slug,
                    "timestamp": timestamp,
                    "asset_id": asset_id,
                    "chain_name": chain_name,
                    "chain_id": chain_id,
                    "amount": asset["amount"],
                    "usd_value": float(asset["usdValue"]) if "usdValue" in asset else None,
                    "usd_price": float(asset["usdPrice"]) if "usdPrice" in asset else None,
                    "is_gas_token": asset.get("isGasToken"),
                    "token_address": token_address,
                    "escrow_address": escrow_address,
                    "escrow_name": esc.get("name"),
                    "is_shared_escrow": esc.get("isSharedEscrow"),
                    "escrow_url": esc.get("url"),
                    "asset_url": url,
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
        pl.DataFrame(
            rows,
            schema_overrides={
                "token_address": pl.String,
                "escrow_address": pl.String,
                "chain_id": pl.Int64,
                "usd_price": pl.Float64,
                "is_shared_escrow": pl.Boolean,
                "supply": pl.String,
            },
        )
        .select(_[0] for _ in TVS_BREAKDOWN_SCHEMA)
        .with_columns(
            pl.col("token_address").str.to_lowercase().alias("token_address"),
            pl.col("escrow_address").str.to_lowercase().alias("escrow_address"),
        )
    )
