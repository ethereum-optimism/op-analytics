import polars as pl
import requests

from op_analytics.coreutils.request import new_session, get_data
from op_analytics.coreutils.threads import run_concurrently
from .utils import apply_schema, L2BeatProject


TVS_BREAKDOWN_SCHEMA: dict[str, type[pl.DataType]] = {
    "timestamp": pl.Int64,
    "asset_id": pl.Utf8,
    "chain_name": pl.Utf8,
    "chain_id": pl.Int64,
    "amount": pl.Float64,
    "usd_value": pl.Float64,
    "usd_price": pl.Float64,
    "is_gas_token": pl.Boolean,
    "token_address": pl.Utf8,
    "escrow_address": pl.Utf8,
    "escrow_name": pl.Utf8,
    "is_shared_escrow": pl.Boolean,
    "escrow_url": pl.Utf8,
    "asset_url": pl.Utf8,
    "icon_url": pl.Utf8,
    "symbol": pl.Utf8,
    "name": pl.Utf8,
    "supply": pl.Utf8,
    "category": pl.Utf8,
}


def fetch_tvs_breakdown(
    projects: list[L2BeatProject],
    session: requests.Session | None = None,
):
    session = session or new_session()

    all_data = run_concurrently(
        function=lambda x: get_data(
            session, url=f"https://l2beat.com/api/scaling/tvs/{x.slug}/breakdown"
        ),
        targets=projects,
        max_workers=8,
    )

    processed_data = {}
    for project, data in all_data.items():
        if not data.get("success"):
            continue

        breakdown = data["data"].get("breakdown", {})
        timestamp = data["data"]["dataTimestamp"]

        # Process each category and its assets
        rows = []
        for category, assets in breakdown.items():
            for asset in assets:
                for escrow in asset.get("escrows", []):
                    row = {
                        "timestamp": timestamp,
                        "asset_id": asset["assetId"],
                        "chain_name": asset["chain"]["name"],
                        "chain_id": asset["chain"]["id"],
                        "amount": asset["amount"],
                        "usd_value": asset["usdValue"],
                        "usd_price": float(asset["usdPrice"]),
                        "is_gas_token": asset["isGasToken"],
                        "token_address": asset.get("tokenAddress", ""),
                        "escrow_address": escrow["escrowAddress"],
                        "escrow_name": escrow["name"],
                        "is_shared_escrow": escrow["isSharedEscrow"],
                        "escrow_url": escrow["url"],
                        "asset_url": asset.get("url", ""),
                        "icon_url": asset["iconUrl"],
                        "symbol": asset["symbol"],
                        "name": asset["name"],
                        "supply": asset["supply"],
                        "category": category,
                    }
                    rows.append(row)

        # Format data to match the expected structure
        processed_data[project] = {
            "success": True,
            "data": {"chart": {"types": list(TVS_BREAKDOWN_SCHEMA.keys()), "data": rows}},
        }

    # Convert to dataframe using existing utility
    return apply_schema(processed_data, TVS_BREAKDOWN_SCHEMA)
