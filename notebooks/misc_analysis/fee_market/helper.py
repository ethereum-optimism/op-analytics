import string
from op_analytics.coreutils.clickhouse.client import run_query
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def fetch_eip1559_params(chain: str, date: string) -> dict:
    """
    Fetch EIP-1559 and related system config params for a chain from ClickHouse, with GitHub fallback.

    Args:
        chain: Chain name (e.g., 'op', 'base')
        date: Date string in YYYY-MM-DD format

    Returns:
        Dict with relevant EIP-1559 and system config fields

    Returns:
        result_df (DataFrame) with all columns from the query.
    """
    try:
        query = f"""
        SELECT
            eip1559_elasticity
            ,eip1559_denominator
            ,gas_limit
            ,latest_block_number
            ,latest_block_timestamp
            ,basefee_scalar
            ,operator_fee_constant
            ,operator_fee_scalar
        FROM transforms_systemconfig.dim_superchain_system_configs_v1
        WHERE
            identifier = 'mainnet/{chain}'
            and date_trunc('day',latest_block_timestamp) = toDate('{date}')
        ORDER BY latest_block_number DESC
        LIMIT 1
        """

        result_df = run_query(instance="OPLABS", query=query)

        if result_df.is_empty():
            print(f"⚠️ No EIP-1559 params found for {chain} on {date}, trying GitHub fallback...")
            return _fetch_eip1559_params_from_github(chain)

        # Check if values are 0 (extract scalar from Series)
        elasticity = result_df["eip1559_elasticity"].item()
        denominator = result_df["eip1559_denominator"].item()

        if elasticity == 0 or denominator == 0:
            print(f"⚠️ Invalid EIP-1559 params (elasticity={elasticity}, denominator={denominator}) for {chain} on {date}, trying GitHub fallback...")
            return _fetch_eip1559_params_from_github(chain)

        print(f"✅ Fetched EIP-1559 params for {chain} ({date})")
        return result_df

    except Exception as e:
        print(f"❌ ClickHouse query failed for {chain}: {e}")
        print(f"🔄 Trying GitHub fallback for {chain}...")
        return _fetch_eip1559_params_from_github(chain)


def _fetch_eip1559_params_from_github(chain: str):
    """
    Fallback: fetch raw parameters from the superchain registry TOML (main branch).
    Returns a minimal DataFrame-like dict structure.
    """
    import requests
    import toml
    import pandas as pd

    url = (
        "https://raw.githubusercontent.com/ethereum-optimism/superchain-registry/"
        f"refs/heads/main/superchain/configs/mainnet/{chain}.toml"
    )
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    config = toml.loads(response.text)

    optimism = config.get("optimism", {}) or {}
    genesis = config.get("genesis", {}) or {}
    system_cfg = (genesis.get("system_config") or {})

    fallback_dict = {
        "eip1559_elasticity": [optimism.get("eip1559_elasticity")],
        "eip1559_denominator": [optimism.get("eip1559_denominator")],
        "gas_limit": [system_cfg.get("gasLimit")],
        "latest_block_number": [None],
        "latest_block_timestamp": [None],
        "basefee_scalar": [None],
        "operator_fee_constant": [None],
        "operator_fee_scalar": [None],
    }

    print(f"✅ Fetched fallback params for {chain} from GitHub")
    return pd.DataFrame(fallback_dict)

def plot_base_fee_and_fullness(df_output, chain_name: str, block_date: str, smoothing_window: int | None = None):
    """
    One plot with base_fee_per_gas (left y-axis) and block fullness % (right y-axis).

    Args:
        df_output: Polars DataFrame with at least columns datetime, base_fee_per_gas, gas_used, gas_limit.
        chain_name: str, chain label for title.
        block_date: str, date label for title.
        smoothing_window: int | None, optional window size for rolling mean (in blocks).
                          If None, plot raw block fullness.
    """
    # Compute block fullness (%)
    df_output = df_output.with_columns(
        ((df_output["gas_used"] / df_output["gas_limit"]) * 100).alias("block_fullness_pct")
    )

    # Convert to pandas for plotting
    df_plot = df_output.select(["datetime", "base_fee_per_gas", "block_fullness_pct"]).to_pandas()

    # Optionally smooth the fullness curve
    if smoothing_window and smoothing_window > 1:
        df_plot["block_fullness_smooth"] = df_plot["block_fullness_pct"].rolling(smoothing_window).mean()
        fullness_col = "block_fullness_smooth"
        fullness_label = f"Block Fullness (%) (MA{int(smoothing_window)})"
    else:
        fullness_col = "block_fullness_pct"
        fullness_label = "Block Fullness (%)"

    fig, ax1 = plt.subplots(figsize=(12, 5))

    # Base fee line (left axis)
    ax1.plot(df_plot["datetime"], df_plot["base_fee_per_gas"],
             color="tab:blue", linewidth=2, label="Base Fee (wei)")
    ax1.set_ylabel("Base Fee (wei)", color="tab:blue")
    ax1.tick_params(axis="y", labelcolor="tab:blue")

    # Fullness line (right axis)
    ax2 = ax1.twinx()
    ax2.plot(df_plot["datetime"], df_plot[fullness_col],
             color="tab:orange", linewidth=1.5, alpha=0.8, label=fullness_label)
    ax2.set_ylabel("Block Fullness (%)", color="tab:orange")
    ax2.tick_params(axis="y", labelcolor="tab:orange")
    ax2.set_ylim(0, 105)

    # Title & formatting
    ax1.set_title(f"Base Fee and Block Fullness on {block_date} ({chain_name})")
    ax1.set_xlabel("Time")
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())

    # Combined legend
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, loc="upper left", frameon=False)

    ax1.grid(alpha=0.3, linestyle="--")
    plt.tight_layout()
    plt.show()
