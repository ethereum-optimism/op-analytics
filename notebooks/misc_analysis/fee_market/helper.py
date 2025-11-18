import string
from op_analytics.coreutils.clickhouse.client import run_query


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
