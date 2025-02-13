import re
from dataclasses import dataclass
from functools import cache

import polars as pl


from op_analytics.datapipeline.chains.load import load_chain_metadata
from op_analytics.coreutils.clickhouse.oplabs import run_query_oplabs
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.logger import structlog


log = structlog.get_logger()


@dataclass(frozen=True)
class TokenContract:
    chain: str
    chain_id: int
    contract_address: str


def execute_pull():
    # Find the list of ERC20 tokens that are not yet in our metadata table.
    tokens_to_fetch = run_query_oplabs("""
    SELECT
        t.chain, 
        t.chain_id, 
        CAST(t.contract_address AS String) AS contract_address
    FROM transforms_erc20transfers.fact_erc20_daily_transfers_v1 t FINAL
        LEFT ANTI JOIN onchain.dim_erc20_token_metadata_v1
        USING (chain, chain_id, contract_address)
    WHERE t.dt = '2025-02-10' 
    ORDER BY num_transfers DESC
    LIMIT 1000
    """).to_dicts()

    targets = []
    for token in tokens_to_fetch:
        targets.append(
            TokenContract(
                chain=token["chain"],
                chain_id=token["chain_id"],
                contract_address=token["contract_address"],
            )
        )

    results = run_concurrently(
        function=lambda x: get_token_metadata(x),
        targets=targets[:10],
        max_workers=4,
    )

    return results


def get_rpc_for_chain(chain_id: int) -> str:
    df = load_chain_metadata()
    rpcs = (
        df.filter(pl.col("oplabs_db_schema").is_not_null())
        .select(
            pl.col("oplabs_db_schema").alias("chain"),
            pl.col("mainnet_chain_id").alias("chain_id"),
            pl.col("rpc_url"),
        )
        .filter(pl.col("chain_id") == chain_id)
        .to_dicts()
    )
    if len(rpcs) == 0:
        raise Exception(f"could not find rpc for chain_id: {chain_id}")
    return rpcs[0]["rpc_url"]


METHOD_NAMES = [
    "decimals",
    "symbol",
    "name",
    "totalSupply",
]


@cache
def get_method_ids():
    from web3 import Web3

    return {name: Web3.keccak(text=f"{name}()")[:4].hex() for name in METHOD_NAMES}


def get_token_metadata(token: TokenContract):
    """
    Aggregates multiple contract calls into one Python function call.
    (Still multiple calls to your Ethereum node, but simpler to manage in code.)
    """
    from web3 import Web3

    w3_conn = Web3(Web3.HTTPProvider(get_rpc_for_chain(token.chain_id)))

    # Get the block info.
    block = w3_conn.eth.get_block("latest")

    # Call each method.

    try:
        response_map = {}
        for method_name, method_id in get_method_ids().items():
            response = w3_conn.eth.call({"to": token.contract_address, "data": method_id})
            response_map[method_name] = response

        # Parse rsponses.
        decimals = Web3.to_int(hexstr=response_map["decimals"].hex())
        symbol = clean_string(Web3.to_text(hexstr=response_map["symbol"].hex()))
        name = clean_string(Web3.to_text(hexstr=response_map["name"].hex()))
        total_supply = Web3.to_int(hexstr=response_map["totalSupply"].hex())

        log.info(f"fetched token metadata for {token}")

        return {
            "chain": token.chain,
            "chain_id": token.chain_id,
            "contract_address": token.contract_address,
            #
            # Metadata
            "decimals": decimals,
            "symbol": symbol,
            "name": name,
            "total_supply": total_supply,
            #
            # Block when metadata was last retrieved from RPC
            "block_number": block.number,
            "block_timestamp": block.timestamp,
        }
    except:
        return {}


def clean_string(raw_str: str) -> str:
    """
    Remove all control characters (ASCII 0–31, plus 127),
    then strip leading/trailing whitespace.
    """
    return re.sub(r"[\x00-\x1F\x7F]+", "", raw_str).strip()
