from dataclasses import dataclass
from functools import cache

import polars as pl
import requests

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
    PERCENTILE = 0.90

    query = f"""
        SELECT
            derived.chain,
            derived.chain_id,
            derived.contract_address,
            derived.num_transfers
        FROM (
            SELECT
                t.chain,
                t.chain_id,
                CAST(t.contract_address AS String) AS contract_address,
                t.num_transfers,
                quantile({PERCENTILE})(t.num_transfers) OVER (PARTITION BY t.chain, t.chain_id) AS percentile_num_transfers
            FROM transforms_erc20transfers.fact_erc20_daily_transfers_v1 AS t FINAL
            LEFT ANTI JOIN onchain.dim_erc20_token_metadata_v1 USING (chain, chain_id, contract_address)
            WHERE t.dt = (select max(dt) from transforms_erc20transfers.fact_erc20_daily_transfers_v1)
        ) AS derived
        WHERE derived.num_transfers >= derived.percentile_num_transfers
        LIMIT 100
        """

    tokens_to_fetch = run_query_oplabs(query).to_dicts()

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


@cache
def get_method_ids():
    return {
        "decimals": "0x313ce567",
        "symbol": "0x95d89b41",
        "name": "0x06fdde03",
        "totalSupply": "0x18160ddd",
    }


def get_token_metadata(token: TokenContract):
    """
    Aggregates multiple contract calls into one Python function call.
    (Still multiple calls to your Ethereum node, but simpler to manage in code.)
    """
    # Construct JSON-RPC payloads for contract calls + block queries
    payloads = []
    request_id = 1

    try:
        # 1) Add ERC20 method calls
        for method_id in get_method_ids().values():
            payloads.append(
                {
                    "jsonrpc": "2.0",
                    "method": "eth_call",
                    "params": [{"to": token.contract_address, "data": method_id}, "latest"],
                    "id": request_id,
                }
            )
            request_id += 1

        # 2) Add a request for current block number
        payloads.append(
            {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": request_id}
        )
        request_id += 1

        # 3) Add a request for the latest block details
        payloads.append(
            {
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": ["latest", False],
                "id": request_id,
            }
        )
        request_id += 1

        response = requests.post(get_rpc_for_chain(token.chain_id), json=payloads)
        results = response.json()

        # Parse
        decimals = int(results[0]["result"], 16)
        symbol = decode_abi_encoded_string(results[1]["result"])
        name = decode_abi_encoded_string(results[2]["result"])
        total_supply = int(results[3]["result"], 16)
        block_number_hex = results[4]["result"]
        latest_block_data = results[5]["result"]

        block_number = int(block_number_hex, 16)
        block_timestamp = int(latest_block_data["timestamp"], 16)

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
            "block_number": block_number,
            "block_timestamp": block_timestamp,
        }

    except Exception as e:
        log.error(f"error fetching token metadata for {token.contract_address}: {e}")
        return {}


def decode_abi_encoded_string(hex_str: str) -> str:
    """
    Decode an ABI-encoded dynamic string that comes back
    from an eth_call returning (string).
    """
    # Strip off 0x prefix, if present
    if hex_str.startswith("0x"):
        hex_str = hex_str[2:]
    data = bytes.fromhex(hex_str)

    # The next 32 bytes are the string length
    str_len = int.from_bytes(data[32:64], byteorder="big")

    # The next str_len bytes are the actual UTF-8 bytes of the string
    string_bytes = data[64 : 64 + str_len]

    return string_bytes.decode("utf-8", errors="replace")
