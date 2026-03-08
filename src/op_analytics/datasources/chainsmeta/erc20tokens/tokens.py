import time
from dataclasses import asdict, dataclass
from typing import Any, Optional

import requests
import stamina
from requests.exceptions import (
    JSONDecodeError,
    SSLError,
    ConnectionError,
    Timeout,
    RequestException,
)

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import new_session

from .endpoints import get_rpc_for_chain

log = structlog.get_logger()


# JSON-RPC Request IDs
BLOCK_REQUEST_ID = "block"


# JSON-RPC Block request
BLOCK_REQUEST = {
    "jsonrpc": "2.0",
    "method": "eth_getBlockByNumber",
    "params": ["latest", False],
    "id": BLOCK_REQUEST_ID,
}


# ERC-20 Contract method IDs
ERC20_METHODS = {
    "decimals": "0x313ce567",
    "symbol": "0x95d89b41",
    "name": "0x06fdde03",
    "totalSupply": "0x18160ddd",
}


class RateLimit(Exception):
    """Raised when a rate limit error is encountered on the JSON-RPC response."""

    pass


class RPCConnectionError(Exception):
    """Raised when there's a connection error to the RPC endpoint."""

    pass


class TokenMetadataError(Exception):
    """Raised when we fail to parse the RPC response."""

    pass


class TokenResponseError(Exception):
    """Raised when the RPC response is invalid json."""

    pass


@dataclass(frozen=True)
class Token:
    chain: str
    chain_id: int
    contract_address: str

    @classmethod
    def from_database_row(cls, row: dict) -> "Token":
        return cls(
            chain=row["chain"],
            chain_id=row["chain_id"],
            # In the database the contract address is stored as a Fixed(42) String
            # Which returns bytes on queries.
            contract_address=row["contract_address"].decode(),
        )

    def rpc_batch(self):
        """Build the batch of RPC calls needed to update the token metadata."""
        batch = []

        # The first request is for the block number and timestamp.
        batch.append(BLOCK_REQUEST)

        # Following requests are for the token metadata.
        for method_name, method_id in ERC20_METHODS.items():
            batch.append(
                {
                    "jsonrpc": "2.0",
                    "method": "eth_call",
                    "params": [
                        {"to": self.contract_address, "data": method_id},
                        "latest",
                    ],
                    "id": method_name,
                }
            )

        return batch

    @stamina.retry(on=(RateLimit, RPCConnectionError), attempts=3, wait_initial=10)
    def call_rpc(
        self,
        rpc_endpoint: str | None = None,
        session: requests.Session | None = None,
        speed_bump: float = 0.05,
    ) -> Optional["TokenMetadata"]:
        session = session or new_session()
        rpc_endpoint = rpc_endpoint or get_rpc_for_chain(chain_id=self.chain_id)

        start = time.perf_counter()
        try:
            response = session.post(rpc_endpoint, json=self.rpc_batch())
        except (SSLError, ConnectionError, Timeout, RequestException) as ex:
            log.warning(f"RPC connection error for {self} at {rpc_endpoint}: {ex}")
            raise RPCConnectionError(f"Connection error to RPC endpoint: {ex}") from ex

        try:
            response_data = response.json()
        except JSONDecodeError as ex:
            raise TokenResponseError(dict(token=self)) from ex

        try:
            result = TokenMetadata.of(token=self, response=response_data)
        except TokenMetadataError as ex:
            log.warning(f"failed to parse token metadata for {self}: {ex}")
            return None

        ellapsed = time.perf_counter() - start
        if ellapsed < speed_bump:
            dur = round(speed_bump - ellapsed, 2)
            time.sleep(dur)

        return result


@dataclass
class TokenMetadata:
    chain: str
    chain_id: int
    contract_address: str

    #
    block_number: int
    block_timestamp: int
    #
    decimals: int
    symbol: str
    name: str
    total_supply: int

    def to_dict(self):
        return asdict(self)

    @classmethod
    def of(cls, token: Token, response: list[dict]) -> Optional["TokenMetadata"]:
        data: dict[str, Any] = {}
        failed_methods = []

        for item in response:
            if "error" in item:
                code = item["error"].get("code")
                if code == -32016:  # rate limit
                    raise RateLimit(f"JSON-RPC error: {item} [{token}]")

                # Handle various error codes that indicate missing functions or contract issues
                # -32000: execution reverted (function doesn't exist or reverts)
                # -32001: method not found
                # -32002: invalid params
                # -32003: internal error
                # -32004: invalid request
                # -32005: parse error
                # 3: execution reverted with specific message (e.g., "Contract does not have fallback nor receive functions")
                if code in [
                    -32000,
                    -32001,
                    -32002,
                    -32003,
                    -32004,
                    -32005,
                    3,
                ]:  # execution reverted, method not found, contract issues, etc.
                    method_name = item.get("id", "unknown")
                    failed_methods.append(method_name)
                    log.debug(f"method {method_name} failed with code {code} for {token}")
                    continue

                if code == 3:  # "execution reverted"
                    log.warning(f"rpc returned 3 'execution reverted' {token}")
                    return None

                raise Exception(f"JSON-RPC error: {item} [{token}]")

            if "result" not in item:
                raise Exception(f"JSON-RPC missing result: {item} [{token}]")

            if item["id"] == BLOCK_REQUEST_ID:
                data["block_number"] = int(item["result"]["number"], 16)
                data["block_timestamp"] = int(item["result"]["timestamp"], 16)

            elif item["id"] in ERC20_METHODS:
                data[item["id"]] = item["result"]

            else:
                raise Exception("invalid item id: " + item["id"])

        # If we have failed methods, log them and check if we can still proceed
        if failed_methods:
            log.warning(
                f"methods {failed_methods} failed for {token}, checking if we can still proceed"
            )

            # Check if we have the minimum required data to create token metadata
            required_methods = ["decimals", "symbol", "name", "totalSupply"]
            missing_required = [method for method in required_methods if method not in data]

            if missing_required:
                log.warning(
                    f"missing required methods {missing_required} for {token}, skipping token"
                )
                return None

        # Helper functions for safe decoding with defaults
        def safe_decode(key: str, decode_type: str, default_value: Any = None):
            if key in data:
                try:
                    return decode(decode_type, data[key])
                except Exception as ex:
                    log.warning(f"failed to decode {key} for {token}: {ex}")
                    return default_value
            return default_value

        def safe_decode_string(key: str, default_value: str = "UNKNOWN"):
            if key in data:
                try:
                    return decode_string(data[key])
                except Exception as ex:
                    log.warning(f"failed to decode string {key} for {token}: {ex}")
                    return default_value
            return default_value

        try:
            return cls(
                chain=token.chain,
                chain_id=token.chain_id,
                contract_address=token.contract_address,
                block_number=data["block_number"],
                block_timestamp=data["block_timestamp"],
                decimals=safe_decode("decimals", "uint8", 18),  # Default to 18 decimals
                symbol=safe_decode_string("symbol", "UNKNOWN"),
                name=safe_decode_string("name", "Unknown Token"),
                total_supply=safe_decode("totalSupply", "uint256", 0),  # Default to 0
            )
        except Exception as ex:
            raise TokenMetadataError(dict(data=data, token=token)) from ex


def decode_string(hexstr: str):
    # Some tokens have invalid utf-8 on their symbol or name.
    # We decode as bytes and then handle replacing invalid characters.
    asbytes = decode("(bytes)", hexstr)[0]

    # From: https://docs.python.org/3/library/codecs.html#error-handlers
    # On decoding, use � (U+FFFD, the official REPLACEMENT CHARACTER).
    return asbytes.decode("utf-8", errors="replace")


def decode(typestr: str, hexstr: str):
    from eth_abi_lite.abi import default_codec

    return default_codec.decode_single(typestr, bytearray.fromhex(hexstr[2:]))


# Example RPC response (the block response has been edited to only show number and timestamp):
# [
#     {
#         "jsonrpc": "2.0",
#         "result": {
#         "number": "0x1934a94",
#         "timestamp": "0x67b0f20b",
#         },
#         "id": "block"
#     },
#     {
#         "jsonrpc": "2.0",
#         "result": "0x0000000000000000000000000000000000000000000000000000000000000012",
#         "id": "decimals"
#     },
#     {
#         "jsonrpc": "2.0",
#         "result": "0x000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000054265744d65000000000000000000000000000000000000000000000000000000",
#         "id": "symbol"
#     },
#     {
#         "jsonrpc": "2.0",
#         "result": "0x000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000054265744d65000000000000000000000000000000000000000000000000000000",
#         "id": "name"
#     },
#     {
#         "jsonrpc": "2.0",
#         "result": "0x0000000000000000000000000000000000000000033b2e3c9fd0803ce8000000",
#         "id": "totalSupply"
#     }
# ]
