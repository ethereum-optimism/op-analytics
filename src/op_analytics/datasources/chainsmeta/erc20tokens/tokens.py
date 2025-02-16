import time
from dataclasses import asdict, dataclass
from typing import Any

import requests
import stamina

from op_analytics.coreutils.logger import structlog

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

    @stamina.retry(on=RateLimit, attempts=3, wait_initial=3)
    def call_rpc(
        self,
        session: requests.Session,
        rpc_endpoint: str,
        speed_bump: float,
    ) -> "TokenMetadata":
        start = time.perf_counter()
        response = session.post(rpc_endpoint, json=self.rpc_batch())
        result = TokenMetadata.of(token=self, response=response.json())

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
    def of(cls, token: Token, response: list[dict]):
        data: dict[str, Any] = {}
        for item in response:
            if "error" in item:
                code = item["error"].get("code")
                if code == -32016:
                    raise RateLimit(f"JSON-RPC error: {item}")

                raise Exception(f"JSON-RPC error: {item}")

            if "result" not in item:
                raise Exception(f"JSON-RPC missing result: {item}")

            if item["id"] == BLOCK_REQUEST_ID:
                data["block_number"] = int(item["result"]["number"], 16)
                data["block_timestamp"] = int(item["result"]["timestamp"], 16)

            elif item["id"] in ERC20_METHODS:
                data[item["id"]] = item["result"]

            else:
                raise Exception("invalid item id: " + item["id"])

        return cls(
            chain=token.chain,
            chain_id=token.chain_id,
            contract_address=token.contract_address,
            block_number=data["block_number"],
            block_timestamp=data["block_timestamp"],
            decimals=decode("uint8", data["decimals"]),
            symbol=decode_string(data["symbol"]),
            name=decode_string(data["name"]),
            total_supply=decode("uint256", data["totalSupply"]),
        )


def decode_string(hexstr: str):
    return decode("(string)", hexstr)[0]


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
