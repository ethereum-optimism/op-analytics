import time
from dataclasses import asdict, dataclass
from typing import Any, Optional

import requests
import stamina
from requests.exceptions import JSONDecodeError

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import new_session

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


# System Config Contract method IDs
SYSTEM_CONFIG_METHODS = {
    "BATCH_INBOX_SLOT": "0xbc49ce5f",
    "DISPUTE_GAME_FACTORY_SLOT": "0xe2a3285c",
    "L1_CROSS_DOMAIN_MESSENGER_SLOT": "0x5d73369c",
    "L1_ERC_721_BRIDGE_SLOT": "0x19f5cea8",
    "L1_STANDARD_BRIDGE_SLOT": "0xf8c68de0",
    "OPTIMISM_MINTABLE_ERC20_FACTORY_SLOT": "0x06c92657",
    "OPTIMISM_PORTAL_SLOT": "0xfd32aa0f",
    "START_BLOCK_SLOT": "0xe0e2016d",
    "UNSAFE_BLOCK_SIGNER_SLOT": "0x4f16540b",
    "VERSION": "0xffa1ad74",
    "basefeeScalar": "0xbfb14fb7",
    "batchInbox": "0xdac6e63a",
    "batcherHash": "0xe81b2c6d",
    "blobbasefeeScalar": "0xec707517",
    "disputeGameFactory": "0xf2b4e617",
    "eip1559Denominator": "0xd220a9e0",
    "eip1559Elasticity": "0xc9ff2d16",
    "gasLimit": "0xf68016b7",
    "l1CrossDomainMessenger": "0xa7119869",
    "l1ERC721Bridge": "0xc4e8ddfa",
    "l1StandardBridge": "0x078f29cf",
    "maximumGasLimit": "0x0ae14b1b",
    "minimumGasLimit": "0x4add321d",
    "operatorFeeConstant": "0x16d3bc7f",
    "operatorFeeScalar": "0x4d5d9a2a",
    "minBaseFee": "0xa62611a2",
    "daFootprintGasScalar": "0xfe3d5710",
    "optimismMintableERC20Factory": "0x9b7d7f0a",
    "optimismPortal": "0x0a49cb03",
    "overhead": "0x0c18c162",
    "owner": "0x8da5cb5b",
    "scalar": "0xf45e65d8",
    "startBlock": "0x48cd4cb1",
    "unsafeBlockSigner": "0x1fd19ee1",
    "version": "0x54fd4d50",
}


class RateLimit(Exception):
    """Raised when a rate limit error is encountered on the JSON-RPC response."""

    pass


class SystemConfigError(Exception):
    """Raised when we fail to parse the RPC response."""

    pass


class SystemConfigResponseError(Exception):
    """Raised when the RPC response is invalid json."""

    pass


@dataclass(frozen=True)
class RPCManager:
    """A system config contract for a blockchain."""

    system_config_proxy: str

    def rpc_batch(self):
        """Build the batch of RPC calls needed to update the system config metadata."""
        batch = []

        # The first request is for the block number and timestamp.
        batch.append(BLOCK_REQUEST)

        # Following requests are for the system config metadata.
        for method_name, method_id in SYSTEM_CONFIG_METHODS.items():
            batch.append(
                {
                    "jsonrpc": "2.0",
                    "method": "eth_call",
                    "params": [
                        {"to": self.system_config_proxy, "data": method_id},
                        "latest",
                    ],
                    "id": method_name,
                }
            )

        return batch

    @stamina.retry(on=RateLimit, attempts=3, wait_initial=10)
    def call_rpc(
        self,
        rpc_endpoint: str,
        session: requests.Session | None = None,
        speed_bump: float = 0.05,
    ) -> Optional["SystemConfigMetadata"]:
        session = session or new_session()

        start = time.perf_counter()
        response = session.post(rpc_endpoint, json=self.rpc_batch())

        try:
            response_data = response.json()
        except JSONDecodeError as ex:
            raise SystemConfigResponseError(dict(config=self)) from ex

        result = SystemConfigMetadata.of(config=self, response=response_data)

        ellapsed = time.perf_counter() - start
        if ellapsed < speed_bump:
            dur = round(speed_bump - ellapsed, 2)
            time.sleep(dur)

        return result


@dataclass
class SystemConfigMetadata:
    """Metadata for a system config contract."""

    contract_address: str
    block_number: int
    block_timestamp: int
    batch_inbox_slot: str
    dispute_game_factory_slot: str
    l1_cross_domain_messenger_slot: str
    l1_erc721_bridge_slot: str
    l1_standard_bridge_slot: str
    optimism_mintable_erc20_factory_slot: str
    optimism_portal_slot: str
    start_block_slot: str
    unsafe_block_signer_slot: str
    version: Optional[int]
    basefee_scalar: Optional[int]
    batch_inbox: Optional[str]
    batcher_hash: Optional[str]
    blob_basefee_scalar: Optional[int]
    dispute_game_factory: Optional[str]
    eip1559_denominator: Optional[int]
    eip1559_elasticity: Optional[int]
    gas_limit: Optional[int]
    l1_cross_domain_messenger: Optional[str]
    l1_erc721_bridge: Optional[str]
    l1_standard_bridge: Optional[str]
    maximum_gas_limit: Optional[int]
    minimum_gas_limit: Optional[int]
    operator_fee_constant: Optional[int]
    operator_fee_scalar: Optional[int]
    min_base_fee: Optional[int]
    da_footprint_gas_scalar: Optional[int]
    optimism_mintable_erc20_factory: Optional[str]
    optimism_portal: Optional[str]
    overhead: Optional[int]
    owner: Optional[str]
    scalar: Optional[int]
    start_block: Optional[str]
    unsafe_block_signer: Optional[str]
    version_hex: Optional[str]

    def to_dict(self):
        return asdict(self)

    @classmethod
    def of(cls, config: RPCManager, response: list[dict]) -> Optional["SystemConfigMetadata"]:
        """Parse the RPC response into SystemConfigMetadata, handling errors and missing fields."""
        data: dict[str, Any] = {}
        failed_methods = []
        for item in response:
            if "error" in item:
                code = item["error"].get("code")
                if code == -32016:  # rate limit
                    raise RateLimit(f"JSON-RPC error: {item} [{config}]")

                if code in (-32000, 3):  # "execution reverted"
                    # Log the failed method but continue processing
                    method_name = item.get("id", "unknown")
                    failed_methods.append(method_name)
                    log.debug(f"method {method_name} failed with -32000 for {config}")
                    continue

                raise Exception(f"JSON-RPC error: {item} [{config}]")

            if "result" not in item:
                raise Exception(f"JSON-RPC missing result: {item} [{config}]")

            if item["id"] == BLOCK_REQUEST_ID:
                data["block_number"] = int(item["result"]["number"], 16)
                data["block_timestamp"] = int(item["result"]["timestamp"], 16)

            elif item["id"] in SYSTEM_CONFIG_METHODS:
                data[item["id"]] = item["result"]

            else:
                raise Exception("invalid item id: " + item["id"])

        if failed_methods:
            log.info(f"methods {failed_methods} not supported by {config}, using default values")

        def safe_decode(key: str, decode_type: str, default_value: Any = None):
            if key in data:
                return decode(decode_type, data[key])
            return default_value

        def safe_decode_address(key: str, default_value: str | None = None) -> str | None:
            if key in data:
                return decode_address(data[key])
            return default_value

        def safe_get(key: str, default_value: Any = None):
            if key in data:
                return data[key]
            return default_value

        def safe_decode_string(key: str, default_value: str | None = None) -> str | None:
            if key in data:
                return decode_string_from_abi(data[key])
            return default_value

        try:
            return cls(
                contract_address=config.system_config_proxy,
                block_number=data["block_number"],
                block_timestamp=data["block_timestamp"],
                batch_inbox_slot=safe_get("BATCH_INBOX_SLOT"),
                dispute_game_factory_slot=safe_get("DISPUTE_GAME_FACTORY_SLOT"),
                l1_cross_domain_messenger_slot=safe_get("L1_CROSS_DOMAIN_MESSENGER_SLOT"),
                l1_erc721_bridge_slot=safe_get("L1_ERC_721_BRIDGE_SLOT"),
                l1_standard_bridge_slot=safe_get("L1_STANDARD_BRIDGE_SLOT"),
                optimism_mintable_erc20_factory_slot=safe_get(
                    "OPTIMISM_MINTABLE_ERC20_FACTORY_SLOT"
                ),
                optimism_portal_slot=safe_get("OPTIMISM_PORTAL_SLOT"),
                start_block_slot=safe_get("START_BLOCK_SLOT"),
                unsafe_block_signer_slot=safe_get("UNSAFE_BLOCK_SIGNER_SLOT"),
                version=safe_decode("VERSION", "uint256", None),
                basefee_scalar=safe_decode("basefeeScalar", "uint32", None),
                batch_inbox=safe_decode_address("batchInbox"),
                batcher_hash=safe_get("batcherHash"),
                blob_basefee_scalar=safe_decode("blobbasefeeScalar", "uint64", None),
                dispute_game_factory=safe_decode_address("disputeGameFactory"),
                eip1559_denominator=safe_decode("eip1559Denominator", "uint32", None),
                eip1559_elasticity=safe_decode("eip1559Elasticity", "uint32", None),
                gas_limit=safe_decode("gasLimit", "uint64", None),
                l1_cross_domain_messenger=safe_decode_address("l1CrossDomainMessenger"),
                l1_erc721_bridge=safe_decode_address("l1ERC721Bridge"),
                l1_standard_bridge=safe_decode_address("l1StandardBridge"),
                maximum_gas_limit=safe_decode("maximumGasLimit", "uint64", None),
                minimum_gas_limit=safe_decode("minimumGasLimit", "uint64", None),
                operator_fee_constant=safe_decode("operatorFeeConstant", "uint64", None),
                operator_fee_scalar=safe_decode("operatorFeeScalar", "uint32", None),
                min_base_fee=safe_decode("minBaseFee", "uint64", None),
                da_footprint_gas_scalar=safe_decode("daFootprintGasScalar", "uint16", None),
                optimism_mintable_erc20_factory=safe_decode_address("optimismMintableERC20Factory"),
                optimism_portal=safe_decode_address("optimismPortal"),
                overhead=safe_decode("overhead", "uint256", None),
                owner=safe_decode_address("owner"),
                scalar=safe_decode("scalar", "uint256", None),
                start_block=safe_decode_address("startBlock"),
                unsafe_block_signer=safe_decode_address("unsafeBlockSigner"),
                version_hex=safe_decode_string("version"),
            )
        except Exception as ex:
            raise SystemConfigError(dict(data=data, config=config)) from ex


# --- Decoding Helpers ---
def decode_address(hexstr: str) -> str:
    """Decode an Ethereum address from hex."""
    return "0x" + hexstr[-40:]


def decode(typestr: str, hexstr: str):
    """Decode a value from hex using eth_abi_lite."""
    from eth_abi_lite.abi import default_codec

    return default_codec.decode_single(typestr, bytearray.fromhex(hexstr[2:]))


def decode_string_from_abi(hexstr: str) -> str:
    raw_bytes = bytes.fromhex(hexstr[2:])  # remove "0x"
    length = int.from_bytes(raw_bytes[32:64], byteorder="big")
    string_bytes = raw_bytes[64 : 64 + length]
    return string_bytes.decode("utf-8")
