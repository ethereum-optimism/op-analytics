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
    "L1_ERC_721_BRIDGE_SLOT": "0x1f95cea8",
    "L1_STANDARD_BRIDGE_SLOT": "0x8c8d8ed0",
    "OPTIMISM_MINTABLE_ERC20_FACTORY_SLOT": "0x6c926575",
    "OPTIMISM_PORTAL_SLOT": "0xfd32aa0f",
    "START_BLOCK_SLOT": "0xee02016d",
    "UNSAFE_BLOCK_SIGNER_SLOT": "0x4f16540b",
    "version": "0x54fd4d50",
    "basefeeScalar": "0xbbf14fb7",
    "batchInbox": "0xdac6e63a",
    "batcherHash": "0xe812c86d",
    "blobbasefeeScalar": "0xce707517",
    "disputeGameFactory": "0xf2b4e617",
    "eip1559Denominator": "0xd220a9e0",
    "eip1559Elasticity": "0xe9f2d1f9",
    "gasLimit": "0xf86018c7",
    "l1CrossDomainMessenger": "0xa7119869",
    "l1ERC721Bridge": "0xe4e8dfa9",
    "l1StandardBridge": "0x07d829cf",
    "maximumGasLimit": "0xea141b1b",
    "minimumGasLimit": "0x4ad4321d",
    "operatorFeeConstant": "0x1663bc7f",
    "operatorFeeScalar": "0x4d5d92a4",
    "optimismMintableERC20Factory": "0x9b7d7f0a",
    "optimismPortal": "0x0a49cb03",
    "overhead": "0x0c18c162",
    "owner": "0x8da5cb5b",
    "scalar": "0xf45e65d8",
    "startBlock": "0x48cd4cb1",
    "unsafeBlockSigner": "0x1fd19ee1",
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
class SystemConfig:
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
    version: int
    basefee_scalar: int
    batch_inbox: str
    batcher_hash: str
    blob_basefee_scalar: int
    dispute_game_factory: str
    eip1559_denominator: int
    eip1559_elasticity: int
    gas_limit: int
    l1_cross_domain_messenger: str
    l1_erc721_bridge: str
    l1_standard_bridge: str
    maximum_gas_limit: int
    minimum_gas_limit: int
    operator_fee_constant: int
    operator_fee_scalar: int
    optimism_mintable_erc20_factory: str
    optimism_portal: str
    overhead: int
    owner: str
    scalar: int
    start_block: str
    unsafe_block_signer: str
    version_hex: str

    def to_dict(self):
        return asdict(self)

    @classmethod
    def of(cls, config: SystemConfig, response: list[dict]) -> Optional["SystemConfigMetadata"]:
        data: dict[str, Any] = {}
        for item in response:
            if "error" in item:
                code = item["error"].get("code")
                if code == -32016:  # rate limit
                    raise RateLimit(f"JSON-RPC error: {item} [{config}]")

                if code == -32000:  # "execution reverted"
                    log.warning(f"rpc returned -32000 'execution reverted' {config}")
                    return None

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

        try:
            return cls(
                contract_address=config.system_config_proxy,
                block_number=data["block_number"],
                block_timestamp=data["block_timestamp"],
                batch_inbox_slot=data["BATCH_INBOX_SLOT"],
                dispute_game_factory_slot=data["DISPUTE_GAME_FACTORY_SLOT"],
                l1_cross_domain_messenger_slot=data["L1_CROSS_DOMAIN_MESSENGER_SLOT"],
                l1_erc721_bridge_slot=data["L1_ERC_721_BRIDGE_SLOT"],
                l1_standard_bridge_slot=data["L1_STANDARD_BRIDGE_SLOT"],
                optimism_mintable_erc20_factory_slot=data["OPTIMISM_MINTABLE_ERC20_FACTORY_SLOT"],
                optimism_portal_slot=data["OPTIMISM_PORTAL_SLOT"],
                start_block_slot=data["START_BLOCK_SLOT"],
                unsafe_block_signer_slot=data["UNSAFE_BLOCK_SIGNER_SLOT"],
                version=decode("uint256", data["version"]),
                basefee_scalar=decode("uint32", data["basefeeScalar"]),
                batch_inbox=decode_address(data["batchInbox"]),
                batcher_hash=decode_hash(data["batcherHash"]),
                blob_basefee_scalar=decode("uint64", data["blobbasefeeScalar"]),
                dispute_game_factory=decode_address(data["disputeGameFactory"]),
                eip1559_denominator=decode("uint32", data["eip1559Denominator"]),
                eip1559_elasticity=decode("uint32", data["eip1559Elasticity"]),
                gas_limit=decode("uint64", data["gasLimit"]),
                l1_cross_domain_messenger=decode_address(data["l1CrossDomainMessenger"]),
                l1_erc721_bridge=decode_address(data["l1ERC721Bridge"]),
                l1_standard_bridge=decode_address(data["l1StandardBridge"]),
                maximum_gas_limit=decode("uint64", data["maximumGasLimit"]),
                minimum_gas_limit=decode("uint64", data["minimumGasLimit"]),
                operator_fee_constant=decode("uint64", data["operatorFeeConstant"]),
                operator_fee_scalar=decode("uint32", data["operatorFeeScalar"]),
                optimism_mintable_erc20_factory=decode_address(
                    data["optimismMintableERC20Factory"]
                ),
                optimism_portal=decode_address(data["optimismPortal"]),
                overhead=decode("uint256", data["overhead"]),
                owner=decode_address(data["owner"]),
                scalar=decode("uint256", data["scalar"]),
                start_block=decode_address(data["startBlock"]),
                unsafe_block_signer=decode_address(data["unsafeBlockSigner"]),
                version_hex=hex(decode("uint256", data["version"]))[2:],
            )
        except Exception as ex:
            raise SystemConfigError(dict(data=data, config=config)) from ex


def decode_address(hexstr: str) -> str:
    """Decode an Ethereum address from hex."""
    return "0x" + hexstr[-40:]


def decode_hash(hexstr: str) -> str:
    """Decode an Ethereum hash from hex."""
    return hexstr


def decode(typestr: str, hexstr: str):
    """Decode a value from hex using eth_abi_lite."""
    from eth_abi_lite.abi import default_codec

    return default_codec.decode_single(typestr, bytearray.fromhex(hexstr[2:]))
