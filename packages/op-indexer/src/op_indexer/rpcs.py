import time

import urllib3

import structlog

log = structlog.get_logger()


# Hard-coded for now. Will be configuration based to support more chains.
ENDPOINTS = {
    "op": "https://mainnet.optimism.io/",
    "base": "https://mainnet.base.org",
}


def get_block(chain: str, block_number: str):
    """Proxies to  eth_getBlockByNumber to retrieve a single block."""
    start = time.time()

    resp = urllib3.request(
        method="POST",
        url=ENDPOINTS[chain],
        json={
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [block_number, False],
            "id": 1,
        },
        headers={"Content-Type": "application/json"},
    )
    log.info(f"get_block {time.time() - start:.2f} seconds")
    return resp.json()


def get_transactions(chain: str, tx_hashes: list[str]):
    """Proxies to  eth_getTransactionReceipt to get all receipts for a list of transactions."""
    start = time.time()

    resp = urllib3.request(
        method="POST",
        url=ENDPOINTS[chain],
        json={
            "jsonrpc": "2.0",
            "method": "eth_getTransactionReceipt",
            "params": tx_hashes,
            "id": 1,
        },
        headers={"Content-Type": "application/json"},
    )
    log.info(f"get_block {time.time() - start:.2f} seconds")
    return resp.json()
