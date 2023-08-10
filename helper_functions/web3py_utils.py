import datetime
from web3 import Web3
import pandas_utils as pu
import requests as r
import json


OPCODES = {
    "STOP": "00",
    "ADD": "01",
    "MUL": "02",
    "SUB": "03",
    # ... (add all opcodes here)
    "PUSH1": "60",
    "PUSH2": "61",
    "PUSH3": "62",
    "PUSH4": "63",
    # ... (continue for other PUSH opcodes and other opcodes)
    "LOG0": "a0",
    "LOG1": "a1",
    # ... (and so on)
}
def get_opcode(name):
    """Retrieve the opcode for a given mnemonic."""
    return OPCODES.get(name.upper(), '')

def get_bytecode_pattern(endpoint, function_signature, opcode):
    w3_conn = Web3(Web3.HTTPProvider(endpoint))
    """
    Generate the bytecode pattern we're looking for in contracts.

    :param function_signature: Function signature string, e.g., "test(uint256)".
    :return: Bytecode pattern string.
    """
    # Compute function signature hash
    hashed = w3_conn.keccak(text=function_signature)
    # Extract the first 4 bytes
    selector = hashed[:4].hex()[2:]
    # Combine with PUSH4 opcode
    pattern = '0x' + get_opcode(opcode) + selector
    
    return pattern

def get_duration_dict(num_periods, time_granularity):
        if time_granularity == 'hours':
                duration = datetime.timedelta(hours=num_periods)
        elif time_granularity == 'days':
                duration = datetime.timedelta(days=num_periods)
        elif time_granularity == 'seconds':
                duration = datetime.timedelta(seconds=num_periods)
        else:
                raise ValueError('Invalid time granularity')
        return duration

def getLatestBlock(endpoint):
    w3_conn = Web3(Web3.HTTPProvider(endpoint))
    latestBlock = w3_conn.eth.get_block('latest')

    return latestBlock

def getLatestBlockTimestamp(endpoint):
    latestBlock = getLatestBlock(endpoint)
    latestBlockTimestamp = latestBlock.timestamp

    return latestBlockTimestamp

def getLatestBlockNumber(endpoint):
    latestBlock = getLatestBlock(endpoint)
    latestBlockNumber = latestBlock.number

    return latestBlockNumber


def getAverageBlockTime(endpoint, trailing_num_blocks = 500):
    w3_conn = Web3(Web3.HTTPProvider(endpoint))
    currentBlock = w3_conn.eth.get_block('latest')
    thenBlock = w3_conn.eth.get_block(w3_conn.eth.block_number - trailing_num_blocks)

    return float((currentBlock.timestamp - thenBlock.timestamp) / (trailing_num_blocks*1.0))

def getBlockByTimestamp(etherscan_api, timestamp):
        timestamp_convert = pu.datetime_to_unix_timestamp(timestamp)

        url = 'https://api-optimistic.etherscan.io/api?module=block&action=getblocknobytime&timestamp={}&closest=before&apikey={}'.format(timestamp_convert, etherscan_api)

        res = r.get(url).json()['result']
        
        return res


def getBlockByTimestamp_approx(endpoint,timestamp):
    # Approximates based on avg block time, likely error prone pre-bedrock
    latestBlockTimestamp = getLatestBlockTimestamp(endpoint)
    
    average_time = latestBlockTimestamp - timestamp
    average_block = average_time / getAverageBlockTime(endpoint)

    w3_conn = Web3(Web3.HTTPProvider(endpoint))

    return min(
                int(w3_conn.eth.blockNumber - average_block),
                int(w3_conn.eth.blockNumber)
    )

def get_blockrange_by_timedelta(endpoint,num_periods, time_granularity):
        w3_conn = Web3(Web3.HTTPProvider(endpoint))
        duration = get_duration_dict(num_periods, time_granularity)

        # Get the latest block number
        latest_block_number = w3_conn.eth.get_block('latest')['number']
        # Get the block number X hours ago

        time_st = datetime.datetime.now() - duration
        block_timestamp = int(time_st.timestamp())
        print(block_timestamp)
        starting_block_number = getBlockByTimestamp_approx(w3_conn, block_timestamp)

        return [starting_block_number, latest_block_number]

def get_block_receipt(endpoint,block_number):
       # Connect to the Ethereum mainnet
        w3 = Web3(Web3.HTTPProvider(endpoint))

        # Retrieve the block information
        block = w3.eth.getBlock(block_number)

        # # Get the block receipt
        # receipt = w3.eth.getTransactionReceipt(block['hash'])

        print(block)

def alchemy_get_block_by_number(endpoint, block_number):
        url = endpoint

        payload = {
                "id": block_number,
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": ["finalized", False]
        }
        headers = {
                "accept": "application/json",
                "content-type": "application/json"
        }
       
        response = r.post(url, json=payload, headers=headers).json()

        return response['result']['number']
       

def get_eth_balance_by_block(endpoint, address, block_number):
       w3_conn = Web3(Web3.HTTPProvider(endpoint))
       bal = w3_conn.eth.get_balance(address, block_identifier=block_number)
       bal_eth = bal/1e18
       return bal_eth

def get_implementation_contract(w3_conn, proxy_address, proxy_abi):

        proxy_contract = w3_conn.eth.contract(address=proxy_address, abi=proxy_abi)
        implementation_address = proxy_contract.functions.implementation().call()

        return implementation_address
