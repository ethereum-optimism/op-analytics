import datetime
from web3 import Web3
import pandas_utils as pu
import requests as r
import json

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
       

def alchemy_get_eth_balance_by_block(endpoint, block_number_hash, address):
        # # Connect to an Ethereum node (or Optimism)
        # w3_conn = Web3(Web3.HTTPProvider(endpoint))

        # # Get the ETH balance at a specific block
        # balance = w3_conn.eth.getBalance(address, block_identifier=block_number)

        # # Convert balance from Wei to Ether
        # balance_eth = w3_conn.fromWei(balance, 'ether')

        # return balance_eth

        url = endpoint

        payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "params": [address, block_number_hash],
        "method": "eth_getBalance"
        }
        headers = {
        "accept": "application/json",
        "content-type": "application/json"
        }

        response = r.post(url, json=payload, headers=headers)

        balance = json.loads(response.text)['result']

        balance_wei = int(balance, 16)
        balance_eth = balance_wei / 1e18

        return balance_eth