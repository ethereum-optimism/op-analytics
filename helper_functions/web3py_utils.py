import datetime
from web3 import Web3

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


def getBlockByTimestamp(endpoint,timestamp):
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
        starting_block_number = getBlockByTimestamp(w3_conn, block_timestamp)

        return [starting_block_number, latest_block_number]