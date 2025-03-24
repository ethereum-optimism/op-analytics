from datetime import datetime

from op_analytics.datapipeline.etl.ingestion.reader.request import BlockBatchRequest
from op_analytics.datapipeline.etl.ingestion.reader.rootpaths import RootPath
from op_analytics.coreutils.rangeutils.timerange import TimeRange


def test_query_mixed_networks():
    spec = BlockBatchRequest(
        chains=["op", "op_sepolia"],
        root_paths_to_read=[
            RootPath.of("ingestion/blocks_v1"),
            RootPath.of("ingestion/logs_v1"),
            RootPath.of("ingestion/traces_v1"),
            RootPath.of("ingestion/transactions_v1"),
        ],
        # The following arguments are not relevant for this test.
        chain_block_ranges={},
        chain_max_blocks={},
        time_range=TimeRange(
            min=datetime(2000, 1, 1),
            max=datetime(2000, 1, 2),
            requested_max_timestamp=None,
        ),
    )

    assert spec.physical_root_paths() == [
        "ingestion/blocks_v1",
        "ingestion/logs_v1",
        "ingestion/traces_v1",
        "ingestion/transactions_v1",
        "ingestion_testnets/blocks_v1",
        "ingestion_testnets/logs_v1",
        "ingestion_testnets/traces_v1",
        "ingestion_testnets/transactions_v1",
    ]


def test_query_mainnet():
    spec = BlockBatchRequest(
        chains=["op", "mode"],
        root_paths_to_read=[
            RootPath.of("ingestion/blocks_v1"),
            RootPath.of("ingestion/logs_v1"),
            RootPath.of("ingestion/traces_v1"),
            RootPath.of("ingestion/transactions_v1"),
        ],
        # The following arguments are not relevant for this test.
        chain_block_ranges={},
        chain_max_blocks={},
        time_range=TimeRange(
            min=datetime(2000, 1, 1),
            max=datetime(2000, 1, 2),
            requested_max_timestamp=None,
        ),
    )

    assert spec.physical_root_paths() == [
        "ingestion/blocks_v1",
        "ingestion/logs_v1",
        "ingestion/traces_v1",
        "ingestion/transactions_v1",
    ]


def test_query_testnet():
    spec = BlockBatchRequest(
        chains=["op_sepolia", "unichain_sepolia"],
        root_paths_to_read=[
            RootPath.of("ingestion/blocks_v1"),
            RootPath.of("ingestion/logs_v1"),
            RootPath.of("ingestion/traces_v1"),
            RootPath.of("ingestion/transactions_v1"),
        ],
        # The following arguments are not relevant for this test.
        chain_block_ranges={},
        chain_max_blocks={},
        time_range=TimeRange(
            min=datetime(2000, 1, 1),
            max=datetime(2000, 1, 2),
            requested_max_timestamp=None,
        ),
    )

    assert spec.physical_root_paths() == [
        "ingestion_testnets/blocks_v1",
        "ingestion_testnets/logs_v1",
        "ingestion_testnets/traces_v1",
        "ingestion_testnets/transactions_v1",
    ]
