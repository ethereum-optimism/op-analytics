import datetime
from unittest.mock import patch

import polars as pl

from op_analytics.coreutils.duckdb_local.client import init_client as duckb_local_client
from op_analytics.coreutils.partitioned.dataaccess import (
    DateFilter,
    init_data_access,
)
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.datapipeline.etl.ingestion.reader.ranges import BlockRange, ChainMaxBlock
from op_analytics.datapipeline.etl.loadbq.main import load_superchain_raw_to_bq

MOCK_MARKERS = [
    {
        "dt": datetime.date(2024, 9, 30),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10420000,
        "max_block": 10440000,
        "data_path": "ingestion/blocks_v1/chain=fraxtal/dt=2024-09-30/000010420000.parquet",
        "root_path": "ingestion/blocks_v1",
    },
    {
        "dt": datetime.date(2024, 9, 30),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10440000,
        "max_block": 10460000,
        "data_path": "ingestion/blocks_v1/chain=fraxtal/dt=2024-09-30/000010440000.parquet",
        "root_path": "ingestion/blocks_v1",
    },
    {
        "dt": datetime.date(2024, 9, 30),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10460000,
        "max_block": 10480000,
        "data_path": "ingestion/blocks_v1/chain=fraxtal/dt=2024-09-30/000010460000.parquet",
        "root_path": "ingestion/blocks_v1",
    },
    {
        "dt": datetime.date(2024, 10, 1),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10460000,
        "max_block": 10480000,
        "data_path": "ingestion/blocks_v1/chain=fraxtal/dt=2024-10-01/000010460000.parquet",
        "root_path": "ingestion/blocks_v1",
    },
    {
        "dt": datetime.date(2024, 10, 1),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10480000,
        "max_block": 10500000,
        "data_path": "ingestion/blocks_v1/chain=fraxtal/dt=2024-10-01/000010480000.parquet",
        "root_path": "ingestion/blocks_v1",
    },
    {
        "dt": datetime.date(2024, 10, 2),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10500000,
        "max_block": 10520000,
        "data_path": "ingestion/blocks_v1/chain=fraxtal/dt=2024-10-02/000010500000.parquet",
        "root_path": "ingestion/blocks_v1",
    },
    {
        "dt": datetime.date(2024, 10, 1),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10500000,
        "max_block": 10520000,
        "data_path": "ingestion/blocks_v1/chain=fraxtal/dt=2024-10-01/000010500000.parquet",
        "root_path": "ingestion/blocks_v1",
    },
    {
        "dt": datetime.date(2024, 10, 2),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10520000,
        "max_block": 10540000,
        "data_path": "ingestion/blocks_v1/chain=fraxtal/dt=2024-10-02/000010520000.parquet",
        "root_path": "ingestion/blocks_v1",
    },
    {
        "dt": datetime.date(2024, 10, 2),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10540000,
        "max_block": 10560000,
        "data_path": "ingestion/blocks_v1/chain=fraxtal/dt=2024-10-02/000010540000.parquet",
        "root_path": "ingestion/blocks_v1",
    },
    {
        "dt": datetime.date(2024, 10, 3),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10540000,
        "max_block": 10560000,
        "data_path": "ingestion/blocks_v1/chain=fraxtal/dt=2024-10-03/000010540000.parquet",
        "root_path": "ingestion/blocks_v1",
    },
    {
        "dt": datetime.date(2024, 10, 3),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10560000,
        "max_block": 10580000,
        "data_path": "ingestion/blocks_v1/chain=fraxtal/dt=2024-10-03/000010560000.parquet",
        "root_path": "ingestion/blocks_v1",
    },
    {
        "dt": datetime.date(2024, 10, 4),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10580000,
        "max_block": 10600000,
        "data_path": "ingestion/blocks_v1/chain=fraxtal/dt=2024-10-04/000010580000.parquet",
        "root_path": "ingestion/blocks_v1",
    },
    {
        "dt": datetime.date(2024, 10, 3),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10580000,
        "max_block": 10600000,
        "data_path": "ingestion/blocks_v1/chain=fraxtal/dt=2024-10-03/000010580000.parquet",
        "root_path": "ingestion/blocks_v1",
    },
    {
        "dt": datetime.date(2024, 10, 4),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10600000,
        "max_block": 10620000,
        "data_path": "ingestion/blocks_v1/chain=fraxtal/dt=2024-10-04/000010600000.parquet",
        "root_path": "ingestion/blocks_v1",
    },
    {
        "dt": datetime.date(2024, 10, 4),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10620000,
        "max_block": 10640000,
        "data_path": "ingestion/blocks_v1/chain=fraxtal/dt=2024-10-04/000010620000.parquet",
        "root_path": "ingestion/blocks_v1",
    },
    {
        "dt": datetime.date(2024, 9, 30),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10420000,
        "max_block": 10440000,
        "data_path": "ingestion/logs_v1/chain=fraxtal/dt=2024-09-30/000010420000.parquet",
        "root_path": "ingestion/logs_v1",
    },
    {
        "dt": datetime.date(2024, 9, 30),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10440000,
        "max_block": 10460000,
        "data_path": "ingestion/logs_v1/chain=fraxtal/dt=2024-09-30/000010440000.parquet",
        "root_path": "ingestion/logs_v1",
    },
    {
        "dt": datetime.date(2024, 10, 1),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10460000,
        "max_block": 10480000,
        "data_path": "ingestion/logs_v1/chain=fraxtal/dt=2024-10-01/000010460000.parquet",
        "root_path": "ingestion/logs_v1",
    },
    {
        "dt": datetime.date(2024, 9, 30),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10460000,
        "max_block": 10480000,
        "data_path": "ingestion/logs_v1/chain=fraxtal/dt=2024-09-30/000010460000.parquet",
        "root_path": "ingestion/logs_v1",
    },
    {
        "dt": datetime.date(2024, 10, 1),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10480000,
        "max_block": 10500000,
        "data_path": "ingestion/logs_v1/chain=fraxtal/dt=2024-10-01/000010480000.parquet",
        "root_path": "ingestion/logs_v1",
    },
    {
        "dt": datetime.date(2024, 10, 1),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10500000,
        "max_block": 10520000,
        "data_path": "ingestion/logs_v1/chain=fraxtal/dt=2024-10-01/000010500000.parquet",
        "root_path": "ingestion/logs_v1",
    },
    {
        "dt": datetime.date(2024, 10, 2),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10500000,
        "max_block": 10520000,
        "data_path": "ingestion/logs_v1/chain=fraxtal/dt=2024-10-02/000010500000.parquet",
        "root_path": "ingestion/logs_v1",
    },
    {
        "dt": datetime.date(2024, 10, 2),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10520000,
        "max_block": 10540000,
        "data_path": "ingestion/logs_v1/chain=fraxtal/dt=2024-10-02/000010520000.parquet",
        "root_path": "ingestion/logs_v1",
    },
    {
        "dt": datetime.date(2024, 10, 3),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10540000,
        "max_block": 10560000,
        "data_path": "ingestion/logs_v1/chain=fraxtal/dt=2024-10-03/000010540000.parquet",
        "root_path": "ingestion/logs_v1",
    },
    {
        "dt": datetime.date(2024, 10, 2),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10540000,
        "max_block": 10560000,
        "data_path": "ingestion/logs_v1/chain=fraxtal/dt=2024-10-02/000010540000.parquet",
        "root_path": "ingestion/logs_v1",
    },
    {
        "dt": datetime.date(2024, 10, 3),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10560000,
        "max_block": 10580000,
        "data_path": "ingestion/logs_v1/chain=fraxtal/dt=2024-10-03/000010560000.parquet",
        "root_path": "ingestion/logs_v1",
    },
    {
        "dt": datetime.date(2024, 10, 3),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10580000,
        "max_block": 10600000,
        "data_path": "ingestion/logs_v1/chain=fraxtal/dt=2024-10-03/000010580000.parquet",
        "root_path": "ingestion/logs_v1",
    },
    {
        "dt": datetime.date(2024, 10, 4),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10580000,
        "max_block": 10600000,
        "data_path": "ingestion/logs_v1/chain=fraxtal/dt=2024-10-04/000010580000.parquet",
        "root_path": "ingestion/logs_v1",
    },
    {
        "dt": datetime.date(2024, 10, 4),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10600000,
        "max_block": 10620000,
        "data_path": "ingestion/logs_v1/chain=fraxtal/dt=2024-10-04/000010600000.parquet",
        "root_path": "ingestion/logs_v1",
    },
    {
        "dt": datetime.date(2024, 10, 4),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10620000,
        "max_block": 10640000,
        "data_path": "ingestion/logs_v1/chain=fraxtal/dt=2024-10-04/000010620000.parquet",
        "root_path": "ingestion/logs_v1",
    },
    {
        "dt": datetime.date(2024, 9, 30),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10420000,
        "max_block": 10440000,
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-09-30/000010420000.parquet",
        "root_path": "ingestion/traces_v1",
    },
    {
        "dt": datetime.date(2024, 9, 30),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10440000,
        "max_block": 10460000,
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-09-30/000010440000.parquet",
        "root_path": "ingestion/traces_v1",
    },
    {
        "dt": datetime.date(2024, 10, 1),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10460000,
        "max_block": 10480000,
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-01/000010460000.parquet",
        "root_path": "ingestion/traces_v1",
    },
    {
        "dt": datetime.date(2024, 9, 30),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10460000,
        "max_block": 10480000,
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-09-30/000010460000.parquet",
        "root_path": "ingestion/traces_v1",
    },
    {
        "dt": datetime.date(2024, 10, 1),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10480000,
        "max_block": 10500000,
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-01/000010480000.parquet",
        "root_path": "ingestion/traces_v1",
    },
    {
        "dt": datetime.date(2024, 10, 2),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10500000,
        "max_block": 10520000,
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-02/000010500000.parquet",
        "root_path": "ingestion/traces_v1",
    },
    {
        "dt": datetime.date(2024, 10, 1),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10500000,
        "max_block": 10520000,
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-01/000010500000.parquet",
        "root_path": "ingestion/traces_v1",
    },
    {
        "dt": datetime.date(2024, 10, 2),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10520000,
        "max_block": 10540000,
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-02/000010520000.parquet",
        "root_path": "ingestion/traces_v1",
    },
    {
        "dt": datetime.date(2024, 10, 3),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10540000,
        "max_block": 10560000,
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-03/000010540000.parquet",
        "root_path": "ingestion/traces_v1",
    },
    {
        "dt": datetime.date(2024, 10, 2),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10540000,
        "max_block": 10560000,
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-02/000010540000.parquet",
        "root_path": "ingestion/traces_v1",
    },
    {
        "dt": datetime.date(2024, 10, 3),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10560000,
        "max_block": 10580000,
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-03/000010560000.parquet",
        "root_path": "ingestion/traces_v1",
    },
    {
        "dt": datetime.date(2024, 10, 3),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10580000,
        "max_block": 10600000,
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-03/000010580000.parquet",
        "root_path": "ingestion/traces_v1",
    },
    {
        "dt": datetime.date(2024, 10, 4),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10580000,
        "max_block": 10600000,
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-04/000010580000.parquet",
        "root_path": "ingestion/traces_v1",
    },
    {
        "dt": datetime.date(2024, 10, 4),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10600000,
        "max_block": 10620000,
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-04/000010600000.parquet",
        "root_path": "ingestion/traces_v1",
    },
    {
        "dt": datetime.date(2024, 10, 4),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10620000,
        "max_block": 10640000,
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-04/000010620000.parquet",
        "root_path": "ingestion/traces_v1",
    },
    {
        "dt": datetime.date(2024, 9, 30),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10420000,
        "max_block": 10440000,
        "data_path": "ingestion/transactions_v1/chain=fraxtal/dt=2024-09-30/000010420000.parquet",
        "root_path": "ingestion/transactions_v1",
    },
    {
        "dt": datetime.date(2024, 9, 30),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10440000,
        "max_block": 10460000,
        "data_path": "ingestion/transactions_v1/chain=fraxtal/dt=2024-09-30/000010440000.parquet",
        "root_path": "ingestion/transactions_v1",
    },
    {
        "dt": datetime.date(2024, 10, 1),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10460000,
        "max_block": 10480000,
        "data_path": "ingestion/transactions_v1/chain=fraxtal/dt=2024-10-01/000010460000.parquet",
        "root_path": "ingestion/transactions_v1",
    },
    {
        "dt": datetime.date(2024, 9, 30),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10460000,
        "max_block": 10480000,
        "data_path": "ingestion/transactions_v1/chain=fraxtal/dt=2024-09-30/000010460000.parquet",
        "root_path": "ingestion/transactions_v1",
    },
    {
        "dt": datetime.date(2024, 10, 1),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10480000,
        "max_block": 10500000,
        "data_path": "ingestion/transactions_v1/chain=fraxtal/dt=2024-10-01/000010480000.parquet",
        "root_path": "ingestion/transactions_v1",
    },
    {
        "dt": datetime.date(2024, 10, 1),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10500000,
        "max_block": 10520000,
        "data_path": "ingestion/transactions_v1/chain=fraxtal/dt=2024-10-01/000010500000.parquet",
        "root_path": "ingestion/transactions_v1",
    },
    {
        "dt": datetime.date(2024, 10, 2),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10500000,
        "max_block": 10520000,
        "data_path": "ingestion/transactions_v1/chain=fraxtal/dt=2024-10-02/000010500000.parquet",
        "root_path": "ingestion/transactions_v1",
    },
    {
        "dt": datetime.date(2024, 10, 2),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10520000,
        "max_block": 10540000,
        "data_path": "ingestion/transactions_v1/chain=fraxtal/dt=2024-10-02/000010520000.parquet",
        "root_path": "ingestion/transactions_v1",
    },
    {
        "dt": datetime.date(2024, 10, 3),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10540000,
        "max_block": 10560000,
        "data_path": "ingestion/transactions_v1/chain=fraxtal/dt=2024-10-03/000010540000.parquet",
        "root_path": "ingestion/transactions_v1",
    },
    {
        "dt": datetime.date(2024, 10, 2),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10540000,
        "max_block": 10560000,
        "data_path": "ingestion/transactions_v1/chain=fraxtal/dt=2024-10-02/000010540000.parquet",
        "root_path": "ingestion/transactions_v1",
    },
    {
        "dt": datetime.date(2024, 10, 3),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10560000,
        "max_block": 10580000,
        "data_path": "ingestion/transactions_v1/chain=fraxtal/dt=2024-10-03/000010560000.parquet",
        "root_path": "ingestion/transactions_v1",
    },
    {
        "dt": datetime.date(2024, 10, 3),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10580000,
        "max_block": 10600000,
        "data_path": "ingestion/transactions_v1/chain=fraxtal/dt=2024-10-03/000010580000.parquet",
        "root_path": "ingestion/transactions_v1",
    },
    {
        "dt": datetime.date(2024, 10, 4),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10580000,
        "max_block": 10600000,
        "data_path": "ingestion/transactions_v1/chain=fraxtal/dt=2024-10-04/000010580000.parquet",
        "root_path": "ingestion/transactions_v1",
    },
    {
        "dt": datetime.date(2024, 10, 4),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10600000,
        "max_block": 10620000,
        "data_path": "ingestion/transactions_v1/chain=fraxtal/dt=2024-10-04/000010600000.parquet",
        "root_path": "ingestion/transactions_v1",
    },
    {
        "dt": datetime.date(2024, 10, 4),
        "chain": "fraxtal",
        "num_blocks": 20000,
        "min_block": 10620000,
        "max_block": 10640000,
        "data_path": "ingestion/transactions_v1/chain=fraxtal/dt=2024-10-04/000010620000.parquet",
        "root_path": "ingestion/transactions_v1",
    },
]


def mock_block_range(chain: str, min_ts: int, max_ts: int):
    if chain == "fraxtal":
        return BlockRange(min=10465045, max=10594644)

    raise NotImplementedError()


def mock_max_block(chain: str):
    if chain == "fraxtal":
        return ChainMaxBlock(chain="fraxtal", ts=1736476259, number=14832774)

    raise NotADirectoryError()


def test_load_tasks():
    client = init_data_access()

    duckdb_client = duckb_local_client()
    duckdb_client.sql("TRUNCATE TABLE etl_monitor_dev.superchain_raw_bigquery_markers")

    with (
        patch(
            "op_analytics.datapipeline.etl.ingestion.reader.ranges.block_range_for_dates",
            new=mock_block_range,
        ),
        patch(
            "op_analytics.datapipeline.etl.ingestion.reader.ranges.chain_max_block",
            new=mock_max_block,
        ),
        patch("op_analytics.datapipeline.etl.loadbq.load.goldsky_mainnet_chains") as m1,
        patch.object(client, "query_markers_with_filters") as m2,
        patch("op_analytics.datapipeline.etl.loadbq.loader.load_from_parquet_uris") as m3,
    ):
        m1.return_value = ["fraxtal"]
        m2.return_value = pl.DataFrame(
            MOCK_MARKERS,
            {
                "dt": pl.Date,
                "chain": pl.String,
                "marker_path": pl.String,
                "num_parts": pl.UInt32,
                "num_blocks": pl.Int32,
                "min_block": pl.Int64,
                "max_block": pl.Int64,
                "root_path": pl.String,
                "data_path": pl.String,
            },
        )

        load_superchain_raw_to_bq(
            range_spec="@20241001:+3",
            dryrun=False,
            force_complete=True,
            force_not_ready=False,
        )

        load_calls = []
        for _ in m3.call_args_list:
            load_calls.append(
                dict(
                    dateval=_.kwargs["date_partition"],
                    uris=_.kwargs["source_uris"],
                    uris_root_path=_.kwargs["source_uri_prefix"],
                )
            )

        load_calls.sort(key=lambda x: (x["dateval"], x["uris_root_path"]))

        assert load_calls == [
            {
                "dateval": datetime.date(2024, 10, 1),
                "uris": [
                    "gs://oplabs-tools-data-sink/ingestion/blocks_v1/chain=fraxtal/dt=2024-10-01/000010460000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/blocks_v1/chain=fraxtal/dt=2024-10-01/000010480000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/blocks_v1/chain=fraxtal/dt=2024-10-01/000010500000.parquet",
                ],
                "uris_root_path": "gs://oplabs-tools-data-sink/ingestion/blocks_v1/{chain:STRING}/{dt:DATE}",
            },
            {
                "dateval": datetime.date(2024, 10, 1),
                "uris": [
                    "gs://oplabs-tools-data-sink/ingestion/logs_v1/chain=fraxtal/dt=2024-10-01/000010460000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/logs_v1/chain=fraxtal/dt=2024-10-01/000010480000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/logs_v1/chain=fraxtal/dt=2024-10-01/000010500000.parquet",
                ],
                "uris_root_path": "gs://oplabs-tools-data-sink/ingestion/logs_v1/{chain:STRING}/{dt:DATE}",
            },
            {
                "dateval": datetime.date(2024, 10, 1),
                "uris": [
                    "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=fraxtal/dt=2024-10-01/000010460000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=fraxtal/dt=2024-10-01/000010480000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=fraxtal/dt=2024-10-01/000010500000.parquet",
                ],
                "uris_root_path": "gs://oplabs-tools-data-sink/ingestion/traces_v1/{chain:STRING}/{dt:DATE}",
            },
            {
                "dateval": datetime.date(2024, 10, 1),
                "uris": [
                    "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=fraxtal/dt=2024-10-01/000010460000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=fraxtal/dt=2024-10-01/000010480000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=fraxtal/dt=2024-10-01/000010500000.parquet",
                ],
                "uris_root_path": "gs://oplabs-tools-data-sink/ingestion/transactions_v1/{chain:STRING}/{dt:DATE}",
            },
            {
                "dateval": datetime.date(2024, 10, 2),
                "uris": [
                    "gs://oplabs-tools-data-sink/ingestion/blocks_v1/chain=fraxtal/dt=2024-10-02/000010500000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/blocks_v1/chain=fraxtal/dt=2024-10-02/000010520000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/blocks_v1/chain=fraxtal/dt=2024-10-02/000010540000.parquet",
                ],
                "uris_root_path": "gs://oplabs-tools-data-sink/ingestion/blocks_v1/{chain:STRING}/{dt:DATE}",
            },
            {
                "dateval": datetime.date(2024, 10, 2),
                "uris": [
                    "gs://oplabs-tools-data-sink/ingestion/logs_v1/chain=fraxtal/dt=2024-10-02/000010500000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/logs_v1/chain=fraxtal/dt=2024-10-02/000010520000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/logs_v1/chain=fraxtal/dt=2024-10-02/000010540000.parquet",
                ],
                "uris_root_path": "gs://oplabs-tools-data-sink/ingestion/logs_v1/{chain:STRING}/{dt:DATE}",
            },
            {
                "dateval": datetime.date(2024, 10, 2),
                "uris": [
                    "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=fraxtal/dt=2024-10-02/000010500000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=fraxtal/dt=2024-10-02/000010520000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=fraxtal/dt=2024-10-02/000010540000.parquet",
                ],
                "uris_root_path": "gs://oplabs-tools-data-sink/ingestion/traces_v1/{chain:STRING}/{dt:DATE}",
            },
            {
                "dateval": datetime.date(2024, 10, 2),
                "uris": [
                    "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=fraxtal/dt=2024-10-02/000010500000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=fraxtal/dt=2024-10-02/000010520000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=fraxtal/dt=2024-10-02/000010540000.parquet",
                ],
                "uris_root_path": "gs://oplabs-tools-data-sink/ingestion/transactions_v1/{chain:STRING}/{dt:DATE}",
            },
            {
                "dateval": datetime.date(2024, 10, 3),
                "uris": [
                    "gs://oplabs-tools-data-sink/ingestion/blocks_v1/chain=fraxtal/dt=2024-10-03/000010540000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/blocks_v1/chain=fraxtal/dt=2024-10-03/000010560000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/blocks_v1/chain=fraxtal/dt=2024-10-03/000010580000.parquet",
                ],
                "uris_root_path": "gs://oplabs-tools-data-sink/ingestion/blocks_v1/{chain:STRING}/{dt:DATE}",
            },
            {
                "dateval": datetime.date(2024, 10, 3),
                "uris": [
                    "gs://oplabs-tools-data-sink/ingestion/logs_v1/chain=fraxtal/dt=2024-10-03/000010540000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/logs_v1/chain=fraxtal/dt=2024-10-03/000010560000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/logs_v1/chain=fraxtal/dt=2024-10-03/000010580000.parquet",
                ],
                "uris_root_path": "gs://oplabs-tools-data-sink/ingestion/logs_v1/{chain:STRING}/{dt:DATE}",
            },
            {
                "dateval": datetime.date(2024, 10, 3),
                "uris": [
                    "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=fraxtal/dt=2024-10-03/000010540000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=fraxtal/dt=2024-10-03/000010560000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=fraxtal/dt=2024-10-03/000010580000.parquet",
                ],
                "uris_root_path": "gs://oplabs-tools-data-sink/ingestion/traces_v1/{chain:STRING}/{dt:DATE}",
            },
            {
                "dateval": datetime.date(2024, 10, 3),
                "uris": [
                    "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=fraxtal/dt=2024-10-03/000010540000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=fraxtal/dt=2024-10-03/000010560000.parquet",
                    "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=fraxtal/dt=2024-10-03/000010580000.parquet",
                ],
                "uris_root_path": "gs://oplabs-tools-data-sink/ingestion/transactions_v1/{chain:STRING}/{dt:DATE}",
            },
        ]

    markers = client.query_markers_with_filters(
        data_location=DataLocation.BIGQUERY_LOCAL_MARKERS,
        markers_table="superchain_raw_bigquery_markers",
        datefilter=DateFilter(
            min_date=None,
            max_date=None,
            datevals=[datetime.date(2024, 10, 2)],
        ),
        projections=["dt", "marker_path", "data_path", "row_count", "num_parts"],
        filters={},
    ).to_dicts()

    markers.sort(key=lambda x: x["marker_path"])

    assert markers == [
        {
            "dt": datetime.date(2024, 10, 2),
            "marker_path": "superchain_raw/blocks/2024-10-02",
            "data_path": "superchain_raw/blocks/dt=2024-10-02/",
            "num_parts": 1,
            "row_count": 3,
        },
        {
            "dt": datetime.date(2024, 10, 2),
            "marker_path": "superchain_raw/logs/2024-10-02",
            "data_path": "superchain_raw/logs/dt=2024-10-02/",
            "num_parts": 1,
            "row_count": 3,
        },
        {
            "dt": datetime.date(2024, 10, 2),
            "marker_path": "superchain_raw/traces/2024-10-02",
            "data_path": "superchain_raw/traces/dt=2024-10-02/",
            "num_parts": 1,
            "row_count": 3,
        },
        {
            "dt": datetime.date(2024, 10, 2),
            "marker_path": "superchain_raw/transactions/2024-10-02",
            "data_path": "superchain_raw/transactions/dt=2024-10-02/",
            "num_parts": 1,
            "row_count": 3,
        },
    ]
