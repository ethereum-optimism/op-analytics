import datetime

import pyarrow as pa

from op_analytics.coreutils.duckdb_local.client import run_query_duckdb_local
from op_analytics.coreutils.partitioned.dataaccess import init_data_access
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.marker import Marker
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.partitioned.partition import (
    PartitionColumn,
    Partition,
    PartitionMetadata,
)
from op_analytics.datapipeline.etl.ingestion.reader.markers import IngestionDataSpec
from op_analytics.coreutils.time import now

MARKERS_TABLE = "raw_onchain_ingestion_markers"


def test_marker():
    client = init_data_access()

    run_query_duckdb_local(
        f"DELETE FROM etl_monitor_dev.{MARKERS_TABLE} WHERE chain = 'DUMMYCHAIN'"
    )

    marker = Marker(
        written_parts={
            Partition(
                [
                    PartitionColumn(name="chain", value="DUMMYCHAIN"),
                    PartitionColumn(name="dt", value="2024-10-25"),
                ]
            ): PartitionMetadata(row_count=5045),
            Partition(
                [
                    PartitionColumn(name="chain", value="DUMMYCHAIN"),
                    PartitionColumn(name="dt", value="2024-10-26"),
                ]
            ): PartitionMetadata(row_count=14955),
        },
        expected_output=ExpectedOutput(
            marker_path="ingestion/blocks_v1/DUMMYCHAIN/000011540000",
            root_path="ingestion/blocks_v1",
            file_name="000011540000.parquet",
        ),
    )

    initially_exists = client.marker_exists(
        data_location=DataLocation.LOCAL,
        marker_path=marker.marker_path,
        markers_table=MARKERS_TABLE,
    )
    assert not initially_exists

    marker_df = marker.to_pyarrow_table(
        process_name="default",
        extra_marker_columns={"num_blocks": 20000, "min_block": 11540000, "max_block": 11560000},
        extra_marker_columns_schema=[
            pa.field("chain", pa.string()),
            pa.field("dt", pa.date32()),
            pa.field("num_blocks", pa.int32()),
            pa.field("min_block", pa.int64()),
            pa.field("max_block", pa.int64()),
        ],
    )

    client.write_marker(
        marker_df=marker_df,
        data_location=DataLocation.LOCAL,
        markers_table=MARKERS_TABLE,
    )

    result = (
        run_query_duckdb_local(
            "SELECT * FROM etl_monitor_dev.raw_onchain_ingestion_markers WHERE chain = 'DUMMYCHAIN'"
        )
        .pl()
        .to_dicts()
    )

    current_time = now()
    for row in result:
        updated = row.pop("updated_at")
        assert current_time - updated < datetime.timedelta(seconds=5)

        writer_name = row.pop("writer_name")
        assert writer_name is not None

    assert result == [
        {
            "marker_path": "ingestion/blocks_v1/DUMMYCHAIN/000011540000",
            "dataset_name": "",
            "root_path": "ingestion/blocks_v1",
            "num_parts": 2,
            "data_path": "ingestion/blocks_v1/chain=DUMMYCHAIN/dt=2024-10-25/000011540000.parquet",
            "row_count": 5045,
            "process_name": "default",
            "chain": "DUMMYCHAIN",
            "dt": datetime.date(2024, 10, 25),
            "num_blocks": 20000,
            "min_block": 11540000,
            "max_block": 11560000,
        },
        {
            "marker_path": "ingestion/blocks_v1/DUMMYCHAIN/000011540000",
            "dataset_name": "",
            "root_path": "ingestion/blocks_v1",
            "num_parts": 2,
            "data_path": "ingestion/blocks_v1/chain=DUMMYCHAIN/dt=2024-10-26/000011540000.parquet",
            "row_count": 14955,
            "process_name": "default",
            "chain": "DUMMYCHAIN",
            "dt": datetime.date(2024, 10, 26),
            "num_blocks": 20000,
            "min_block": 11540000,
            "max_block": 11560000,
        },
    ]

    exists = client.marker_exists(
        data_location=DataLocation.LOCAL,
        marker_path=marker.marker_path,
        markers_table=MARKERS_TABLE,
    )
    assert exists

    data_spec = IngestionDataSpec(
        chains=["DUMMYCHAIN"],
        root_paths_to_read=["ingestion/blocks_v1"],
    )
    markers_df = data_spec.query_markers(
        datevals=[datetime.date(2024, 10, 25)],
        location=DataLocation.LOCAL,
    )

    assert len(markers_df) == 1

    assert (
        markers_df.limit(1)["data_path"].item()
        == "ingestion/blocks_v1/chain=DUMMYCHAIN/dt=2024-10-25/000011540000.parquet"
    )

    data_spec = IngestionDataSpec(
        chains=["DUMMYCHAIN"],
        root_paths_to_read=["ingestion/transactions_v1"],
    )
    markers_df = data_spec.query_markers(
        datevals=[datetime.date(2024, 10, 25)],
        location=DataLocation.LOCAL,
    )
    assert len(markers_df) == 0
