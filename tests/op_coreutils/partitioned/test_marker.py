import datetime
import pyarrow as pa

from op_coreutils.partitioned.dataaccess import init_data_access
from op_coreutils.partitioned.location import DataLocation
from op_coreutils.partitioned.marker import Marker
from op_coreutils.partitioned.output import ExpectedOutput, KeyValue, OutputPartMeta
from op_coreutils.partitioned.types import SinkMarkerPath, SinkOutputRootPath
from op_coreutils.duckdb_local import run_query
from op_coreutils.time import now

MARKERS_TABLE = "raw_onchain_ingestion_markers"


def test_marker():
    run_query(f"DELETE FROM etl_monitor_dev.{MARKERS_TABLE} WHERE chain = 'DUMMYCHAIN'")

    marker = Marker(
        written_parts=[
            OutputPartMeta(
                partitions=[
                    KeyValue(key="chain", value="DUMMYCHAIN"),
                    KeyValue(key="dt", value="2024-10-25"),
                ],
                row_count=5045,
            ),
            OutputPartMeta(
                partitions=[
                    KeyValue(key="chain", value="DUMMYCHAIN"),
                    KeyValue(key="dt", value="2024-10-26"),
                ],
                row_count=14955,
            ),
        ],
        expected_output=ExpectedOutput(
            marker_path=SinkMarkerPath(
                "markers/ingestion/blocks_v1/chain=DUMMYCHAIN/000011540000.json"
            ),
            dataset_name="blocks",
            root_path=SinkOutputRootPath("ingestion/blocks_v1"),
            file_name="000011540000.parquet",
            process_name="default",
            additional_columns={"num_blocks": 20000, "min_block": 11540000, "max_block": 11560000},
            additional_columns_schema=[
                pa.field("chain", pa.string()),
                pa.field("dt", pa.date32()),
                pa.field("num_blocks", pa.int32()),
                pa.field("min_block", pa.int64()),
                pa.field("max_block", pa.int64()),
            ],
        ),
    )

    # OK to use the real data access client since we are using DUMMYCHAIN values.
    client = init_data_access()

    initially_exists = client.marker_exists(
        data_location=DataLocation.LOCAL,
        marker_path=marker.marker_path,
        markers_table=MARKERS_TABLE,
    )
    assert not initially_exists

    client.write_marker(
        data_location=DataLocation.LOCAL,
        expected_output=marker.expected_output,
        written_parts=marker.written_parts,
        markers_table=MARKERS_TABLE,
    )

    result = (
        run_query(
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
            "marker_path": "markers/ingestion/blocks_v1/chain=DUMMYCHAIN/000011540000.json",
            "dataset_name": "blocks",
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
            "marker_path": "markers/ingestion/blocks_v1/chain=DUMMYCHAIN/000011540000.json",
            "dataset_name": "blocks",
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

    markers_df = client.markers_for_raw_ingestion(
        data_location=DataLocation.LOCAL,
        markers_table=MARKERS_TABLE,
        datevals=[datetime.date(2024, 10, 25)],
        chains=["DUMMYCHAIN"],
        dataset_names=["blocks"],
    )
    assert len(markers_df) == 1

    assert (
        markers_df.limit(1)["data_path"].item()
        == "ingestion/blocks_v1/chain=DUMMYCHAIN/dt=2024-10-25/000011540000.parquet"
    )

    markers_df = client.markers_for_raw_ingestion(
        data_location=DataLocation.LOCAL,
        markers_table=MARKERS_TABLE,
        datevals=[datetime.date(2024, 10, 25)],
        chains=["DUMMYCHAIN"],
        dataset_names=["transactions"],
    )
    assert len(markers_df) == 0
