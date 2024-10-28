import datetime
import pyarrow as pa

from op_coreutils.partitioned.location import DataLocation
from op_coreutils.partitioned.marker import Marker, marker_exists, markers_for_dates
from op_coreutils.partitioned.output import WrittenParquetPath, KeyValue
from op_coreutils.partitioned.writer import write_marker
from op_coreutils.duckdb_local import run_query
from op_coreutils.time import now


def test_marker():
    run_query("DELETE FROM etl_monitor.raw_onchain_ingestion_markers WHERE chain = 'DUMMYCHAIN'")

    marker = Marker(
        marker_path="markers/ingestion/blocks_v1/chain=DUMMYCHAIN/000011540000.json",
        dataset_name="blocks",
        root_path="ingestion/blocks_v1",
        data_paths=[
            WrittenParquetPath(
                root="ingestion/blocks_v1",
                basename="000011540000.parquet",
                partitions=[
                    KeyValue(key="chain", value="DUMMYCHAIN"),
                    KeyValue(key="dt", value="2024-10-25"),
                ],
                row_count=5045,
            ),
            WrittenParquetPath(
                root="ingestion/blocks_v1",
                basename="000011540000.parquet",
                partitions=[
                    KeyValue(key="chain", value="DUMMYCHAIN"),
                    KeyValue(key="dt", value="2024-10-26"),
                ],
                row_count=14955,
            ),
        ],
        process_name="default",
        additional_columns={"num_blocks": 20000, "min_block": 11540000, "max_block": 11560000},
        additional_columns_schema=[
            pa.field("chain", pa.string()),
            pa.field("dt", pa.date32()),
            pa.field("num_blocks", pa.int32()),
            pa.field("min_block", pa.int64()),
            pa.field("max_block", pa.int64()),
        ],
    )

    initially_exists = marker_exists(DataLocation.LOCAL, marker.marker_path)
    assert not initially_exists

    write_marker(DataLocation.LOCAL, marker.to_pyarrow_table())

    result = (
        run_query(
            "SELECT * FROM etl_monitor.raw_onchain_ingestion_markers WHERE chain = 'DUMMYCHAIN'"
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

    exists = marker_exists(DataLocation.LOCAL, marker.marker_path)
    assert exists

    markers_df = markers_for_dates(
        DataLocation.LOCAL, datevals=[datetime.date(2024, 10, 25)], chains=["DUMMYCHAIN"]
    )
    assert len(markers_df) == 1

    assert (
        markers_df.limit(1)["data_path"].item()
        == "ingestion/blocks_v1/chain=DUMMYCHAIN/dt=2024-10-25/000011540000.parquet"
    )
