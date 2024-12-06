import datetime

import polars as pl

from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.datapipeline.etl.ingestion.reader_bydate import are_inputs_ready

MARKER_PATHS_DATA = [
    {
        "marker_path": "dummy",
        "chain": "fraxtal",
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-22/000011400000.parquet",
        "root_path": "ingestion/traces_v1",
        "dt": datetime.date(2024, 10, 22),
        "max_block": 11420000,
        "min_block": 11400000,
        "num_blocks": 20000,
    },
    {
        "marker_path": "dummy",
        "chain": "fraxtal",
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-23/000011400000.parquet",
        "root_path": "ingestion/traces_v1",
        "dt": datetime.date(2024, 10, 23),
        "max_block": 11420000,
        "min_block": 11400000,
        "num_blocks": 20000,
    },
    {
        "marker_path": "dummy",
        "chain": "fraxtal",
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-23/000011420000.parquet",
        "root_path": "ingestion/traces_v1",
        "dt": datetime.date(2024, 10, 23),
        "max_block": 11440000,
        "min_block": 11420000,
        "num_blocks": 20000,
    },
    {
        "chain": "fraxtal",
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-23/000011440000.parquet",
        "root_path": "ingestion/traces_v1",
        "dt": datetime.date(2024, 10, 23),
        "max_block": 11460000,
        "min_block": 11440000,
        "num_blocks": 20000,
    },
    {
        "marker_path": "dummy",
        "chain": "fraxtal",
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-24/000011440000.parquet",
        "root_path": "ingestion/traces_v1",
        "dt": datetime.date(2024, 10, 24),
        "max_block": 11460000,
        "min_block": 11440000,
        "num_blocks": 20000,
    },
    {
        "marker_path": "dummy",
        "chain": "fraxtal",
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-24/000011460000.parquet",
        "root_path": "ingestion/traces_v1",
        "dt": datetime.date(2024, 10, 24),
        "max_block": 11480000,
        "min_block": 11460000,
        "num_blocks": 20000,
    },
    {
        "marker_path": "dummy",
        "chain": "fraxtal",
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-24/000011480000.parquet",
        "root_path": "ingestion/traces_v1",
        "dt": datetime.date(2024, 10, 24),
        "max_block": 11500000,
        "min_block": 11480000,
        "num_blocks": 20000,
    },
    {
        "marker_path": "dummy",
        "chain": "fraxtal",
        "data_path": "ingestion/traces_v1/chain=fraxtal/dt=2024-10-24/000011500000.parquet",
        "root_path": "ingestion/traces_v1",
        "dt": datetime.date(2024, 10, 24),
        "max_block": 11520000,
        "min_block": 11500000,
        "num_blocks": 20000,
    },
]


def test_are_inputs_ready():
    markers_df = pl.DataFrame(MARKER_PATHS_DATA, schema_overrides={"num_blocks": pl.Int32})
    dateval = datetime.date(2024, 10, 23)

    input_data = are_inputs_ready(
        markers_df=markers_df,
        dateval=dateval,
        root_paths_to_check={
            "ingestion/traces_v1",
        },
        storage_location=DataLocation.GCS,
    )
    assert input_data.is_complete
    assert input_data.data_paths == {
        "ingestion/traces_v1": [
            "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=fraxtal/dt=2024-10-23/000011400000.parquet",
            "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=fraxtal/dt=2024-10-23/000011420000.parquet",
            "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=fraxtal/dt=2024-10-23/000011440000.parquet",
        ]
    }


def test_not_ready_01():
    markers_df = pl.DataFrame(MARKER_PATHS_DATA[:-4], schema_overrides={"num_blocks": pl.Int32})
    dateval = datetime.date(2024, 10, 23)

    input_data = are_inputs_ready(
        markers_df=markers_df,
        dateval=dateval,
        root_paths_to_check={
            "ingestion/traces_v1",
        },
        storage_location=DataLocation.GCS,
    )
    assert not input_data.is_complete
    assert input_data.data_paths is None


def test_not_ready_02():
    markers_df = pl.DataFrame(MARKER_PATHS_DATA, schema_overrides={"num_blocks": pl.Int32})
    dateval = datetime.date(2024, 10, 23)

    input_data = are_inputs_ready(
        markers_df=markers_df,
        dateval=dateval,
        root_paths_to_check={
            "ingestion/traces_v1",
            "ingestion/logs_v1",
        },
        storage_location=DataLocation.GCS,
    )
    assert not input_data.is_complete
    assert input_data.data_paths is None
