import io
import re
from datetime import date, datetime
from unittest.mock import MagicMock


import numpy as np
import pandas as pd
import polars as pl
import pytest
from google.cloud import bigquery

from op_coreutils.bigquery.write import (
    OPLabsBigQueryError,
    init_client,
    most_recent_dates,
    overwrite_partition_static,
    overwrite_partitioned_table,
    overwrite_partitions_dynamic,
    overwrite_unpartitioned_table,
    upsert_unpartitioned_table,
    upsert_partitioned_table,
)

# Generate a larger sample DataFrame for testing
date_range = pd.date_range("2024-01-01", periods=100, freq="D")
num_rows = 1000

# Create random data for each column
test_df = pl.DataFrame(
    {
        "dt": np.random.choice(date_range, num_rows),
        "data": np.random.randint(100, 500, size=num_rows),
        "category": np.random.choice(["A", "B", "C", "D"], num_rows),
        "value": np.random.uniform(1.0, 10.0, size=num_rows).round(2),
    }
)

# Sort by 'dt' for consistency
test_df = test_df.sort("dt")


def test_overwrite_unpartitioned_table():
    mock_client = init_client()
    mock_client.reset_mock()

    overwrite_unpartitioned_table(test_df, "test_dataset", "test_table_staging")
    args, kwargs = mock_client.load_table_from_file.call_args
    assert kwargs["destination"] == "test_dataset.test_table_staging"
    assert kwargs["job_config"].write_disposition == "WRITE_TRUNCATE"


def test_overwrite_partitioned_table():
    mock_client = init_client()
    mock_client.reset_mock()

    overwrite_partitioned_table(test_df, "test_dataset", "test_table_staging")
    args, kwargs = mock_client.load_table_from_file.call_args
    assert kwargs["destination"] == "test_dataset.test_table_staging"
    assert kwargs["job_config"].write_disposition == "WRITE_TRUNCATE"


def test_overwrite_partition_static():
    mock_client = init_client()

    overwrite_partition_static(test_df, date(2024, 1, 1), "test_dataset", "test_table")
    args, kwargs = mock_client.load_table_from_file.call_args
    assert kwargs["destination"] == "test_dataset.test_table$20240101"


def test_overwrite_partitions_dynamic_fail():
    mock_client = init_client()
    mock_client.reset_mock()

    with pytest.raises(OPLabsBigQueryError) as ex:
        overwrite_partitions_dynamic(test_df, "test_dataset", "test_table")
    assert ex.value.args == (
        "Dynamic Partition Overwrite detected more than 10 partitions. Aborting.",
    )


def test_overwrite_partitions_dynamic_pass():
    mock_client = init_client()
    mock_client.reset_mock()

    overwrite_partitions_dynamic(
        most_recent_dates(test_df, n_dates=3), "test_dataset", "test_table"
    )

    assert (
        mock_client.load_table_from_file.call_args_list[0]
        .kwargs["job_config"]
        .time_partitioning.field
        == "dt"
    )

    actual = [_.kwargs["destination"] for _ in mock_client.load_table_from_file.call_args_list]

    assert actual == [
        "test_dataset.test_table$20240407",
        "test_dataset.test_table$20240408",
        "test_dataset.test_table$20240409",
    ]


def test_overwrite_partitions_dynamic_pass_with_dt_string():
    mock_client = init_client()
    mock_client.reset_mock()

    overwrite_partitions_dynamic(
        df=most_recent_dates(
            df=test_df.with_columns(dt=pl.col("dt").dt.strftime("%Y-%m-%d")), n_dates=3
        ),
        dataset="test_dataset",
        table_name="test_table",
    )

    assert (
        mock_client.load_table_from_file.call_args_list[0]
        .kwargs["job_config"]
        .time_partitioning.field
        == "dt"
    )

    actual = [_.kwargs["destination"] for _ in mock_client.load_table_from_file.call_args_list]

    assert actual == [
        "test_dataset.test_table$20240407",
        "test_dataset.test_table$20240408",
        "test_dataset.test_table$20240409",
    ]


def test_upsert_unpartitioned_table():
    mock_client = init_client()
    mock_client.reset_mock()

    # Create a test DataFrame
    test_df = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "value": [10, 20, 30],
        }
    )

    dataset = "test_dataset"
    table_name = "test_table"
    unique_keys = ["id"]

    # Mock the load_table_from_file method
    mock_load_job = MagicMock()
    mock_client.load_table_from_file.return_value = mock_load_job
    mock_load_job.result.return_value = None

    # Mock the query method
    mock_query_job = MagicMock()
    mock_client.query.return_value = mock_query_job
    mock_query_job.result.return_value = None

    # Mock get_table and update_table for setting expiration
    mock_table = MagicMock()
    mock_client.get_table.return_value = mock_table

    upsert_unpartitioned_table(
        df=test_df, dataset=dataset, table_name=table_name, unique_keys=unique_keys
    )

    # Assertions
    # Verify that the staging table was written with WRITE_EMPTY
    mock_client.load_table_from_file.assert_called()
    args, kwargs = mock_client.load_table_from_file.call_args
    destination = kwargs["destination"]
    assert re.match(r"temp_upserts.[\w_]+_\d{12}-\w{8}$", destination)
    assert kwargs["job_config"].write_disposition == bigquery.WriteDisposition.WRITE_EMPTY

    # Check that the merge query was constructed correctly
    merge_query = mock_client.query.call_args[0][0]
    assert "MERGE" in merge_query
    assert "ON T.id = S.id" in merge_query


def test_upsert_partitioned_table():
    mock_client = init_client()
    mock_client.reset_mock()

    # Create a test DataFrame with 2 different dts.
    test_df = pl.DataFrame(
        {
            "dt": [date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 2)],
            "id": [1, 2, 3],
            "value": [10, 20, 30],
        }
    )

    dataset = "test_dataset"
    table_name = "test_table"
    unique_keys = ["id", "dt"]

    # Prepare a mock load job
    mock_load_job = MagicMock()
    mock_load_job.result.return_value = None

    # Capture all dataframes that are written out.
    written_dataframes = []

    # Define side_effect function to capture the stream
    def load_table_from_file_side_effect(stream, destination, job_config):
        # Make a copy of the stream content
        captured_stream = io.BytesIO(stream.read())
        written_dataframes.append(pl.read_parquet(captured_stream))
        # Reset the original stream position
        stream.seek(0)
        return mock_load_job

    # Set the side_effect on the mocked method
    mock_client.load_table_from_file.side_effect = load_table_from_file_side_effect

    # Mock the query method
    mock_query_job = MagicMock()
    mock_client.query.return_value = mock_query_job
    mock_query_job.result.return_value = None

    # Mock get_table and update_table for setting expiration
    mock_table = MagicMock()
    mock_client.get_table.return_value = mock_table

    upsert_partitioned_table(
        df=test_df,
        dataset=dataset,
        table_name=table_name,
        unique_keys=unique_keys,
    )

    # Assertions
    # Verify that the staging table was written with WRITE_EMPTY
    mock_client.load_table_from_file.assert_called()

    for call_args in mock_client.load_table_from_file.call_args_list:
        assert re.match(
            r"temp_upserts.test_table_202401\d{2}_\d{12}-\w{8}$", call_args.kwargs["destination"]
        )
        assert (
            call_args.kwargs["job_config"].write_disposition
            == bigquery.WriteDisposition.WRITE_EMPTY
        )

    # Check that the DataFrame has 'dt' column set to the correct date
    assert len(written_dataframes) == 2

    assert len(written_dataframes[0]) == 2  # 2 rows
    assert written_dataframes[0]["dt"].unique().to_list() == [date(2024, 1, 1)]

    assert len(written_dataframes[1]) == 1  # 1 row
    assert written_dataframes[1]["dt"].unique().to_list() == [date(2024, 1, 2)]

    # Check that the merge query was constructed correctly
    merge_query = mock_client.query.call_args[0][0]
    assert "MERGE" in merge_query
    assert "ON T.id = S.id AND T.dt = S.dt" in merge_query


def test_most_recent_dates():
    assert test_df["dt"].unique().to_list() == [
        datetime(2024, 1, 1, 0, 0),
        datetime(2024, 1, 2, 0, 0),
        datetime(2024, 1, 3, 0, 0),
        datetime(2024, 1, 4, 0, 0),
        datetime(2024, 1, 5, 0, 0),
        datetime(2024, 1, 6, 0, 0),
        datetime(2024, 1, 7, 0, 0),
        datetime(2024, 1, 8, 0, 0),
        datetime(2024, 1, 9, 0, 0),
        datetime(2024, 1, 10, 0, 0),
        datetime(2024, 1, 11, 0, 0),
        datetime(2024, 1, 12, 0, 0),
        datetime(2024, 1, 13, 0, 0),
        datetime(2024, 1, 14, 0, 0),
        datetime(2024, 1, 15, 0, 0),
        datetime(2024, 1, 16, 0, 0),
        datetime(2024, 1, 17, 0, 0),
        datetime(2024, 1, 18, 0, 0),
        datetime(2024, 1, 19, 0, 0),
        datetime(2024, 1, 20, 0, 0),
        datetime(2024, 1, 21, 0, 0),
        datetime(2024, 1, 22, 0, 0),
        datetime(2024, 1, 23, 0, 0),
        datetime(2024, 1, 24, 0, 0),
        datetime(2024, 1, 25, 0, 0),
        datetime(2024, 1, 26, 0, 0),
        datetime(2024, 1, 27, 0, 0),
        datetime(2024, 1, 28, 0, 0),
        datetime(2024, 1, 29, 0, 0),
        datetime(2024, 1, 30, 0, 0),
        datetime(2024, 1, 31, 0, 0),
        datetime(2024, 2, 1, 0, 0),
        datetime(2024, 2, 2, 0, 0),
        datetime(2024, 2, 3, 0, 0),
        datetime(2024, 2, 4, 0, 0),
        datetime(2024, 2, 5, 0, 0),
        datetime(2024, 2, 6, 0, 0),
        datetime(2024, 2, 7, 0, 0),
        datetime(2024, 2, 8, 0, 0),
        datetime(2024, 2, 9, 0, 0),
        datetime(2024, 2, 10, 0, 0),
        datetime(2024, 2, 11, 0, 0),
        datetime(2024, 2, 12, 0, 0),
        datetime(2024, 2, 13, 0, 0),
        datetime(2024, 2, 14, 0, 0),
        datetime(2024, 2, 15, 0, 0),
        datetime(2024, 2, 16, 0, 0),
        datetime(2024, 2, 17, 0, 0),
        datetime(2024, 2, 18, 0, 0),
        datetime(2024, 2, 19, 0, 0),
        datetime(2024, 2, 20, 0, 0),
        datetime(2024, 2, 21, 0, 0),
        datetime(2024, 2, 22, 0, 0),
        datetime(2024, 2, 23, 0, 0),
        datetime(2024, 2, 24, 0, 0),
        datetime(2024, 2, 25, 0, 0),
        datetime(2024, 2, 26, 0, 0),
        datetime(2024, 2, 27, 0, 0),
        datetime(2024, 2, 28, 0, 0),
        datetime(2024, 2, 29, 0, 0),
        datetime(2024, 3, 1, 0, 0),
        datetime(2024, 3, 2, 0, 0),
        datetime(2024, 3, 3, 0, 0),
        datetime(2024, 3, 4, 0, 0),
        datetime(2024, 3, 5, 0, 0),
        datetime(2024, 3, 6, 0, 0),
        datetime(2024, 3, 7, 0, 0),
        datetime(2024, 3, 8, 0, 0),
        datetime(2024, 3, 9, 0, 0),
        datetime(2024, 3, 10, 0, 0),
        datetime(2024, 3, 11, 0, 0),
        datetime(2024, 3, 12, 0, 0),
        datetime(2024, 3, 13, 0, 0),
        datetime(2024, 3, 14, 0, 0),
        datetime(2024, 3, 15, 0, 0),
        datetime(2024, 3, 16, 0, 0),
        datetime(2024, 3, 17, 0, 0),
        datetime(2024, 3, 18, 0, 0),
        datetime(2024, 3, 19, 0, 0),
        datetime(2024, 3, 20, 0, 0),
        datetime(2024, 3, 21, 0, 0),
        datetime(2024, 3, 22, 0, 0),
        datetime(2024, 3, 23, 0, 0),
        datetime(2024, 3, 24, 0, 0),
        datetime(2024, 3, 25, 0, 0),
        datetime(2024, 3, 26, 0, 0),
        datetime(2024, 3, 27, 0, 0),
        datetime(2024, 3, 28, 0, 0),
        datetime(2024, 3, 29, 0, 0),
        datetime(2024, 3, 30, 0, 0),
        datetime(2024, 3, 31, 0, 0),
        datetime(2024, 4, 1, 0, 0),
        datetime(2024, 4, 2, 0, 0),
        datetime(2024, 4, 3, 0, 0),
        datetime(2024, 4, 4, 0, 0),
        datetime(2024, 4, 5, 0, 0),
        datetime(2024, 4, 6, 0, 0),
        datetime(2024, 4, 7, 0, 0),
        datetime(2024, 4, 8, 0, 0),
        datetime(2024, 4, 9, 0, 0),
    ]

    actual = most_recent_dates(test_df, n_dates=3)["dt"].unique().to_list()
    expected = [
        datetime(2024, 4, 7, 0, 0),
        datetime(2024, 4, 8, 0, 0),
        datetime(2024, 4, 9, 0, 0),
    ]
    assert actual == expected

    actual2 = (
        most_recent_dates(test_df.rename({"dt": "date"}), n_dates=3, date_column="date")["date"]
        .unique()
        .to_list()
    )
    assert actual2 == expected
