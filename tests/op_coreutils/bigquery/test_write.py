# -*- coding: utf-8 -*-
import pytest
import io
from unittest.mock import MagicMock
from datetime import date, datetime
import polars as pl
import pandas as pd
import numpy as np
from google.cloud import bigquery
from op_coreutils.bigquery.write import (
    overwrite_table,
    overwrite_partition_static,
    overwrite_partitions_dynamic,
    OPLabsBigQueryError,
    upsert_table,
    init_client,
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


def test_overwrite_table():
    client = init_client()  # Get the MagicMock client

    # Mock the load_table_from_file method
    mock_load_job = MagicMock()
    client.load_table_from_file.return_value = mock_load_job

    overwrite_table(test_df, "test_dataset", "test_table_staging")
    args, kwargs = client.load_table_from_file.call_args
    assert kwargs["destination"] == "test_dataset.test_table_staging"
    assert kwargs["job_config"].write_disposition == "WRITE_TRUNCATE"

    # Testing with incorrect table name to raise custom error
    with pytest.raises(
        OPLabsBigQueryError, match="cannot overwrite data at test_dataset.test_table"
    ):
        overwrite_table(test_df, "test_dataset", "test_table")


def test_overwrite_partition_static():
    client = init_client()  # Get the MagicMock client

    overwrite_partition_static(test_df, date(2024, 1, 1), "test_dataset", "test_table")
    args, kwargs = client.load_table_from_file.call_args
    assert kwargs["destination"] == "test_dataset.test_table"


def test_overwrite_partitions_dynamic():
    client = init_client()  # Get the MagicMock client

    overwrite_partitions_dynamic(test_df, "test_dataset", "test_table")
    args, kwargs = client.load_table_from_file.call_args
    assert kwargs["destination"] == "test_dataset.test_table"
    job_config = kwargs["job_config"]
    assert job_config.time_partitioning.field == "dt"


def test_upsert_table():
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

    client = init_client()  # Get the MagicMock client

    # Mock the load_table_from_file method
    mock_load_job = MagicMock()
    client.load_table_from_file.return_value = mock_load_job
    mock_load_job.result.return_value = None

    # Mock the query method
    mock_query_job = MagicMock()
    client.query.return_value = mock_query_job
    mock_query_job.result.return_value = None

    # Mock get_table and update_table for setting expiration
    mock_table = MagicMock()
    client.get_table.return_value = mock_table

    # Call the function under test without partition_dt
    upsert_table(
        df=test_df, dataset=dataset, table_name=table_name, unique_keys=unique_keys
    )

    # Assertions
    # Verify that the staging table was written with WRITE_EMPTY
    client.load_table_from_file.assert_called()
    args, kwargs = client.load_table_from_file.call_args
    destination = kwargs["destination"]
    assert destination.startswith(f"{dataset}_staging.{table_name}_staging_")
    assert (
        kwargs["job_config"].write_disposition == bigquery.WriteDisposition.WRITE_EMPTY
    )

    # Check that the merge query was constructed correctly
    merge_query = client.query.call_args[0][0]
    assert "MERGE" in merge_query
    assert "ON T.id = S.id" in merge_query

    # Check that the staging table was deleted
    client.delete_table.assert_called_with(destination)


def test_upsert_table_with_partition_dt():
    # Create a test DataFrame without 'dt' column
    test_df = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "value": [10, 20, 30],
        }
    )

    dataset = "test_dataset"
    table_name = "test_table"
    unique_keys = ["id", "dt"]
    partition_dt = "2024-01-01"

    client = init_client()  # Get the MagicMock client

    # Prepare a mock load job
    mock_load_job = MagicMock()
    mock_load_job.result.return_value = None

    # Variable to capture the stream
    captured_stream = None

    # Define side_effect function to capture the stream
    def load_table_from_file_side_effect(stream, destination, job_config):
        nonlocal captured_stream
        # Make a copy of the stream content
        captured_stream = io.BytesIO(stream.read())
        # Reset the original stream position
        stream.seek(0)
        return mock_load_job

    # Set the side_effect on the mocked method
    client.load_table_from_file.side_effect = load_table_from_file_side_effect

    # Mock the query method
    mock_query_job = MagicMock()
    client.query.return_value = mock_query_job
    mock_query_job.result.return_value = None

    # Mock get_table and update_table for setting expiration
    mock_table = MagicMock()
    client.get_table.return_value = mock_table

    # Call the function under test with partition_dt
    upsert_table(
        df=test_df,
        dataset=dataset,
        table_name=table_name,
        unique_keys=unique_keys,
        partition_dt=partition_dt,
    )

    # Assertions
    # Verify that the staging table was written with WRITE_EMPTY
    client.load_table_from_file.assert_called()
    args, kwargs = client.load_table_from_file.call_args
    destination = kwargs["destination"]
    assert destination.startswith(f"{dataset}_staging.{table_name}_staging_")
    assert (
        kwargs["job_config"].write_disposition == bigquery.WriteDisposition.WRITE_EMPTY
    )

    # Check that the DataFrame has 'dt' column set to the correct date
    if captured_stream is not None:
        captured_stream.seek(0)
        written_df = pl.read_parquet(captured_stream)
        assert "dt" in written_df.columns
        assert written_df["dt"].unique().to_list() == [
            datetime.strptime(partition_dt, "%Y-%m-%d")
        ]
    else:
        pytest.fail("Failed to capture the stream.")

    # Check that the merge query was constructed correctly
    merge_query = client.query.call_args[0][0]
    assert "MERGE" in merge_query
    assert "ON T.id = S.id AND T.dt = S.dt" in merge_query

    # Check that the staging table was deleted
    client.delete_table.assert_called_with(destination)
