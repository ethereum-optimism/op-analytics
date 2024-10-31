# -*- coding: utf-8 -*-
import pytest
from unittest.mock import patch, MagicMock
from datetime import date
import polars as pl
import pandas as pd
import numpy as np
from op_coreutils.bigquery.write import (
    overwrite_table,
    overwrite_partition_static,
    overwrite_partitions_dynamic,
    upsert_partition,
    OPLabsBigQueryError,
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


@patch("op_coreutils.bigquery.write.init_client")
def test_overwrite_table(mock_init_client):
    mock_client = MagicMock()
    mock_init_client.return_value = mock_client

    mock_load_job = MagicMock()
    mock_client.load_table_from_file.return_value = mock_load_job

    overwrite_table(test_df, "test_dataset", "test_table_staging")
    args, kwargs = mock_client.load_table_from_file.call_args
    assert kwargs["destination"] == "test_dataset.test_table_staging"
    assert kwargs["job_config"].write_disposition == "WRITE_TRUNCATE"

    # Testing with incorrect table name to raise custom error
    with pytest.raises(
        OPLabsBigQueryError, match="cannot overwrite data at test_dataset.test_table"
    ):
        overwrite_table(test_df, "test_dataset", "test_table")


@patch("op_coreutils.bigquery.write.init_client")
def test_overwrite_partition_static(mock_init_client):
    mock_client = MagicMock()
    mock_init_client.return_value = mock_client

    overwrite_partition_static(test_df, date(2024, 1, 1), "test_dataset", "test_table")
    args, kwargs = mock_client.load_table_from_file.call_args
    assert kwargs["destination"] == "test_dataset.test_table"


@patch("op_coreutils.bigquery.write.init_client")
def test_overwrite_partitions_dynamic(mock_init_client):
    mock_client = MagicMock()
    mock_init_client.return_value = mock_client

    overwrite_partitions_dynamic(test_df, "test_dataset", "test_table")
    args, kwargs = mock_client.load_table_from_file.call_args
    assert kwargs["destination"] == "test_dataset.test_table"
    job_config = kwargs["job_config"]
    assert job_config.time_partitioning.field == "dt"


@patch("op_coreutils.bigquery.write.init_client")
def test_upsert_partition(mock_init_client):
    mock_client = MagicMock()
    mock_init_client.return_value = mock_client

    mock_query_job = MagicMock()
    mock_client.query.return_value = mock_query_job
    mock_query_job.result.return_value = None

    upsert_partition(
        test_df, "2024-01-01", "test_dataset", "test_table", unique_keys=["dt"]
    )
    merge_query = mock_client.query.call_args[0][0]
    assert "MERGE" in merge_query
    assert "ON T.dt = S.dt" in merge_query

    # Verify that the staging table was written with WRITE_TRUNCATE
    mock_client.load_table_from_file.assert_called()
    args, kwargs = mock_client.load_table_from_file.call_args
    assert kwargs["destination"] == "test_dataset.test_table_staging"
    assert kwargs["job_config"].write_disposition == "WRITE_TRUNCATE"

    # Verify that the staging table was deleted after use
    mock_client.delete_table.assert_called_with("test_dataset.test_table_staging")
