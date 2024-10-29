# -*- coding: utf-8 -*-
# test_write.py

from unittest.mock import patch, MagicMock
import polars as pl
import pytest
import numpy as np
import pandas as pd

from op_coreutils.bigquery.write import (
    init_client,
    ensure_valid_dt,
    overwrite_table,
    overwrite_partition,
    overwrite_partitions,
    OPLabsBigQueryError,
    OPLabsEnvironment,
    upsert_partition,
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


def reset_client():
    """Utility function to reset the global _CLIENT variable."""
    import op_coreutils.bigquery.write

    op_coreutils.bigquery.write._CLIENT = None


@patch("op_coreutils.bigquery.write.get_credentials")
@patch("op_coreutils.bigquery.write.bigquery.Client")
def test_init_client(mock_bigquery_client, mock_get_credentials):
    reset_client()
    with patch(
        "op_coreutils.bigquery.write.current_environment",
        return_value=OPLabsEnvironment.DEV,
    ):
        client = init_client()
        assert isinstance(
            client, MagicMock
        ), "Client should be MagicMock in non-PROD environments."

    reset_client()
    with patch(
        "op_coreutils.bigquery.write.current_environment",
        return_value=OPLabsEnvironment.PROD,
    ):
        client = init_client()
        mock_bigquery_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value
        )
        assert client == mock_bigquery_client.return_value


def test_ensure_valid_dt():
    valid_df = pl.DataFrame({"dt": ["2024-01-01"], "data": [100]})
    try:
        ensure_valid_dt(valid_df, "2024-01-01")
    except OPLabsBigQueryError:
        pytest.fail(
            "ensure_valid_dt() raised an unexpected OPLabsBigQueryError for a valid date."
        )

    with pytest.raises(
        OPLabsBigQueryError, match="invalid date partition dt=invalid-date"
    ):
        ensure_valid_dt(valid_df, "invalid-date")

    empty_df = pl.DataFrame()
    with pytest.raises(
        ValueError, match="DataFrame is empty or missing required 'dt' column."
    ):
        ensure_valid_dt(empty_df, "2024-01-01")

    df_missing_dt = pl.DataFrame({"data": [1, 2, 3]})
    with pytest.raises(
        ValueError, match="DataFrame is empty or missing required 'dt' column."
    ):
        ensure_valid_dt(df_missing_dt, "2024-01-01")


@patch("op_coreutils.bigquery.write.init_client")
def test_overwrite_partitions_empty_df(mock_init_client):
    mock_client = MagicMock()
    mock_init_client.return_value = mock_client

    empty_df = pl.DataFrame()
    with pytest.raises(
        ValueError, match="DataFrame is empty or missing required 'dt' column."
    ):
        overwrite_partitions(empty_df, "test_dataset", "test_table")


@patch("op_coreutils.bigquery.write.init_client")
def test_overwrite_table(mock_init_client):
    mock_client = MagicMock()
    mock_init_client.return_value = mock_client

    mock_load_job = MagicMock()
    mock_client.load_table_from_file.return_value = mock_load_job
    mock_load_job.result.return_value = None

    overwrite_table(test_df, "test_dataset", "test_table_staging")

    mock_client.load_table_from_file.assert_called_once()
    args, kwargs = mock_client.load_table_from_file.call_args
    assert kwargs["destination"] == "test_dataset.test_table_staging"
    assert kwargs["job_config"].write_disposition == "WRITE_TRUNCATE"
    assert mock_load_job.result.called

    with pytest.raises(
        OPLabsBigQueryError, match="cannot overwrite data at test_dataset.test_table"
    ):
        overwrite_table(test_df, "test_dataset", "test_table")


@patch("op_coreutils.bigquery.write.init_client")
def test_overwrite_partition(mock_init_client):
    mock_client = MagicMock()
    mock_init_client.return_value = mock_client

    mock_load_job = MagicMock()
    mock_client.load_table_from_file.return_value = mock_load_job
    mock_load_job.result.return_value = None

    overwrite_partition(test_df, "2024-01-01", "test_dataset", "test_table")

    mock_client.load_table_from_file.assert_called_once()
    assert mock_load_job.result.called

    with pytest.raises(
        OPLabsBigQueryError, match="invalid date partition dt=invalid-date"
    ):
        overwrite_partition(test_df, "invalid-date", "test_dataset", "test_table")


@patch("op_coreutils.bigquery.write.init_client")
def test_overwrite_partitions(mock_init_client):
    mock_client = MagicMock()
    mock_init_client.return_value = mock_client

    mock_load_job = MagicMock()
    mock_client.load_table_from_file.return_value = mock_load_job
    mock_load_job.result.return_value = None

    overwrite_partitions(test_df, "test_dataset", "test_table")

    mock_client.load_table_from_file.assert_called_once()
    assert mock_load_job.result.called

    args, kwargs = mock_client.load_table_from_file.call_args
    job_config = kwargs["job_config"]
    assert job_config.time_partitioning.field == "dt"


@patch("op_coreutils.bigquery.write.init_client")
def test_upsert_partition_default_unique_key(mock_init_client):
    """Test upsert_partition with default unique_keys (['dt'])."""
    mock_client = MagicMock()
    mock_init_client.return_value = mock_client

    mock_load_job = MagicMock()
    mock_client.load_table_from_file.return_value = mock_load_job
    mock_load_job.result.return_value = None

    mock_query_job = MagicMock()
    mock_client.query.return_value = mock_query_job
    mock_query_job.result.return_value = None

    mock_client.delete_table.return_value = None

    upsert_partition(test_df, "2024-01-01", "test_dataset", "test_table")

    merge_query = mock_client.query.call_args[0][0]
    assert "MERGE" in merge_query
    assert (
        "ON T.dt = S.dt" in merge_query
    ), "Merge condition should use 'dt' as the unique key."


@patch("op_coreutils.bigquery.write.init_client")
def test_upsert_partition_custom_unique_keys(mock_init_client):
    """Test upsert_partition with custom unique_keys (['dt', 'category'])."""
    mock_client = MagicMock()
    mock_init_client.return_value = mock_client

    mock_load_job = MagicMock()
    mock_client.load_table_from_file.return_value = mock_load_job
    mock_load_job.result.return_value = None

    mock_query_job = MagicMock()
    mock_client.query.return_value = mock_query_job
    mock_query_job.result.return_value = None

    mock_client.delete_table.return_value = None

    upsert_partition(
        test_df,
        "2024-01-01",
        "test_dataset",
        "test_table",
        unique_keys=["dt", "category"],
    )

    merge_query = mock_client.query.call_args[0][0]
    assert "MERGE" in merge_query
    assert (
        "ON T.dt = S.dt AND T.category = S.category" in merge_query
    ), "Merge condition should use 'dt' and 'category' as the unique keys."


@patch("op_coreutils.bigquery.write.init_client")
def test_upsert_partition_single_column_unique_key(mock_init_client):
    """Test upsert_partition with a single unique key (['category'])."""
    mock_client = MagicMock()
    mock_init_client.return_value = mock_client

    mock_load_job = MagicMock()
    mock_client.load_table_from_file.return_value = mock_load_job
    mock_load_job.result.return_value = None

    mock_query_job = MagicMock()
    mock_client.query.return_value = mock_query_job
    mock_query_job.result.return_value = None

    mock_client.delete_table.return_value = None

    upsert_partition(
        test_df, "2024-01-01", "test_dataset", "test_table", unique_keys=["category"]
    )

    merge_query = mock_client.query.call_args[0][0]
    assert "MERGE" in merge_query
    assert (
        "ON T.category = S.category" in merge_query
    ), "Merge condition should use 'category' as the unique key."


def test_overwrite_partition_invalid_dt():
    with pytest.raises(
        OPLabsBigQueryError, match="invalid date partition dt=not-a-date"
    ):
        overwrite_partition(test_df, "not-a-date", "test_dataset", "test_table")
