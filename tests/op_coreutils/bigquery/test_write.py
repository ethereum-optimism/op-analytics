from datetime import date

import numpy as np
import pandas as pd
import polars as pl
import pytest
from op_coreutils.bigquery.write import (
    OPLabsBigQueryError,
    init_client,
    most_recent_dates,
    overwrite_partition_static,
    overwrite_partitioned_table,
    overwrite_partitions_dynamic,
    overwrite_unpartitioned_table,
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

    # Testing with incorrect table name to raise custom error
    with pytest.raises(
        OPLabsBigQueryError, match="cannot overwrite data at test_dataset.test_table"
    ):
        overwrite_unpartitioned_table(test_df, "test_dataset", "test_table")


def test_overwrite_partitioned_table():
    mock_client = init_client()
    mock_client.reset_mock()

    overwrite_partitioned_table(test_df, "test_dataset", "test_table_staging")
    args, kwargs = mock_client.load_table_from_file.call_args
    assert kwargs["destination"] == "test_dataset.test_table_staging"
    assert kwargs["job_config"].write_disposition == "WRITE_TRUNCATE"

    # Testing with incorrect table name to raise custom error
    with pytest.raises(
        OPLabsBigQueryError, match="cannot overwrite data at test_dataset.test_table"
    ):
        overwrite_unpartitioned_table(test_df, "test_dataset", "test_table")


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
