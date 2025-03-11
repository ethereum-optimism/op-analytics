from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from clickhouse_connect.driver.summary import QuerySummary

from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.transforms.create import TableColumn, TableStructure
from op_analytics.transforms.main import execute_dt_transforms
from op_analytics.transforms.updates import Step, StepType
from op_analytics.transforms.transform import NoWrittenRows


@pytest.fixture
def mock_create_tables():
    """Mock the create_tables function to return a predefined set of tables."""
    with patch("op_analytics.transforms.main.create_tables") as mock:
        # Create a sample table structure that matches the interop group
        tables = {
            "dim_erc20_first_seen_v1": TableStructure(
                name="dim_erc20_first_seen_v1",
                columns=[
                    TableColumn(name="token_address", data_type="String"),
                    TableColumn(name="chain_id", data_type="UInt64"),
                    TableColumn(name="first_seen_block_number", data_type="UInt64"),
                    TableColumn(name="first_seen_timestamp", data_type="DateTime"),
                    TableColumn(name="dt", data_type="Date"),
                ],
            ),
            "fact_erc20_oft_transfers_v1": TableStructure(
                name="fact_erc20_oft_transfers_v1",
                columns=[
                    TableColumn(name="token_address", data_type="String"),
                    TableColumn(name="from_chain_id", data_type="UInt64"),
                    TableColumn(name="to_chain_id", data_type="UInt64"),
                    TableColumn(name="amount", data_type="UInt256"),
                    TableColumn(name="block_number", data_type="UInt64"),
                    TableColumn(name="timestamp", data_type="DateTime"),
                    TableColumn(name="dt", data_type="Date"),
                ],
            ),
        }
        mock.return_value = tables
        yield mock


@pytest.fixture
def mock_existing_markers():
    """Mock the existing_markers function to return an empty DataFrame."""
    with patch("op_analytics.transforms.main.existing_markers") as mock:
        # Return an empty DataFrame to simulate no existing markers
        mock.return_value = pl.DataFrame(schema={"transform": pl.Utf8, "dt": pl.Date})
        yield mock


@pytest.fixture
def mock_transform_task():
    """Mock the TransformTask class to avoid actual execution."""
    with patch("op_analytics.transforms.main.TransformTask") as mock:
        # Configure the mock to return a predefined result when execute is called
        task_instance = MagicMock()
        task_instance.execute.return_value = {"status": "success", "rows_written": 100}
        mock.return_value = task_instance
        yield mock


@pytest.fixture
def mock_new_stateful_client():
    """Mock the new_stateful_client function in create.py."""
    with patch("op_analytics.transforms.create.new_stateful_client") as mock:
        client = MagicMock()

        # Mock the command method
        command_result = MagicMock(spec=QuerySummary)
        command_result.summary = {"status": "success"}
        client.command.return_value = command_result

        # Mock the query_arrow method
        arrow_data = MagicMock()
        client.query_arrow.return_value = arrow_data

        # Configure from_arrow to return a DataFrame with column information
        with patch("polars.from_arrow") as mock_from_arrow:
            df = pl.DataFrame(
                [
                    {"position": 1, "column_name": "token_address", "data_type": "String"},
                    {"position": 2, "column_name": "chain_id", "data_type": "UInt64"},
                    {"position": 3, "column_name": "dt", "data_type": "Date"},
                ]
            )
            mock_from_arrow.return_value = df

        mock.return_value = client
        yield mock


@pytest.fixture
def mock_init_data_access():
    """Mock the init_data_access function in markers.py."""
    with patch("op_analytics.transforms.markers.init_data_access") as mock:
        data_access = MagicMock()

        # Configure query_markers_with_filters to return an empty DataFrame
        data_access.query_markers_with_filters.return_value = pl.DataFrame(
            schema={"transform": pl.Utf8, "dt": pl.Date}
        )

        mock.return_value = data_access
        yield mock


@pytest.fixture
def mock_insert_oplabs():
    """Mock the insert_oplabs function in transform.py."""
    with patch("op_analytics.transforms.transform.insert_oplabs") as mock:
        mock.return_value = {"status": "success", "written_rows": 1}
        yield mock


@pytest.fixture
def mock_export_to_bigquery():
    """Mock the export_to_bigquery function."""
    with patch("op_analytics.transforms.transform.export_to_bigquery") as mock:
        mock.return_value = {"status": "success"}
        yield mock


@pytest.fixture
def mock_read_steps():
    """Mock the read_steps function to return predefined steps."""
    with patch("op_analytics.transforms.transform.read_steps") as mock:
        # Create mock DDL objects
        ddl1 = MagicMock()
        ddl1.basename = "01_dim_erc20_first_seen_v1"
        ddl1.statement = "INSERT INTO _placeholder_ SELECT * FROM source_table WHERE dt = {dtparam}"

        ddl2 = MagicMock()
        ddl2.basename = "02_fact_erc20_oft_transfers_v1"
        ddl2.statement = "SELECT * FROM source_table WHERE dt = {dtparam}"

        # Create steps
        steps = [
            Step(
                index=1,
                db="transforms_interop",
                table_name="dim_erc20_first_seen_v1",
                ddl=ddl1,
                step_type=StepType.DIM,
            ),
            Step(
                index=2,
                db="transforms_interop",
                table_name="fact_erc20_oft_transfers_v1",
                ddl=ddl2,
                step_type=StepType.FACT,
            ),
        ]

        mock.return_value = steps
        yield mock


def test_execute_dt_transforms(
    mock_create_tables,
    mock_existing_markers,
    mock_transform_task,
    mock_new_stateful_client,
    mock_init_data_access,
    mock_insert_oplabs,
    mock_export_to_bigquery,
    mock_read_steps,
):
    """Test the execute_dt_transforms function with the interop group."""
    # Execute the function with test parameters
    result = execute_dt_transforms(
        group_name="interop",
        range_spec="2023-01-01:2023-01-03",  # 3-day range
        update_only=None,
        raise_if_empty=True,
        force_complete=False,
    )

    # Verify that create_tables was called with the correct group_name
    mock_create_tables.assert_called_once_with(group_name="interop")

    # Verify that existing_markers was called with the correct parameters
    mock_existing_markers.assert_called_once()
    args, kwargs = mock_existing_markers.call_args
    assert kwargs["transforms"] == ["interop"]
    assert isinstance(kwargs["date_range"], DateRange)
    assert kwargs["date_range"].min == date(2023, 1, 1)
    assert kwargs["date_range"].max == date(2023, 1, 3)

    # Verify that TransformTask was instantiated correctly for each date
    assert mock_transform_task.call_count == 3  # One for each day in the range

    # Check the parameters for the first call
    args, kwargs = mock_transform_task.call_args_list[0]
    assert kwargs["group_name"] == "interop"
    assert kwargs["dt"] == date(2023, 1, 1)
    assert kwargs["tables"] == mock_create_tables.return_value
    assert kwargs["update_only"] is None
    assert kwargs["raise_if_empty"] is True

    # Verify that execute was called on each task instance
    task_instance = mock_transform_task.return_value
    assert task_instance.execute.call_count == 3

    # Verify the result contains entries for each date
    assert len(result) == 3
    assert "2023-01-01" in result
    assert "2023-01-02" in result
    assert "2023-01-03" in result
    assert result["2023-01-01"] == {"status": "success", "rows_written": 100}


def test_execute_dt_transforms_with_update_only(
    mock_create_tables,
    mock_existing_markers,
    mock_transform_task,
    mock_new_stateful_client,
    mock_init_data_access,
    mock_insert_oplabs,
    mock_export_to_bigquery,
    mock_read_steps,
):
    """Test the execute_dt_transforms function with update_only parameter."""
    # Execute the function with update_only parameter
    result = execute_dt_transforms(
        group_name="interop",
        range_spec="2023-01-01:2023-01-03",
        update_only=[1],  # Only update index 1
        raise_if_empty=True,
        force_complete=False,
    )

    # Verify that TransformTask was instantiated with the correct update_only parameter
    assert mock_transform_task.call_count == 3

    # Check the parameters for the first call
    args, kwargs = mock_transform_task.call_args_list[0]
    assert kwargs["update_only"] == [1]

    # Verify the result
    assert len(result) == 3
    assert "2023-01-01" in result


def test_execute_dt_transforms_with_force_complete(
    mock_create_tables,
    mock_existing_markers,
    mock_transform_task,
    mock_new_stateful_client,
    mock_init_data_access,
    mock_insert_oplabs,
    mock_export_to_bigquery,
    mock_read_steps,
):
    """Test the execute_dt_transforms function with force_complete parameter."""
    # Add some existing markers
    existing_df = pl.DataFrame(
        [
            {"transform": "interop", "dt": date(2023, 1, 1)},
            {"transform": "interop", "dt": date(2023, 1, 2)},
        ]
    )
    mock_existing_markers.return_value = existing_df

    # Execute the function with force_complete=True
    result = execute_dt_transforms(
        group_name="interop",
        range_spec="2023-01-01:2023-01-03",
        update_only=None,
        raise_if_empty=True,
        force_complete=True,  # Force complete even if markers exist
    )

    # Verify that TransformTask was instantiated for all dates despite existing markers
    assert mock_transform_task.call_count == 3

    # Verify the result
    assert len(result) == 3
    assert "2023-01-01" in result
    assert "2023-01-02" in result
    assert "2023-01-03" in result


def test_execute_dt_transforms_with_max_tasks(
    mock_create_tables,
    mock_existing_markers,
    mock_transform_task,
    mock_new_stateful_client,
    mock_init_data_access,
    mock_insert_oplabs,
    mock_export_to_bigquery,
    mock_read_steps,
):
    """Test the execute_dt_transforms function with max_tasks parameter."""
    # Execute the function with max_tasks=1
    result = execute_dt_transforms(
        group_name="interop",
        range_spec="2023-01-01:2023-01-03",
        update_only=None,
        raise_if_empty=True,
        force_complete=False,
        max_tasks=1,  # Only process one task
    )

    # Verify that only one TransformTask was executed
    assert mock_transform_task.call_count == 3  # Still instantiated for all dates
    task_instance = mock_transform_task.return_value
    assert task_instance.execute.call_count == 1  # But only one was executed

    # Verify the result only contains one date
    assert len(result) == 1
    assert "2023-01-01" in result


def test_execute_dt_transforms_with_no_written_rows(
    mock_create_tables,
    mock_existing_markers,
    mock_transform_task,
    mock_new_stateful_client,
    mock_init_data_access,
    mock_insert_oplabs,
    mock_export_to_bigquery,
    mock_read_steps,
):
    """Test the execute_dt_transforms function when NoWrittenRows is raised."""
    # Configure the mock to raise NoWrittenRows for the second date
    task_instance = mock_transform_task.return_value

    def execute_side_effect(*args, **kwargs):
        # Get the dt from the mock call
        dt = mock_transform_task.call_args[1]["dt"]
        if dt == date(2023, 1, 2):
            raise NoWrittenRows(f"no written rows dt={dt}")
        return {"status": "success", "rows_written": 100}

    task_instance.execute.side_effect = execute_side_effect

    # Execute the function
    with pytest.raises(NoWrittenRows):
        execute_dt_transforms(
            group_name="interop",
            range_spec="2023-01-01:2023-01-03",
            update_only=None,
            raise_if_empty=True,
            force_complete=False,
        )

    # Verify that execution stopped after the error
    assert task_instance.execute.call_count == 2  # Only first and second dates were processed


def test_execute_dt_transforms_with_existing_markers(
    mock_create_tables,
    mock_existing_markers,
    mock_transform_task,
    mock_new_stateful_client,
    mock_init_data_access,
    mock_insert_oplabs,
    mock_export_to_bigquery,
    mock_read_steps,
):
    """Test the execute_dt_transforms function with existing markers."""
    # Add some existing markers
    existing_df = pl.DataFrame(
        [
            {"transform": "interop", "dt": date(2023, 1, 1)},
            {"transform": "interop", "dt": date(2023, 1, 2)},
        ]
    )
    mock_existing_markers.return_value = existing_df

    # Execute the function
    result = execute_dt_transforms(
        group_name="interop",
        range_spec="2023-01-01:2023-01-03",
        update_only=None,
        raise_if_empty=True,
        force_complete=False,
    )

    # Verify that TransformTask was only instantiated for dates without markers
    assert mock_transform_task.call_count == 1  # Only for 2023-01-03

    # Check the parameters for the call
    args, kwargs = mock_transform_task.call_args
    assert kwargs["dt"] == date(2023, 1, 3)

    # Verify the result only contains one date
    assert len(result) == 1
    assert "2023-01-03" in result


def test_execute_dt_transforms_with_no_written_rows_current_date(
    mock_create_tables,
    mock_existing_markers,
    mock_transform_task,
    mock_new_stateful_client,
    mock_init_data_access,
    mock_insert_oplabs,
    mock_export_to_bigquery,
    mock_read_steps,
):
    """Test the execute_dt_transforms function when NoWrittenRows is raised on the current date."""
    # Configure the mock to raise NoWrittenRows for the current date
    task_instance = mock_transform_task.return_value

    # Mock the now_date function to return a specific date
    with patch("op_analytics.transforms.main.now_date") as mock_now_date:
        mock_now_date.return_value = date(2023, 1, 3)

        def execute_side_effect(*args, **kwargs):
            # Get the dt from the mock call
            dt = mock_transform_task.call_args[1]["dt"]
            if dt == date(2023, 1, 3):  # Current date
                raise NoWrittenRows(f"no written rows dt={dt}")
            return {"status": "success", "rows_written": 100}

        task_instance.execute.side_effect = execute_side_effect

        # Execute the function - should not raise an exception for current date
        result = execute_dt_transforms(
            group_name="interop",
            range_spec="2023-01-01:2023-01-03",
            update_only=None,
            raise_if_empty=True,
            force_complete=False,
        )

        # Verify that execution processed the first two dates successfully
        assert task_instance.execute.call_count == 3

        # Verify the result contains entries for the first two dates
        assert len(result) == 3
        assert "2023-01-01" in result
        assert "2023-01-02" in result
        assert "2023-01-03" in result

        # Verify the error message is in the result for the current date
        assert "error" in result["2023-01-03"]
        assert "no written rows" in result["2023-01-03"]["error"].lower()
