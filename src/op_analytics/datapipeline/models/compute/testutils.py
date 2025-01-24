import os
import shutil
import tempfile
import unittest
from dataclasses import dataclass
from datetime import date
from textwrap import dedent
from unittest.mock import patch

import duckdb
from overrides import override

from op_analytics.coreutils.duckdb_inmem.client import (
    DuckDBContext,
    ParquetData,
    register_dataset_relation,
    init_client,
)
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.models.compute.execute import (
    ModelDataReader,
    PythonModel,
    PythonModelExecutor,
)

from .udfs import create_duckdb_macros

log = structlog.get_logger()


def input_table_name(dataset_name: str) -> str:
    valid_table_name = dataset_name.replace("/", "_")
    return f"input_data_{valid_table_name}"


@dataclass
class MockParquetData(ParquetData):
    context: DuckDBContext
    dataset: str

    @property
    def sanitized_name(self):
        return input_table_name(self.dataset)

    @override
    def duckdb_ctx(self) -> DuckDBContext:
        return self.context

    @override
    def data_subquery(self) -> str:
        assert self.context is not None
        rel = self.context.client.sql(f"SELECT * FROM {input_table_name(self.dataset)}")
        registered_name = register_dataset_relation(self.context.client, self.dataset, rel)
        return registered_name


@dataclass
class DataReaderTestUtil:
    context: DuckDBContext

    def remote_parquet(
        self,
        dataset: str,
        first_n_parquet_files: int | None = None,
    ) -> MockParquetData:
        """Return a parquet data object for the given dataset."""
        return MockParquetData(context=self.context, dataset=dataset)


class IntermediateModelTestBase(unittest.TestCase):
    """Base Class for Intermediate Model Unit Tests.

    This class helps with fetching and locally storing sample data for use in intermediate
    model unit tests.

    The test data is stored in a local duckdb file. If the file does not exist the data is
    fetched from GCS and stored so it can be used by subsequent runs.

    Fetching is disabled by default. To enable fetching set `_enable_fetching = True` on
    the child class. This should be done explicitly when you wish to fetch data for the
    first time or when you want to update the existing test data.

    Users of the class must supply a "block_filters" to narrow down the input data to
    a set of blocks. This helps control the test data file size.
    """

    # Input parameters (must be set by child class)

    # The name of the model under test
    model: str

    # The path where input data will be stored.
    inputdata: InputTestData

    # Chains that should be included in the test data.
    chains: list[str]

    # Date that should be included in the test data.
    dateval: date

    # Specify blocks that should be included in the test data.
    block_filters: list[str]

    # Internal variables
    _enable_fetching = False
    _duckdb_context = None
    _tempdir = None
    _model_executor = None

    @classmethod
    def setUpClass(cls):
        """Set up the test case.

        If the duckdb file does not exist yet this method fetches the data from GCS and
        stores it locally.

        This method executes the model under test and creates temporary tables in DuckDB
        with the model results.
        """

        # Execute the model on the temporary duckdb instance.
        model = PythonModel.get(cls.model)

        db_file_name = f"{cls.__name__}.duck.db"
        db_path = cls.inputdata.path(f"testdata/{db_file_name}")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        cls._duckdb_context = DuckDBContext(
            client=duckdb.connect(db_path),
            dir_name=os.path.dirname(db_path),
            db_file_name=db_file_name,
        )
        cls._duckdb_context.connect_to_gcs()

        tables_exist = cls._tables_exist(datasets=model.input_datasets)

        if not tables_exist:
            if not cls._enable_fetching:
                raise RuntimeError(
                    dedent(
                        """Intermediate Model Test Utils Error:
                    
                    - Input test data has not been fetched yet.
                    
                    To resolve this error manually set `_enable_fetching = True`
                    on your test class.
                    """
                    )
                )

            else:
                log.info("Fetching test data from GCS.")

                # We patch the markers database to use the real production database.
                # This allows us to fetch test data straight from production.
                with patch(
                    "op_analytics.coreutils.partitioned.markers_clickhouse.etl_monitor_markers_database",
                    lambda: "etl_monitor",
                ):
                    cls._fetch_test_data(model.input_datasets)
                log.info("Fetched test data from GCS.")
        else:
            log.info(f"Using local test data from: {db_path}")

        cls._duckdb_context.close()

        # Make a copy of the duck.db file, to prevent changing the input test data.
        cls._tempdir = tempfile.TemporaryDirectory()
        tmp_db_path = os.path.join(cls._tempdir.name, os.path.basename(db_path))
        shutil.copyfile(db_path, tmp_db_path)
        cls._duckdb_context = DuckDBContext(
            client=duckdb.connect(tmp_db_path),
            dir_name=os.path.dirname(tmp_db_path),
            db_file_name=os.path.basename(db_path),
        )

        cls._model_executor = execute_model_in_memory(
            duckdb_context=cls._duckdb_context,
            model=cls.model,
            data_reader=DataReaderTestUtil(cls._duckdb_context),
        )

    @classmethod
    def tearDownClass(cls) -> None:
        """Ensure duckb client is closed after running the test."""
        assert cls._model_executor is not None
        cls._model_executor.__exit__(None, None, None)

        assert cls._duckdb_context is not None
        cls._duckdb_context.close()

    @classmethod
    def _tables_exist(cls, datasets: list[str]) -> bool:
        """Helper function to check if the test database already contains the test data."""
        assert cls._duckdb_context is not None
        tables = (
            cls._duckdb_context.client.sql("SELECT table_name FROM duckdb_tables;")
            .df()["table_name"]
            .to_list()
        )
        for dataset in datasets:
            if input_table_name(dataset) not in tables:
                return False
        return True

    @classmethod
    def _fetch_test_data(cls, datasets: list[str]):
        """Fetch test data from GCS and save it to the local duckdb."""
        datestr = cls.dateval.strftime("%Y%m%d")

        from op_analytics.datapipeline.etl.blockbatch.construct import construct_tasks

        tasks = construct_tasks(
            chains=cls.chains,
            models=[cls.model],
            range_spec=f"@{datestr}:+1",
            read_from=DataLocation.GCS,
            write_to=DataLocation.GCS,
        )
        assert len(tasks) > 0
        task = tasks[0]

        relations = {}
        for dataset in datasets:
            assert cls._duckdb_context is not None

            # Prepare the WHERE clause to filter blocks.
            if "blocks" in dataset:
                block_number_col = "number"
            else:
                block_number_col = "block_number"

            block_filter = " OR ".join(
                _.format(block_number=block_number_col) for _ in cls.block_filters
            )

            # Get SELECT statement for the remote parquet data.
            select = task.data_reader.remote_parquet(dataset).select_string(
                additional_sql="WHERE " + block_filter
            )

            # Execute SELECT and convert to arrow.
            rel = cls._duckdb_context.client.sql(select)
            arrow_table = rel.to_arrow_table()  # noqa: F841
            table_name = input_table_name(dataset)

            assert cls._duckdb_context is not None
            cls._duckdb_context.client.sql(
                f"CREATE TABLE {table_name} AS SELECT * FROM arrow_table"
            )

            relations[dataset] = rel


def execute_model_in_memory(
    duckdb_context: DuckDBContext,
    model: str,
    data_reader: ModelDataReader,
    limit_input_parquet_files: int | None = None,
):
    """Execute a model and register results as views."""
    log.info("Executing model function...")

    model_obj = PythonModel.get(model)

    create_duckdb_macros(duckdb_context)

    model_executor = PythonModelExecutor(
        model=model_obj,
        duckdb_context=duckdb_context,
        data_reader=data_reader,
        limit_input_parquet_files=limit_input_parquet_files,
    )

    model_executor.__enter__()
    model_results = model_executor.execute()

    print(model_results.keys())

    # Create views with the model results
    for name, relation in model_results.items():
        duckdb_context.client.register(
            view_name=name,
            python_object=duckdb_context.relation_to_arrow(relation),
        )

    return model_executor


def setup_execution_context(model_name: str, data_reader: DataReader):
    # Initialize duckdb.
    ctx = init_client()
    create_duckdb_macros(ctx)

    # Initialize model execution context.
    model_obj = PythonModel.get(model_name)
    model_executor = PythonModelExecutor(
        model=model_obj,
        duckdb_context=ctx,
        data_reader=data_reader,
        limit_input_parquet_files=1,
    )

    # Enter the context and grab the handles we need to manipulate data.
    model_executor.__enter__()
    ctx, input_datasets, aux_templates = model_executor.call_args()

    # Show the names of input datasets and views that are used by the model.
    print()
    for _ in input_datasets:
        print(f"INPUT: {_}")

    print()
    for _ in aux_templates:
        print(f"AUX VIEW: {_}")

    return ctx, input_datasets, aux_templates
