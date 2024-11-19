import os
import shutil
import tempfile
import unittest
from dataclasses import dataclass
from datetime import date
from textwrap import dedent

import duckdb

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.testutils.inputdata import InputTestData

from .construct import construct_tasks
from .registry import (
    REGISTERED_INTERMEDIATE_MODELS,
    PythonModel,
    PythonModelExecutor,
    load_model_definitions,
    ModelInputDataReader,
)
from .udfs import create_duckdb_macros

log = structlog.get_logger()


@dataclass
class DataReaderTestUtil:
    client: duckdb.DuckDBPyConnection

    def duckdb_relation(self, dataset) -> duckdb.DuckDBPyRelation:
        assert self.client is not None
        return self.client.sql(f"SELECT * FROM input_data_{dataset}")


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

    # The path where input data will be stored.
    inputdata: InputTestData

    # Chains that should be included in the test data.
    chains: list[str]

    # Date that should be included in the test data.
    dateval: date

    # Datasets that should be included in the test data.
    datasets: list[str]

    # Specify blocks that should be included in the test data.
    block_filters: list[str]

    # Internal variables
    _enable_fetching = False
    _duckdb_client = None
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
        load_model_definitions()

        db_path = cls.inputdata.path(f"testdata/{cls.__name__}.duck.db")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        cls._duckdb_client = duckdb.connect(db_path)
        tables_exist = cls._tables_exist()

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
                cls._fetch_test_data()
                log.info("Fetched test data from GCS.")
        else:
            log.info(f"Using local test data from: {db_path}")

        cls._duckdb_client.close()

        # Make a copy of the duck.db file, to prevent changing the input test data.
        cls._tempdir = tempfile.TemporaryDirectory()
        tmp_db_path = os.path.join(cls._tempdir.name, os.path.basename(db_path))
        shutil.copyfile(db_path, tmp_db_path)
        cls._duckdb_client = duckdb.connect(tmp_db_path)

        # Execute the model on the temporary duckdb instance.
        model = REGISTERED_INTERMEDIATE_MODELS["daily_address_summary"]

        cls._model_executor = execute_model_in_memory(
            duckdb_client=cls._duckdb_client,
            model=model,
            data_reader=DataReaderTestUtil(cls._duckdb_client),
        )

    @classmethod
    def tearDownClass(cls) -> None:
        """Ensure duckb client is closed after running the test."""
        assert cls._model_executor is not None
        cls._model_executor.__exit__(None, None, None)

        assert cls._duckdb_client is not None
        cls._duckdb_client.close()

    @classmethod
    def input_table_name(self, dataset_name: str) -> str:
        return f"input_data_{dataset_name}"

    @classmethod
    def _tables_exist(cls) -> bool:
        """Helper function to check if the test database already contains the test data."""
        assert cls._duckdb_client is not None
        tables = (
            cls._duckdb_client.sql("SELECT table_name FROM duckdb_tables;")
            .df()["table_name"]
            .to_list()
        )
        for dataset in cls.datasets:
            if cls.input_table_name(dataset) not in tables:
                return False
        return True

    @classmethod
    def _fetch_test_data(cls):
        """Fetch test data from GCS and save it to the local duckdb."""
        datestr = cls.dateval.strftime("%Y%m%d")
        tasks = construct_tasks(
            chains=cls.chains,
            models=[],
            range_spec=f"@{datestr}:+1",
            read_from=DataLocation.GCS,
            write_to=[],
        )
        assert len(tasks) == 1
        task = tasks[0]

        relations = {}
        for dataset in cls.datasets:
            rel = task.data_reader.duckdb_relation(dataset)

            if dataset == "blocks":
                block_number_col = "number"
            else:
                block_number_col = "block_number"

            block_filter = " OR ".join(
                _.format(block_number=block_number_col) for _ in cls.block_filters
            )

            arrow_table = rel.filter(block_filter).to_arrow_table()  # noqa: F841
            table_name = cls.input_table_name(dataset)

            assert cls._duckdb_client is not None
            cls._duckdb_client.sql(f"CREATE TABLE {table_name} AS SELECT * FROM arrow_table")

            relations[dataset] = rel


def execute_model_in_memory(
    duckdb_client: duckdb.DuckDBPyConnection,
    model: PythonModel,
    data_reader: ModelInputDataReader,
):
    """Execute a model and register results as views."""
    log.info("Executing model...")
    create_duckdb_macros(duckdb_client)

    model_executor = PythonModelExecutor(
        model=model,
        client=duckdb_client,
        data_reader=data_reader,
    )

    model_executor.__enter__()
    model_results = model_executor.execute()

    print(model_results.keys())

    # Create views with the model results
    for name, relation in model_results.items():
        duckdb_client.register(
            view_name=name,
            python_object=relation.to_arrow_table(),
        )

    return model_executor
