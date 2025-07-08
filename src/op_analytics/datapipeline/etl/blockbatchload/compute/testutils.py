import os
import shutil
import tempfile
import unittest
from textwrap import dedent
from unittest.mock import patch

import duckdb
from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext
from op_analytics.coreutils.logger import structlog
from op_analytics.datapipeline.etl.blockbatchload.main import load_to_clickhouse

log = structlog.get_logger()


class BlockBatchTestBase(unittest.TestCase):
    """Base class for blockbatchload pipeline integration tests.

    This class helps with fetching and locally storing sample data for use in blockbatchload
    pipeline integration tests.

    The test data is stored in a local duckdb file. If the file does not exist the data is
    fetched from GCS and stored so it can be used by subsequent runs.

    Fetching is disabled by default. To enable fetching set `_enable_fetching = True` on
    the child class. This should be done explicitly when you wish to fetch data for the
    first time or when you want to update the existing test data.

    Users of the class must supply "block_filters" to narrow down the input data to
    a set of blocks. This helps control the test data file size.
    """

    # Input parameters (must be set by child class)
    dataset = None  # The blockbatchload dataset spec (e.g. REVSHARE_TRANSFERS)
    chains: list[str] = []  # List of chains to test
    target_range = None  # date or int
    block_filters: list[str] = []  # List of block filter SQL strings
    inputdata = None  # InputTestData.at(__file__)

    # Internal variables
    _enable_fetching = False
    _duckdb_context = None
    _tempdir = None

    @classmethod
    def setUpClass(cls):
        """Set up the test case.

        If the duckdb file does not exist yet this method fetches the data from GCS and
        stores it locally.

        This method executes the blockbatchload pipeline and creates temporary tables in DuckDB
        with the pipeline results.
        """
        if cls.dataset is None or cls.inputdata is None:
            raise RuntimeError(
                "BlockBatchTestBase: dataset and inputdata must be set on the test class."
            )

        db_file_name = f"{cls.__name__}.duck.db"
        db_path = cls.inputdata.path(f"testdata/{db_file_name}")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        cls._duckdb_context = DuckDBContext(
            client=duckdb.connect(db_path),
            dir_name=os.path.dirname(db_path),
            db_file_name=db_file_name,
        )

        tables_exist = cls._tables_exist()

        if not tables_exist:
            if not cls._enable_fetching:
                raise RuntimeError(
                    dedent(
                        """BlockBatchTestBase Error:
                        - Input test data has not been fetched yet.
                        To resolve this error manually set `_enable_fetching = True` on your test class.
                        """
                    )
                )
            else:
                log.info("Fetching blockbatch test data from GCS.")
                cls._duckdb_context.connect_to_gcs()

                # We patch the markers database to use the real production database.
                # This allows us to fetch test data straight from production.
                with patch(
                    "op_analytics.coreutils.partitioned.markers_clickhouse.etl_monitor_markers_database",
                    lambda: "etl_monitor",
                ):
                    # Use the blockbatchload loader to fetch and store test data
                    for chain in cls.chains:
                        load_to_clickhouse(
                            dataset=cls.dataset,
                            range_spec=cls.target_range,
                            dry_run=False,
                        )
                log.info("Fetched blockbatch test data from GCS.")
        else:
            log.info(f"Using local blockbatch test data from: {db_path}")

        cls._duckdb_context.close(remove_db_path=False)

        # Make a copy of the duck.db file, to prevent changing the input test data.
        cls._tempdir = tempfile.TemporaryDirectory()
        tmp_db_path = os.path.join(cls._tempdir.name, os.path.basename(db_path))
        shutil.copyfile(db_path, tmp_db_path)
        cls._duckdb_context = DuckDBContext(
            client=duckdb.connect(tmp_db_path),
            dir_name=os.path.dirname(tmp_db_path),
            db_file_name=os.path.basename(db_path),
        )

    @classmethod
    def tearDownClass(cls) -> None:
        """Ensure duckdb client is closed after running the test."""
        if cls._duckdb_context is not None:
            cls._duckdb_context.close(remove_db_path=False)
        if cls._tempdir is not None:
            cls._tempdir.cleanup()

    @classmethod
    def _tables_exist(cls) -> bool:
        """Helper function to check if the test database already contains the test data."""
        assert cls._duckdb_context is not None
        try:
            tables = (
                cls._duckdb_context.client.sql("SELECT table_name FROM duckdb_tables;")
                .df()["table_name"]
                .to_list()
            )
            # Check if the output table exists (table name derived from dataset)
            output_table = cls.dataset.output_table_name()
            return output_table in tables
        except Exception:
            return False

    def query_output_table(self, sql: str):
        """Query the output table with the given SQL."""
        assert self._duckdb_context is not None
        return self._duckdb_context.client.sql(sql).pl().to_dicts()
