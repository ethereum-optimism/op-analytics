import os
import tempfile

import duckdb
from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext
from op_analytics.datapipeline.models.compute.auxtemplate import AuxiliaryTemplate


def test_useroperationevent_without_innerhandleop_trace_is_ignored():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test.duck.db")
    ctx = DuckDBContext(client=duckdb.connect(db_path), dir_name=tmpdir, db_file_name="test.duck.db")

    ctx.client.sql(
        """
        CREATE TABLE account_abstraction__useroperationevent_logs (
            block_number BIGINT,
            transaction_hash VARCHAR,
            log_index BIGINT,
            sender VARCHAR,
            success BOOLEAN,
            actualGasUsed VARCHAR
        );
        """
    )
    ctx.client.sql(
        """
        CREATE TABLE account_abstraction__enriched_entrypoint_traces (
            block_number BIGINT,
            transaction_hash VARCHAR,
            userop_sender VARCHAR,
            from_address VARCHAR,
            is_from_sender BOOLEAN,
            is_innerhandleop BOOLEAN
        );
        """
    )

    # UserOp without innerHandleOp invocation (should be ignored)
    ctx.client.sql(
        """
        INSERT INTO account_abstraction__useroperationevent_logs VALUES
            (1, '0x1', 0, '0xaaa', FALSE, '0');
        """
    )

    # UserOp that invokes innerHandleOp but missing trace (should trigger error)
    ctx.client.sql(
        """
        INSERT INTO account_abstraction__useroperationevent_logs VALUES
            (2, '0x2', 0, '0xbbb', TRUE, '100');
        """
    )

    template = AuxiliaryTemplate("account_abstraction/data_quality_check_01")
    errors = template.run_as_data_quality_check(ctx)

    assert len(errors) == 1
    err = errors[0]
    assert err["block_number"] == 2
    assert err["transaction_hash"] == '0x2'

    ctx.close()
