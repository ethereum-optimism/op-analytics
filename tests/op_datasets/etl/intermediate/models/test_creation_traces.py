from datetime import date

from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.etl.intermediate.testutils import IntermediateModelTestBase


class TestCreationTraces001(IntermediateModelTestBase):
    model = "creation_traces"
    inputdata = InputTestData.at(__file__)
    chains = ["op"]
    dateval = date(2024, 11, 18)
    datasets = ["traces", "transactions"]
    block_filters = [
        "{block_number} % 10000 <= 2",
    ]

    _enable_fetching = False

    def test_overall_totals(self):
        assert self._duckdb_client is not None

        num_traces = (
            self._duckdb_client.sql("SELECT COUNT(*) as num_traces FROM traces")
            .pl()
            .to_dicts()[0]["num_traces"]
        )

        num_creation_traces = (
            self._duckdb_client.sql(
                "SELECT COUNT(*) AS num_creation_traces FROM creation_traces_v1"
            )
            .pl()
            .to_dicts()[0]["num_creation_traces"]
        )

        assert num_traces == 11496
        assert num_creation_traces == 3

        percent = round(100.0 * num_creation_traces / num_traces, 2)
        assert percent == 0.03

    def test_model_schema(self):
        assert self._duckdb_client is not None

        schema = (
            self._duckdb_client.sql("DESCRIBE creation_traces_v1")
            .pl()
            .select("column_name", "column_type")
            .to_dicts()
        )
        actual_schema = {row["column_name"]: row["column_type"] for row in schema}

        assert actual_schema == {
            "network": "VARCHAR",
            "chain_id": "INTEGER",
            "chain": "VARCHAR",
            "dt": "VARCHAR",
            "created_timestamp": "UINTEGER",
            "created_block_number": "BIGINT",
            "created_block_hash": "VARCHAR",
            "created_tx_hash": "VARCHAR",
            "created_tx_index": "BIGINT",
            "trace_creator_address": "VARCHAR",
            "created_tx_from": "VARCHAR",
            "contract_address": "VARCHAR",
            "created_tx_to": "VARCHAR",
            "created_tx_method_id": "VARCHAR",
            "value_64": "BIGINT",
            "value_lossless": "VARCHAR",
            "code": "VARCHAR",
            "code_bytelength": "DOUBLE",
            "output": "VARCHAR",
            "trace_type": "VARCHAR",
            "call_type": "VARCHAR",
            "reward_type": "VARCHAR",
            "gas": "BIGINT",
            "gas_used": "BIGINT",
            "subtraces": "BIGINT",
            "trace_address": "VARCHAR",
            "error": "VARCHAR",
            "status": "BIGINT",
        }
