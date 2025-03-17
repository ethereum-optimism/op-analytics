from datetime import date

from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.models.compute.testutils import ModelTestBase

CONTRACT_ADDRESS = "0xa16b2bc8053a320620a1ef33da325491ec064e4b"


class TestCreationTraces001(ModelTestBase):
    model = "contract_creation"
    inputdata = InputTestData.at(__file__)
    chains = ["op"]
    dateval = date(2024, 11, 18)
    block_filters = [
        "{block_number} % 10000 <= 2",
    ]

    _enable_fetching = False

    def test_overall_totals(self):
        assert self._duckdb_context is not None

        num_traces = (
            self._duckdb_context.client.sql(
                "SELECT COUNT(*) as num_traces FROM ingestion_traces_v1"
            )
            .pl()
            .to_dicts()[0]["num_traces"]
        )

        num_creation_traces = (
            self._duckdb_context.client.sql(
                "SELECT COUNT(*) AS num_creation_traces FROM create_traces_v1"
            )
            .pl()
            .to_dicts()[0]["num_creation_traces"]
        )

        num_unique_contracts = (
            self._duckdb_context.client.sql(
                "SELECT COUNT(DISTINCT contract_address) AS num_unique_contracts FROM create_traces_v1"
            )
            .pl()
            .to_dicts()[0]["num_unique_contracts"]
        )

        assert num_traces == 11496
        assert num_creation_traces == 3
        assert num_unique_contracts == num_creation_traces

        percent = round(100.0 * num_creation_traces / num_traces, 2)
        assert percent == 0.03

    def test_model_schema(self):
        assert self._duckdb_context is not None

        schema = (
            self._duckdb_context.client.sql("DESCRIBE create_traces_v1")
            .pl()
            .select("column_name", "column_type")
            .to_dicts()
        )
        actual_schema = {row["column_name"]: row["column_type"] for row in schema}

        assert actual_schema == {
            "network": "VARCHAR",
            "chain_id": "INTEGER",
            "chain": "VARCHAR",
            "dt": "DATE",
            "block_timestamp": "UINTEGER",
            "block_number": "BIGINT",
            "block_hash": "VARCHAR",
            "transaction_hash": "VARCHAR",
            "transaction_index": "BIGINT",
            "tr_from_address": "VARCHAR",
            "tx_from_address": "VARCHAR",
            "contract_address": "VARCHAR",
            "tx_to_address": "VARCHAR",
            "value_64": "BIGINT",
            "value_lossless": "VARCHAR",
            "code": "VARCHAR",
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
            "tx_method_id": "VARCHAR",
            "code_bytelength": "DOUBLE",
        }

    def test_single_tx(self):
        assert self._duckdb_context is not None

        output = (
            self._duckdb_context.client.sql(f"""
        SELECT * FROM create_traces_v1 WHERE contract_address = '{CONTRACT_ADDRESS}'
        """)
            .pl()
            .to_dicts()
        )

        assert output == [
            {
                "network": "mainnet",
                "chain_id": 10,
                "chain": "op",
                "dt": date(2024, 11, 18),
                "block_timestamp": 1731898779,
                "block_number": 128150001,
                "block_hash": "0xd7dfaaf7e5b8eb3b7b19094a3081ed97ca0120a920141062258d5c2375d8465f",
                "transaction_hash": "0x82f321eb3cd99319a5dc6f1ff0b0e64340d367a1f483ed053fe0d50ca8d4267d",
                "transaction_index": 6,
                "tr_from_address": "0x4e1dcf7ad4e460cfd30791ccc4f9c8a4f820ec67",
                "tx_from_address": "0xc97240c92596276b8b9366064123fd76a1207164",
                "contract_address": "0xa16b2bc8053a320620a1ef33da325491ec064e4b",
                "tx_to_address": "0x24f6f36a551fe6008fa80afcff1d6ace182ead2b",
                "value_64": 0,
                "value_lossless": "0",
                "code": "0x608060405234801561001057600080fd5b506040516101e63803806101e68339818101604052602081101561003357600080fd5b8101908080519060200190929190505050600073ffffffffffffffffffffffffffffffffffffffff168173ffffffffffffffffffffffffffffffffffffffff1614156100ca576040517f08c379a00000000000000000000000000000000000000000000000000000000081526004018080602001828103825260228152602001806101c46022913960400191505060405180910390fd5b806000806101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff1602179055505060ab806101196000396000f3fe608060405273ffffffffffffffffffffffffffffffffffffffff600054167fa619486e0000000000000000000000000000000000000000000000000000000060003514156050578060005260206000f35b3660008037600080366000845af43d6000803e60008114156070573d6000fd5b3d6000f3fea264697066735822122003d1488ee65e08fa41e58e888a9865554c535f2c77126a82cb4c0f917f31441364736f6c63430007060033496e76616c69642073696e676c65746f6e20616464726573732070726f766964656400000000000000000000000029fcb43b46531bca003ddc8fcb67ffe91900c762",
                "output": "0x608060405273ffffffffffffffffffffffffffffffffffffffff600054167fa619486e0000000000000000000000000000000000000000000000000000000060003514156050578060005260206000f35b3660008037600080366000845af43d6000803e60008114156070573d6000fd5b3d6000f3fea264697066735822122003d1488ee65e08fa41e58e888a9865554c535f2c77126a82cb4c0f917f31441364736f6c63430007060033",
                "trace_type": "create2",
                "call_type": "",
                "reward_type": "",
                "gas": 258177,
                "gas_used": 56609,
                "subtraces": 0,
                "trace_address": "0,0",
                "error": "",
                "status": 1,
                "tx_method_id": "0x540817d7",
                "code_bytelength": 518.0,
            }
        ]
