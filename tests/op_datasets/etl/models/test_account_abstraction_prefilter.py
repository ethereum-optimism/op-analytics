import datetime
from datetime import date

from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.models.compute.testutils import ModelTestBase


class TestAccountAbstractionPrefilter0001(ModelTestBase):
    model = "account_abstraction_prefilter"
    inputdata = InputTestData.at(__file__)
    chains = ["base"]
    dateval = date(2024, 9, 17)
    block_filters = [
        "{block_number} IN (19910194) OR block_number % 100 < 1",
    ]

    _enable_fetching = False

    def test_overall_totals(self):
        assert self._duckdb_context is not None

        num_logs = (
            self._duckdb_context.client.sql("SELECT COUNT(*) as num_logs FROM entrypoint_logs_v1")
            .pl()
            .to_dicts()[0]["num_logs"]
        )

        num_traces = (
            self._duckdb_context.client.sql(
                "SELECT COUNT(*) as num_traces FROM entrypoint_traces_v1"
            )
            .pl()
            .to_dicts()[0]["num_traces"]
        )

        assert num_logs == 71
        assert num_traces == 520

    def test_model_schema_logs(self):
        assert self._duckdb_context is not None

        schema = (
            self._duckdb_context.client.sql("DESCRIBE entrypoint_logs_v1")
            .pl()
            .select("column_name", "column_type")
            .to_dicts()
        )
        actual_schema = {row["column_name"]: row["column_type"] for row in schema}

        assert actual_schema == {
            "dt": "DATE",
            "chain": "VARCHAR",
            "chain_id": "INTEGER",
            "network": "VARCHAR",
            "block_timestamp": "UINTEGER",
            "block_number": "BIGINT",
            "block_hash": "VARCHAR",
            "transaction_hash": "VARCHAR",
            "transaction_index": "BIGINT",
            "log_index": "BIGINT",
            "contract_address": "VARCHAR",
            "topic0": "VARCHAR",
            "indexed_args": "VARCHAR[]",
            "data": "VARCHAR",
        }

    def test_model_schema_traces(self):
        assert self._duckdb_context is not None

        schema = (
            self._duckdb_context.client.sql("DESCRIBE entrypoint_traces_v1")
            .pl()
            .select("column_name", "column_type")
            .to_dicts()
        )
        actual_schema = {row["column_name"]: row["column_type"] for row in schema}

        assert actual_schema == {
            "dt": "DATE",
            "chain": "VARCHAR",
            "chain_id": "INTEGER",
            "network": "VARCHAR",
            "block_timestamp": "UINTEGER",
            "block_number": "BIGINT",
            "block_hash": "VARCHAR",
            "transaction_hash": "VARCHAR",
            "transaction_index": "BIGINT",
            "from_address": "VARCHAR",
            "to_address": "VARCHAR",
            "value_lossless": "VARCHAR",
            "input": "VARCHAR",
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
            "trace_root": "INTEGER",
            "method_id": "VARCHAR",
        }

    def test_single_log(self):
        assert self._duckdb_context is not None

        output = (
            self._duckdb_context.client.sql("""
        SELECT * FROM entrypoint_logs_v1
        WHERE transaction_hash = '0x544a2d3e241baa7d25c4a0c7123b59ae2aac9f5bb8f67dbe007eec63e6f7d9ab'
        """)
            .pl()
            .to_dicts()
        )

        assert output == [
            {
                "dt": datetime.date(2024, 9, 17),
                "chain": "base",
                "chain_id": 8453,
                "network": "mainnet",
                "block_timestamp": 1726531347,
                "block_number": 19871000,
                "block_hash": "0xaa58ff59b7d4cdb11e0730dbb29001cec59dda83d8e474957df0d158e4c29450",
                "transaction_hash": "0x544a2d3e241baa7d25c4a0c7123b59ae2aac9f5bb8f67dbe007eec63e6f7d9ab",
                "transaction_index": 56,
                "log_index": 128,
                "contract_address": "0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789",
                "topic0": "0x2da466a7b24304f47e87fa2e1e5a81b9831ce54fec19055ce277ca2f39ba42c4",
                "indexed_args": [
                    "0x00000000000000000000000004b0aa67c3acbac1357f330236c3d5148dfeb52b"
                ],
                "data": "0x0000000000000000000000000000000000000000000000000000015f6ded2c4f",
            },
            {
                "dt": datetime.date(2024, 9, 17),
                "chain": "base",
                "chain_id": 8453,
                "network": "mainnet",
                "block_timestamp": 1726531347,
                "block_number": 19871000,
                "block_hash": "0xaa58ff59b7d4cdb11e0730dbb29001cec59dda83d8e474957df0d158e4c29450",
                "transaction_hash": "0x544a2d3e241baa7d25c4a0c7123b59ae2aac9f5bb8f67dbe007eec63e6f7d9ab",
                "transaction_index": 56,
                "log_index": 129,
                "contract_address": "0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789",
                "topic0": "0xbb47ee3e183a558b1a2ff0874b079f3fc5478b7454eacf2bfc5af2ff5878f972",
                "indexed_args": [],
                "data": "0x",
            },
            {
                "dt": datetime.date(2024, 9, 17),
                "chain": "base",
                "chain_id": 8453,
                "network": "mainnet",
                "block_timestamp": 1726531347,
                "block_number": 19871000,
                "block_hash": "0xaa58ff59b7d4cdb11e0730dbb29001cec59dda83d8e474957df0d158e4c29450",
                "transaction_hash": "0x544a2d3e241baa7d25c4a0c7123b59ae2aac9f5bb8f67dbe007eec63e6f7d9ab",
                "transaction_index": 56,
                "log_index": 132,
                "contract_address": "0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789",
                "topic0": "0x49628fd1471006c1482da88028e9ce4dbb080b815c9b0344d39e5a8e6ec1419f",
                "indexed_args": [
                    "0xc9d73a4820ad831ed01b15b8d38fd944e32abb42c010999d6b2acc1349dfaa60",
                    "0x00000000000000000000000004b0aa67c3acbac1357f330236c3d5148dfeb52b",
                    "0x0000000000000000000000000000000000000000000000000000000000000000",
                ],
                "data": "0x00000000000000000000000000000000000000000000000000000000000000390000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000001185547852e0000000000000000000000000000000000000000000000000000000000036f92",
            },
        ]

    def test_single_trace(self):
        assert self._duckdb_context is not None

        output = (
            self._duckdb_context.client.sql("""
        SELECT * FROM entrypoint_traces_v1 WHERE transaction_hash = '0xeb8aed49895870a10eaee7fc6b38d00e6081816e1c7309fd6829f9299a386b58'
        AND trace_address = '0,0,0'
        """)
            .pl()
            .to_dicts()
        )

        assert output == [
            {
                "dt": datetime.date(2024, 9, 17),
                "chain": "base",
                "chain_id": 8453,
                "network": "mainnet",
                "block_timestamp": 1726531547,
                "block_number": 19871100,
                "block_hash": "0x2070ae0725b59739488aaa77096d55c616d366ec8c021700a189f6cd14e4162b",
                "transaction_hash": "0xeb8aed49895870a10eaee7fc6b38d00e6081816e1c7309fd6829f9299a386b58",
                "transaction_index": 40,
                "from_address": "0x118bd3dc35b5ff4d5a20b8f48b1824f471ebc563",
                "to_address": "0x0000000000000000000000000000000000000001",
                "value_lossless": "0",
                "input": "0x993caabad1df1a65f945f093ff92472016acdb9868cc75a9f24d3b98791fb877000000000000000000000000000000000000000000000000000000000000001ca634c62a702e7b96eb7d32633588f629d4ec97495110521b8a0d9d74fc38a6864434f0bfe9ea547d5f8253f4e35580275d4b065b350a2f134c90de835e35bab0",
                "output": "0x000000000000000000000000d61fc44452aa68e61f45dcc895a4079ad6f3e9aa",
                "trace_type": "call",
                "call_type": "staticcall",
                "reward_type": "",
                "gas": 36257,
                "gas_used": 3000,
                "subtraces": 0,
                "trace_address": "0,0,0",
                "error": "",
                "status": 1,
                "trace_root": 0,
                "method_id": "0x993caaba",
            }
        ]

    def test_log_counts(self):
        assert self._duckdb_context is not None

        output = (
            self._duckdb_context.client.sql("""
        SELECT transaction_hash, count(*) as num_logs FROM entrypoint_logs_v1
        GROUP BY 1
        ORDER BY 1
        LIMIT 10
        """)
            .pl()
            .to_dicts()
        )

        assert output == [
            {
                "transaction_hash": "0x075826972d9ef5dcdaa68fe1d2aef7dae1114f83359b56e6df67b56e232853c1",
                "num_logs": 2,
            },
            {
                "transaction_hash": "0x20d2ef78248fd4ab1d024e2a0aa21bb233a8af984eb7136405d2ab04d759ec89",
                "num_logs": 2,
            },
            {
                "transaction_hash": "0x3de6952ace7f1cff6107320aaa890a77d49027d74692402218ab27fa3cb0accc",
                "num_logs": 9,
            },
            {
                "transaction_hash": "0x4340931bc3502cdb912f728da53b922d8eb41709953f8985883f5b483cc85843",
                "num_logs": 2,
            },
            {
                "transaction_hash": "0x475f4b962872405fe53605978c43ce02e06accb0f9f58eaac509213c77e6a637",
                "num_logs": 2,
            },
            {
                "transaction_hash": "0x54025a3db9df5a8792d175deecbf010223ea9c39affdc8ea14cefde7c48a4243",
                "num_logs": 2,
            },
            {
                "transaction_hash": "0x544a2d3e241baa7d25c4a0c7123b59ae2aac9f5bb8f67dbe007eec63e6f7d9ab",
                "num_logs": 3,
            },
            {
                "transaction_hash": "0x569d42f98e509891bcbfa0f23b2fa1b8dfad1faa8d4622db7c7abba587340519",
                "num_logs": 2,
            },
            {
                "transaction_hash": "0x6b00c73a44cd2c6e3f1cfd7a36176b88cad29d15a3b38eca60f21589ae977a0f",
                "num_logs": 2,
            },
            {
                "transaction_hash": "0x7298c85972572a17b5256c891df2b335012778df093bb80e62c1e08db4eab1ef",
                "num_logs": 2,
            },
        ]
