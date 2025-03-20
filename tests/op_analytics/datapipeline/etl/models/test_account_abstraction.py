import datetime
from datetime import date

from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.models.compute.testutils import ModelTestBase


class TestAccountAbstraction0001(ModelTestBase):
    model = "account_abstraction"
    inputdata = InputTestData.at(__file__)
    chains = ["base"]
    target_range = date(2024, 9, 17)
    block_filters = [
        "{block_number} IN (19910194) OR block_number % 100 < 1",
    ]

    _enable_fetching = True

    def test_overall_totals(self):
        assert self._duckdb_context is not None

        num_logs = (
            self._duckdb_context.client.sql(
                "SELECT COUNT(*) as num_logs FROM useroperationevent_logs_v2"
            )
            .pl()
            .to_dicts()[0]["num_logs"]
        )

        num_traces = (
            self._duckdb_context.client.sql(
                "SELECT COUNT(*) as num_traces FROM enriched_entrypoint_traces_v2"
            )
            .pl()
            .to_dicts()[0]["num_traces"]
        )

        assert num_logs == 38
        assert num_traces == 319

    def test_model_schema_logs(self):
        assert self._duckdb_context is not None

        schema = (
            self._duckdb_context.client.sql("DESCRIBE useroperationevent_logs_v2")
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
            "userophash": "VARCHAR",
            "sender": "VARCHAR",
            "paymaster": "VARCHAR",
            "nonce": "VARCHAR",
            "success": "BOOLEAN",
            "actualGasCost": "VARCHAR",
            "actualGasUsed": "VARCHAR",
        }

    def test_model_schema_traces(self):
        assert self._duckdb_context is not None

        schema = (
            self._duckdb_context.client.sql("DESCRIBE enriched_entrypoint_traces_v2")
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
            "value": "VARCHAR",
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
            "tx_from_address": "VARCHAR",
            "bundler_address": "VARCHAR",
            "entrypoint_contract_address": "VARCHAR",
            "entrypoint_contract_version": "VARCHAR",
            "is_innerhandleop": "BOOLEAN",
            "is_from_sender": "BOOLEAN",
            "userop_sender": "VARCHAR",
            "userop_paymaster": "VARCHAR",
            "userop_hash": "VARCHAR",
            "userop_calldata": "VARCHAR",
            "innerhandleop_decodeerror": "VARCHAR",
            "innerhandleop_opinfo": "VARCHAR",
            "innerhandleop_context": "VARCHAR",
            "innerhandleop_trace_address": "VARCHAR",
            "useropevent_nonce": "VARCHAR",
            "useropevent_success": "BOOLEAN",
            "useropevent_actualgascost": "VARCHAR",
            "useropevent_actualgasused": "VARCHAR",
            "userop_idx": "BIGINT",
            "useropevent_actualgascost_eth": "DOUBLE",
        }

    def test_single_log(self):
        assert self._duckdb_context is not None

        output = (
            self._duckdb_context.client.sql("""
        SELECT * FROM useroperationevent_logs_v2
        WHERE transaction_hash = '0xeb8aed49895870a10eaee7fc6b38d00e6081816e1c7309fd6829f9299a386b58'
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
                "log_index": 71,
                "contract_address": "0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789",
                "userophash": "0x643741a0cac24596076e66f13b742da1ef5f0ec49a217571e72f5f2986e2cd6c",
                "sender": "0x118bd3dc35b5ff4d5a20b8f48b1824f471ebc563",
                "paymaster": "0x0000000000000000000000000000000000000000",
                "nonce": "69",
                "success": True,
                "actualGasCost": "1192842115198",
                "actualGasUsed": "225523",
            }
        ]

    def test_single_trace_yes_joined_to_userop(self):
        assert self._duckdb_context is not None

        output = (
            self._duckdb_context.client.sql("""
        SELECT * FROM enriched_entrypoint_traces_v2 WHERE transaction_hash = '0xeb8aed49895870a10eaee7fc6b38d00e6081816e1c7309fd6829f9299a386b58'
        AND trace_address LIKE '1%'
        ORDER BY trace_address
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
                "from_address": "0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789",
                "to_address": "0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789",
                "value": "0",
                "input": "0x1d73275600000000000000000000000000000000000000000000000000000000000001c0000000000000000000000000118bd3dc35b5ff4d5a20b8f48b1824f471ebc5630000000000000000000000000000000000000000000000000000000000000045000000000000000000000000000000000000000000000000000000000002298d000000000000000000000000000000000000000000000000000000000000aba7000000000000000000000000000000000000000000000000000000000000dedc000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005d8ce100000000000000000000000000000000000000000000000000000000000f4240643741a0cac24596076e66f13b742da1ef5f0ec49a217571e72f5f2986e2cd6c0000000000000000000000000000000000000000000000000000015a738a021000000000000000000000000000000000000000000000000000000000000000600000000000000000000000000000000000000000000000000000000000017a4500000000000000000000000000000000000000000000000000000000000003600000000000000000000000000000000000000000000000000000000000000164b61d27f600000000000000000000000080625310db240631a91f61659ccc550ea97617420000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000c429c911c9000000000000000000000000118bd3dc35b5ff4d5a20b8f48b1824f471ebc563000000000000000000000000000000000000000000000000000000000000119d0000000000000000000000000000000000000000000000000000000066e8d5e3000000000000000000000000000000000000000000000000000000000000001c4c315768fd4d0e235d8b90a5a972caafb5dfe134baf56bdeb241ddd93392fd290b1f611e6c488670168466aabea54791faa68dc9c66bb0e2a69b3a840926029a00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
                "output": "0x00000000000000000000000000000000000000000000000000000115baee387e",
                "trace_type": "call",
                "call_type": "call",
                "reward_type": "",
                "gas": 220602,
                "gas_used": 134356,
                "subtraces": 1,
                "trace_address": "1",
                "error": "",
                "status": 1,
                "trace_root": 1,
                "method_id": "0x1d732756",
                "tx_from_address": "0xc4a4e8ae10b82a954519ca2ecc9efc8f77819e86",
                "bundler_address": "0xc4a4e8ae10b82a954519ca2ecc9efc8f77819e86",
                "entrypoint_contract_address": "0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789",
                "is_innerhandleop": True,
                "is_from_sender": False,
                "userop_sender": "0x118bd3dc35b5ff4d5a20b8f48b1824f471ebc563",
                "userop_paymaster": "0x0000000000000000000000000000000000000000",
                "userop_hash": "0x643741a0cac24596076e66f13b742da1ef5f0ec49a217571e72f5f2986e2cd6c",
                "userop_calldata": "0xb61d27f600000000000000000000000080625310db240631a91f61659ccc550ea97617420000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000c429c911c9000000000000000000000000118bd3dc35b5ff4d5a20b8f48b1824f471ebc563000000000000000000000000000000000000000000000000000000000000119d0000000000000000000000000000000000000000000000000000000066e8d5e3000000000000000000000000000000000000000000000000000000000000001c4c315768fd4d0e235d8b90a5a972caafb5dfe134baf56bdeb241ddd93392fd290b1f611e6c488670168466aabea54791faa68dc9c66bb0e2a69b3a840926029a00000000000000000000000000000000000000000000000000000000",
                "innerhandleop_decodeerror": None,
                "innerhandleop_opinfo": '{"mUserOp": {"nonce": "69", "callGasLimit": "141709", "verificationGasLimit": "43943", "preVerificationGas": "57052", "maxFeePerGas": "6130913", "maxPriorityFeePerGas": "1000000"}, "prefund": "1487997108752", "contextOffset": "96", "preOpGas": "96837"}',
                "innerhandleop_context": "0x",
                "innerhandleop_trace_address": "1",
                "entrypoint_contract_version": "v6",
                "useropevent_nonce": "69",
                "useropevent_success": True,
                "useropevent_actualgascost": "1192842115198",
                "useropevent_actualgasused": "225523",
                "userop_idx": None,
                "useropevent_actualgascost_eth": 1.192842115198e-06,
            },
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
                "from_address": "0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789",
                "to_address": "0x118bd3dc35b5ff4d5a20b8f48b1824f471ebc563",
                "value": "0",
                "input": "0xb61d27f600000000000000000000000080625310db240631a91f61659ccc550ea97617420000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000c429c911c9000000000000000000000000118bd3dc35b5ff4d5a20b8f48b1824f471ebc563000000000000000000000000000000000000000000000000000000000000119d0000000000000000000000000000000000000000000000000000000066e8d5e3000000000000000000000000000000000000000000000000000000000000001c4c315768fd4d0e235d8b90a5a972caafb5dfe134baf56bdeb241ddd93392fd290b1f611e6c488670168466aabea54791faa68dc9c66bb0e2a69b3a840926029a00000000000000000000000000000000000000000000000000000000",
                "output": "",
                "trace_type": "call",
                "call_type": "call",
                "reward_type": "",
                "gas": 141709,
                "gas_used": 128036,
                "subtraces": 1,
                "trace_address": "1,0",
                "error": "",
                "status": 1,
                "trace_root": 1,
                "method_id": "0xb61d27f6",
                "tx_from_address": "0xc4a4e8ae10b82a954519ca2ecc9efc8f77819e86",
                "bundler_address": "0xc4a4e8ae10b82a954519ca2ecc9efc8f77819e86",
                "entrypoint_contract_address": "0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789",
                "is_innerhandleop": False,
                "is_from_sender": False,
                "userop_sender": "0x118bd3dc35b5ff4d5a20b8f48b1824f471ebc563",
                "userop_paymaster": "0x0000000000000000000000000000000000000000",
                "userop_hash": "0x643741a0cac24596076e66f13b742da1ef5f0ec49a217571e72f5f2986e2cd6c",
                "userop_calldata": "0xb61d27f600000000000000000000000080625310db240631a91f61659ccc550ea97617420000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000c429c911c9000000000000000000000000118bd3dc35b5ff4d5a20b8f48b1824f471ebc563000000000000000000000000000000000000000000000000000000000000119d0000000000000000000000000000000000000000000000000000000066e8d5e3000000000000000000000000000000000000000000000000000000000000001c4c315768fd4d0e235d8b90a5a972caafb5dfe134baf56bdeb241ddd93392fd290b1f611e6c488670168466aabea54791faa68dc9c66bb0e2a69b3a840926029a00000000000000000000000000000000000000000000000000000000",
                "innerhandleop_decodeerror": None,
                "innerhandleop_opinfo": '{"mUserOp": {"nonce": "69", "callGasLimit": "141709", "verificationGasLimit": "43943", "preVerificationGas": "57052", "maxFeePerGas": "6130913", "maxPriorityFeePerGas": "1000000"}, "prefund": "1487997108752", "contextOffset": "96", "preOpGas": "96837"}',
                "innerhandleop_context": "0x",
                "innerhandleop_trace_address": "1",
                "entrypoint_contract_version": "v6",
                "useropevent_nonce": "69",
                "useropevent_success": True,
                "useropevent_actualgascost": "1192842115198",
                "useropevent_actualgasused": "225523",
                "userop_idx": None,
                "useropevent_actualgascost_eth": 1.192842115198e-06,
            },
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
                "to_address": "0x80625310db240631a91f61659ccc550ea9761742",
                "value": "0",
                "input": "0x29c911c9000000000000000000000000118bd3dc35b5ff4d5a20b8f48b1824f471ebc563000000000000000000000000000000000000000000000000000000000000119d0000000000000000000000000000000000000000000000000000000066e8d5e3000000000000000000000000000000000000000000000000000000000000001c4c315768fd4d0e235d8b90a5a972caafb5dfe134baf56bdeb241ddd93392fd290b1f611e6c488670168466aabea54791faa68dc9c66bb0e2a69b3a840926029a",
                "output": "",
                "trace_type": "call",
                "call_type": "call",
                "reward_type": "",
                "gas": 132775,
                "gas_used": 123267,
                "subtraces": 3,
                "trace_address": "1,0,0,0",
                "error": "",
                "status": 1,
                "trace_root": 1,
                "method_id": "0x29c911c9",
                "tx_from_address": "0xc4a4e8ae10b82a954519ca2ecc9efc8f77819e86",
                "bundler_address": "0xc4a4e8ae10b82a954519ca2ecc9efc8f77819e86",
                "entrypoint_contract_address": "0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789",
                "is_innerhandleop": False,
                "is_from_sender": True,
                "userop_sender": "0x118bd3dc35b5ff4d5a20b8f48b1824f471ebc563",
                "userop_paymaster": "0x0000000000000000000000000000000000000000",
                "userop_hash": "0x643741a0cac24596076e66f13b742da1ef5f0ec49a217571e72f5f2986e2cd6c",
                "userop_calldata": "0xb61d27f600000000000000000000000080625310db240631a91f61659ccc550ea97617420000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000c429c911c9000000000000000000000000118bd3dc35b5ff4d5a20b8f48b1824f471ebc563000000000000000000000000000000000000000000000000000000000000119d0000000000000000000000000000000000000000000000000000000066e8d5e3000000000000000000000000000000000000000000000000000000000000001c4c315768fd4d0e235d8b90a5a972caafb5dfe134baf56bdeb241ddd93392fd290b1f611e6c488670168466aabea54791faa68dc9c66bb0e2a69b3a840926029a00000000000000000000000000000000000000000000000000000000",
                "innerhandleop_decodeerror": None,
                "innerhandleop_opinfo": '{"mUserOp": {"nonce": "69", "callGasLimit": "141709", "verificationGasLimit": "43943", "preVerificationGas": "57052", "maxFeePerGas": "6130913", "maxPriorityFeePerGas": "1000000"}, "prefund": "1487997108752", "contextOffset": "96", "preOpGas": "96837"}',
                "innerhandleop_context": "0x",
                "innerhandleop_trace_address": "1",
                "entrypoint_contract_version": "v6",
                "useropevent_nonce": "69",
                "useropevent_success": True,
                "useropevent_actualgascost": "1192842115198",
                "useropevent_actualgasused": "225523",
                "userop_idx": 1,
                "useropevent_actualgascost_eth": 1.192842115198e-06,
            },
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
                "from_address": "0x80625310db240631a91f61659ccc550ea9761742",
                "to_address": "0x0982b3a5b24b2bd8ef74126e15fca2decfd75a28",
                "value": "0",
                "input": "0x6352211e000000000000000000000000000000000000000000000000000000000000119d",
                "output": "0x000000000000000000000000118bd3dc35b5ff4d5a20b8f48b1824f471ebc563",
                "trace_type": "call",
                "call_type": "staticcall",
                "reward_type": "",
                "gas": 116774,
                "gas_used": 3028,
                "subtraces": 0,
                "trace_address": "1,0,0,0,0",
                "error": "",
                "status": 1,
                "trace_root": 1,
                "method_id": "0x6352211e",
                "tx_from_address": "0xc4a4e8ae10b82a954519ca2ecc9efc8f77819e86",
                "bundler_address": "0xc4a4e8ae10b82a954519ca2ecc9efc8f77819e86",
                "entrypoint_contract_address": "0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789",
                "is_innerhandleop": False,
                "is_from_sender": False,
                "userop_sender": "0x118bd3dc35b5ff4d5a20b8f48b1824f471ebc563",
                "userop_paymaster": "0x0000000000000000000000000000000000000000",
                "userop_hash": "0x643741a0cac24596076e66f13b742da1ef5f0ec49a217571e72f5f2986e2cd6c",
                "userop_calldata": "0xb61d27f600000000000000000000000080625310db240631a91f61659ccc550ea97617420000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000c429c911c9000000000000000000000000118bd3dc35b5ff4d5a20b8f48b1824f471ebc563000000000000000000000000000000000000000000000000000000000000119d0000000000000000000000000000000000000000000000000000000066e8d5e3000000000000000000000000000000000000000000000000000000000000001c4c315768fd4d0e235d8b90a5a972caafb5dfe134baf56bdeb241ddd93392fd290b1f611e6c488670168466aabea54791faa68dc9c66bb0e2a69b3a840926029a00000000000000000000000000000000000000000000000000000000",
                "innerhandleop_decodeerror": None,
                "innerhandleop_opinfo": '{"mUserOp": {"nonce": "69", "callGasLimit": "141709", "verificationGasLimit": "43943", "preVerificationGas": "57052", "maxFeePerGas": "6130913", "maxPriorityFeePerGas": "1000000"}, "prefund": "1487997108752", "contextOffset": "96", "preOpGas": "96837"}',
                "innerhandleop_context": "0x",
                "innerhandleop_trace_address": "1",
                "entrypoint_contract_version": "v6",
                "useropevent_nonce": "69",
                "useropevent_success": True,
                "useropevent_actualgascost": "1192842115198",
                "useropevent_actualgasused": "225523",
                "userop_idx": None,
                "useropevent_actualgascost_eth": 1.192842115198e-06,
            },
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
                "from_address": "0x80625310db240631a91f61659ccc550ea9761742",
                "to_address": "0x0000000000000000000000000000000000000001",
                "value": "0",
                "input": "0x311f9469ff3f5beb7003f5a9054ff3bb77d513a7ea05db003e1d8e492672fe5a000000000000000000000000000000000000000000000000000000000000001c4c315768fd4d0e235d8b90a5a972caafb5dfe134baf56bdeb241ddd93392fd290b1f611e6c488670168466aabea54791faa68dc9c66bb0e2a69b3a840926029a",
                "output": "0x000000000000000000000000f8966317b01750ff17e3a05e3c1c822866d00354",
                "trace_type": "call",
                "call_type": "staticcall",
                "reward_type": "",
                "gas": 108572,
                "gas_used": 3000,
                "subtraces": 0,
                "trace_address": "1,0,0,0,1",
                "error": "",
                "status": 1,
                "trace_root": 1,
                "method_id": "0x311f9469",
                "tx_from_address": "0xc4a4e8ae10b82a954519ca2ecc9efc8f77819e86",
                "bundler_address": "0xc4a4e8ae10b82a954519ca2ecc9efc8f77819e86",
                "entrypoint_contract_address": "0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789",
                "is_innerhandleop": False,
                "is_from_sender": False,
                "userop_sender": "0x118bd3dc35b5ff4d5a20b8f48b1824f471ebc563",
                "userop_paymaster": "0x0000000000000000000000000000000000000000",
                "userop_hash": "0x643741a0cac24596076e66f13b742da1ef5f0ec49a217571e72f5f2986e2cd6c",
                "userop_calldata": "0xb61d27f600000000000000000000000080625310db240631a91f61659ccc550ea97617420000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000c429c911c9000000000000000000000000118bd3dc35b5ff4d5a20b8f48b1824f471ebc563000000000000000000000000000000000000000000000000000000000000119d0000000000000000000000000000000000000000000000000000000066e8d5e3000000000000000000000000000000000000000000000000000000000000001c4c315768fd4d0e235d8b90a5a972caafb5dfe134baf56bdeb241ddd93392fd290b1f611e6c488670168466aabea54791faa68dc9c66bb0e2a69b3a840926029a00000000000000000000000000000000000000000000000000000000",
                "innerhandleop_decodeerror": None,
                "innerhandleop_opinfo": '{"mUserOp": {"nonce": "69", "callGasLimit": "141709", "verificationGasLimit": "43943", "preVerificationGas": "57052", "maxFeePerGas": "6130913", "maxPriorityFeePerGas": "1000000"}, "prefund": "1487997108752", "contextOffset": "96", "preOpGas": "96837"}',
                "innerhandleop_context": "0x",
                "innerhandleop_trace_address": "1",
                "entrypoint_contract_version": "v6",
                "useropevent_nonce": "69",
                "useropevent_success": True,
                "useropevent_actualgascost": "1192842115198",
                "useropevent_actualgasused": "225523",
                "userop_idx": None,
                "useropevent_actualgascost_eth": 1.192842115198e-06,
            },
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
                "from_address": "0x80625310db240631a91f61659ccc550ea9761742",
                "to_address": "0x0982b3a5b24b2bd8ef74126e15fca2decfd75a28",
                "value": "0",
                "input": "0x42842e0e000000000000000000000000118bd3dc35b5ff4d5a20b8f48b1824f471ebc56300000000000000000000000080625310db240631a91f61659ccc550ea9761742000000000000000000000000000000000000000000000000000000000000119d",
                "output": "",
                "trace_type": "call",
                "call_type": "call",
                "reward_type": "",
                "gas": 79717,
                "gas_used": 69161,
                "subtraces": 1,
                "trace_address": "1,0,0,0,2",
                "error": "",
                "status": 1,
                "trace_root": 1,
                "method_id": "0x42842e0e",
                "tx_from_address": "0xc4a4e8ae10b82a954519ca2ecc9efc8f77819e86",
                "bundler_address": "0xc4a4e8ae10b82a954519ca2ecc9efc8f77819e86",
                "entrypoint_contract_address": "0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789",
                "is_innerhandleop": False,
                "is_from_sender": False,
                "userop_sender": "0x118bd3dc35b5ff4d5a20b8f48b1824f471ebc563",
                "userop_paymaster": "0x0000000000000000000000000000000000000000",
                "userop_hash": "0x643741a0cac24596076e66f13b742da1ef5f0ec49a217571e72f5f2986e2cd6c",
                "userop_calldata": "0xb61d27f600000000000000000000000080625310db240631a91f61659ccc550ea97617420000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000c429c911c9000000000000000000000000118bd3dc35b5ff4d5a20b8f48b1824f471ebc563000000000000000000000000000000000000000000000000000000000000119d0000000000000000000000000000000000000000000000000000000066e8d5e3000000000000000000000000000000000000000000000000000000000000001c4c315768fd4d0e235d8b90a5a972caafb5dfe134baf56bdeb241ddd93392fd290b1f611e6c488670168466aabea54791faa68dc9c66bb0e2a69b3a840926029a00000000000000000000000000000000000000000000000000000000",
                "innerhandleop_decodeerror": None,
                "innerhandleop_opinfo": '{"mUserOp": {"nonce": "69", "callGasLimit": "141709", "verificationGasLimit": "43943", "preVerificationGas": "57052", "maxFeePerGas": "6130913", "maxPriorityFeePerGas": "1000000"}, "prefund": "1487997108752", "contextOffset": "96", "preOpGas": "96837"}',
                "innerhandleop_context": "0x",
                "innerhandleop_trace_address": "1",
                "entrypoint_contract_version": "v6",
                "useropevent_nonce": "69",
                "useropevent_success": True,
                "useropevent_actualgascost": "1192842115198",
                "useropevent_actualgasused": "225523",
                "userop_idx": None,
                "useropevent_actualgascost_eth": 1.192842115198e-06,
            },
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
                "from_address": "0x0982b3a5b24b2bd8ef74126e15fca2decfd75a28",
                "to_address": "0x80625310db240631a91f61659ccc550ea9761742",
                "value": "0",
                "input": "0x150b7a0200000000000000000000000080625310db240631a91f61659ccc550ea9761742000000000000000000000000118bd3dc35b5ff4d5a20b8f48b1824f471ebc563000000000000000000000000000000000000000000000000000000000000119d00000000000000000000000000000000000000000000000000000000000000800000000000000000000000000000000000000000000000000000000000000000",
                "output": "0x150b7a0200000000000000000000000000000000000000000000000000000000",
                "trace_type": "call",
                "call_type": "call",
                "reward_type": "",
                "gas": 12154,
                "gas_used": 1349,
                "subtraces": 0,
                "trace_address": "1,0,0,0,2,0",
                "error": "",
                "status": 1,
                "trace_root": 1,
                "method_id": "0x150b7a02",
                "tx_from_address": "0xc4a4e8ae10b82a954519ca2ecc9efc8f77819e86",
                "bundler_address": "0xc4a4e8ae10b82a954519ca2ecc9efc8f77819e86",
                "entrypoint_contract_address": "0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789",
                "is_innerhandleop": False,
                "is_from_sender": False,
                "userop_sender": "0x118bd3dc35b5ff4d5a20b8f48b1824f471ebc563",
                "userop_paymaster": "0x0000000000000000000000000000000000000000",
                "userop_hash": "0x643741a0cac24596076e66f13b742da1ef5f0ec49a217571e72f5f2986e2cd6c",
                "userop_calldata": "0xb61d27f600000000000000000000000080625310db240631a91f61659ccc550ea97617420000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000c429c911c9000000000000000000000000118bd3dc35b5ff4d5a20b8f48b1824f471ebc563000000000000000000000000000000000000000000000000000000000000119d0000000000000000000000000000000000000000000000000000000066e8d5e3000000000000000000000000000000000000000000000000000000000000001c4c315768fd4d0e235d8b90a5a972caafb5dfe134baf56bdeb241ddd93392fd290b1f611e6c488670168466aabea54791faa68dc9c66bb0e2a69b3a840926029a00000000000000000000000000000000000000000000000000000000",
                "innerhandleop_decodeerror": None,
                "innerhandleop_opinfo": '{"mUserOp": {"nonce": "69", "callGasLimit": "141709", "verificationGasLimit": "43943", "preVerificationGas": "57052", "maxFeePerGas": "6130913", "maxPriorityFeePerGas": "1000000"}, "prefund": "1487997108752", "contextOffset": "96", "preOpGas": "96837"}',
                "innerhandleop_context": "0x",
                "innerhandleop_trace_address": "1",
                "entrypoint_contract_version": "v6",
                "useropevent_nonce": "69",
                "useropevent_success": True,
                "useropevent_actualgascost": "1192842115198",
                "useropevent_actualgasused": "225523",
                "userop_idx": None,
                "useropevent_actualgascost_eth": 1.192842115198e-06,
            },
        ]

    def test_single_trace_not_joined_to_userop(self):
        assert self._duckdb_context is not None

        output = (
            self._duckdb_context.client.sql("""
        SELECT * FROM enriched_entrypoint_traces_v2 WHERE transaction_hash = '0xeb8aed49895870a10eaee7fc6b38d00e6081816e1c7309fd6829f9299a386b58'
        AND trace_address = '0,0,0'
        """)
            .pl()
            .to_dicts()
        )

        # (pedro - 2025/03/03) We switched from a LEFT join to an INNER join.
        # There is little value on keeping the outer traces in the transaaction.
        # userOps only exist inside innderHandleOp calls.
        assert output == []

    def test_log_counts(self):
        assert self._duckdb_context is not None

        output = (
            self._duckdb_context.client.sql("""
        SELECT transaction_hash, count(*) as num_logs FROM useroperationevent_logs_v2
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
                "num_logs": 1,
            },
            {
                "transaction_hash": "0x20d2ef78248fd4ab1d024e2a0aa21bb233a8af984eb7136405d2ab04d759ec89",
                "num_logs": 1,
            },
            {
                "transaction_hash": "0x3de6952ace7f1cff6107320aaa890a77d49027d74692402218ab27fa3cb0accc",
                "num_logs": 5,
            },
            {
                "transaction_hash": "0x4340931bc3502cdb912f728da53b922d8eb41709953f8985883f5b483cc85843",
                "num_logs": 1,
            },
            {
                "transaction_hash": "0x475f4b962872405fe53605978c43ce02e06accb0f9f58eaac509213c77e6a637",
                "num_logs": 1,
            },
            {
                "transaction_hash": "0x54025a3db9df5a8792d175deecbf010223ea9c39affdc8ea14cefde7c48a4243",
                "num_logs": 1,
            },
            {
                "transaction_hash": "0x544a2d3e241baa7d25c4a0c7123b59ae2aac9f5bb8f67dbe007eec63e6f7d9ab",
                "num_logs": 1,
            },
            {
                "transaction_hash": "0x569d42f98e509891bcbfa0f23b2fa1b8dfad1faa8d4622db7c7abba587340519",
                "num_logs": 1,
            },
            {
                "transaction_hash": "0x6b00c73a44cd2c6e3f1cfd7a36176b88cad29d15a3b38eca60f21589ae977a0f",
                "num_logs": 1,
            },
            {
                "transaction_hash": "0x7298c85972572a17b5256c891df2b335012778df093bb80e62c1e08db4eab1ef",
                "num_logs": 1,
            },
        ]
