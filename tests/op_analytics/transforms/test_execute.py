import datetime
from unittest.mock import patch, MagicMock

import polars as pl
from clickhouse_connect.driver.summary import QuerySummary

from op_analytics.coreutils.logger import structlog
from op_analytics.transforms.create import TableStructure, TableColumn
from op_analytics.transforms.task import TransformTask

log = structlog.get_logger()


@patch("op_analytics.transforms.task.new_stateful_client")
@patch("op_analytics.transforms.task.insert_oplabs")
def test_execute_task(mock_insert: MagicMock, mock_new: MagicMock):
    mock_new().command.return_value = QuerySummary({"written_rows": 100})

    task = TransformTask(
        group_name="interop",
        dt=datetime.date(2023, 1, 1),
        tables={
            "fact_erc20_oft_transfers_v1": TableStructure(
                name="fact_erc20_oft_transfers_v1",
                columns=[TableColumn(name="dummy", data_type="String")],
            ),
            "fact_erc20_ntt_transfers_v1": TableStructure(
                name="fact_erc20_ntt_transfers_v1",
                columns=[TableColumn(name="dummy", data_type="String")],
            ),
        },
        update_only=[2, 3],
        raise_if_empty=False,
    )

    task.execute()

    clickhouse_commands = mock_new().command.call_args_list
    calls = []
    for call in clickhouse_commands:
        assert isinstance(call.kwargs["cmd"], str)
        calls.append(
            {
                "parameters": call.kwargs["parameters"],
                "settings": call.kwargs["settings"],
            }
        )

    assert calls == [
        {
            "parameters": {"dtparam": datetime.date(2023, 1, 1)},
            "settings": {"use_hive_partitioning": 1},
        },
        {
            "parameters": {"dtparam": datetime.date(2023, 1, 1)},
            "settings": {"use_hive_partitioning": 1},
        },
    ]

    # assert clickhouse_commands == [
    #     call(
    #         cmd="/**\n\nERC-20 Transfer transactions that also emit an OFTSent event.\n\n\nIMPORTANT: this approach filters for cases when the same token contract emits\nthe OFTSent and the Transfer event. So itonly covers OFT Tokens and it does not\nconver OFTAdapter tokens.\n\n*/\n\nINSERT INTO transforms_interop.fact_erc20_oft_transfers_v1\n\nWITH\n\noft_sent_events AS ( -- noqa: ST03\n  SELECT\n    chain_id\n    , transaction_hash\n    , address AS contract_address\n\n  FROM\n    blockbatch_gcs.read_date(\n      rootpath = 'ingestion/logs_v1'\n      , chain = '*'\n      , dt = { dtparam: Date }\n    )\n\n  WHERE\n    -- OFT Docs:\n    -- https://docs.layerzero.network/v2/home/token-standards/oft-standard\n    -- \n    -- Example Log:\n    -- https://optimistic.etherscan.io/tx/0x40ddae2718940c4487af4c02d889510ab47e2e423028b76a3b00ec9bc8c04798#eventlog#21\n    -- \n    -- Signature:\n    -- OFTSent (\n    --    index_topic_1 bytes32 guid, \n    --    uint32 dstEid, \n    --    index_topic_2 address fromAddress, \n    --    uint256 amountSentLD, \n    --    uint256 amountReceivedLD\n    -- )\n    topic0 = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a'\n)\n\nSELECT\n  t.dt\n  , t.chain\n  , t.chain_id\n  , t.network\n  , t.block_timestamp\n  , t.block_number\n  , t.transaction_hash\n  , t.transaction_index\n  , t.log_index\n  , t.contract_address\n  , t.amount\n  , t.from_address\n  , t.to_address\n\nFROM blockbatch.token_transfers__erc20_transfers_v1 AS t\nWHERE\n  t.dt = { dtparam: Date }\n  AND (t.chain_id, t.transaction_hash, t.contract_address) IN (oft_sent_events)\n",
    #         parameters={"dtparam": datetime.date(2023, 1, 1)},
    #         settings={"use_hive_partitioning": 1},
    #     ),
    #     call(
    #         cmd="/**\n\nERC-20 Transfer transactions that also emit an NTT Delivery event.\n\n\nIMPORTANT: this approach looks for transactions where the same transaction has\nan ERC-20 Transfer followed by an NTT Delivery event. It assumes that contracts that\nemit Transfer events prior to a Delivery event in a transaciton are NTT-enabled\ntoken contracts.\n\nTransactions may be simple, with just one Transfer + Delivery, for example:\n\nhttps://basescan.org/tx/0x3717e2df7d7f070254d5f477a94012f8d17417a2ff6e0f0df7daa767d851808c#eventlog\nhttps://basescan.org/tx/0x39f958600df4faff77320801daea1c9757209f93e653b545d3598596077ad1b8#eventlog\n\nBut we can also have mor complex transactions that have several Transfer events\nbefore the Delivery event, for example:\n\nhttps://optimistic.etherscan.io/tx/0x9ae78927d9771a2bcd89fc9eb467c063753dc30214d4b858e0cb6e02151dc592#eventlog\n\n*/\n\nINSERT INTO transforms_interop.fact_erc20_ntt_transfers_v1\n\nWITH\n\n\n-- NOTE: The Delivery event is not sent by the token contract, it is sent\n-- by the Wormhole Relayer or a proxy.\nntt_delivery_events AS ( -- noqa: ST03\n  SELECT\n    chain_id\n    , transaction_hash\n    , log_index\n\n  FROM\n    blockbatch_gcs.read_date(\n      rootpath = 'ingestion/logs_v1'\n      , chain = '*'\n      , dt = { dtparam: Date }\n    )\n\n  WHERE\n    --\n    -- Delivery(\n    --     index_topic_1 address recipientContract,\n    --     index_topic_2 uint16 sourceChain,\n    --     index_topic_3 uint64 sequence,\n    --     bytes32 deliveryVaaHash,\n    --     uint8 status,\n    --     uint256 gasUsed,\n    --     uint8 refundStatus,\n    --     bytes additionalStatusInfo,\n    --     bytes overridesInfo\n    -- )\n    topic0 = '0xbccc00b713f54173962e7de6098f643d8ebf53d488d71f4b2a5171496d038f9e'\n)\n\nSELECT\n  t.dt\n  , t.chain\n  , t.chain_id\n  , t.network\n  , t.block_timestamp\n  , t.block_number\n  , t.transaction_hash\n  , t.transaction_index\n  , t.log_index\n  , t.contract_address\n  , t.amount\n  , t.from_address\n  , t.to_address\n\nFROM blockbatch.token_transfers__erc20_transfers_v1 AS t\n\nINNER JOIN ntt_delivery_events AS n\n  ON\n    t.chain_id = n.chain_id\n    AND t.transaction_hash = n.transaction_hash\nWHERE\n  t.dt = { dtparam: Date }\n  -- Transfer is before Delivery\n  AND t.log_index < n.log_index\n",
    #         parameters={"dtparam": datetime.date(2023, 1, 1)},
    #         settings={"use_hive_partitioning": 1},
    #     ),
    # ]

    inserts = []

    for insert in mock_insert.call_args_list:
        rows = pl.from_arrow(insert.kwargs["df_arrow"]).to_dicts()

        # Delete non-determnistics values.
        for row in rows:
            del row["writer_name"]

        inserts.append(
            {
                "database": insert.kwargs["database"],
                "table": insert.kwargs["table"],
                "rows": rows,
            }
        )

    assert inserts == [
        {
            "database": "etl_monitor",
            "table": "transform_dt_markers",
            "rows": [
                {
                    "transform": "interop",
                    "dt": datetime.date(2023, 1, 1),
                    "metadata": '[{"name": "02_fact_erc20_oft_transfers_v1.sql", "result": {"written_rows": 100}}, {"name": "03_fact_erc20_ntt_transfers_v1.sql", "result": {"written_rows": 100}}]',
                    "process_name": "default",
                }
            ],
        }
    ]
