from op_analytics.datasources.agora.schemas import INCREMENTAL_SCHEMAS, OVERWRITE_SCHEMAS


def test_agora_schemas():
    actual = INCREMENTAL_SCHEMAS[0].select()
    expected = """
        SELECT
            CAST(id AS Nullable(String)) AS id
, CAST(delegate AS Nullable(String)) AS delegate
, CAST(balance AS Nullable(String)) AS balance
, CAST(block_number AS Nullable(Int64)) AS block_number
, CAST(ordinal AS Nullable(Int64)) AS ordinal
, CAST(transaction_index AS Nullable(Int64)) AS transaction_index
, CAST(log_index AS Nullable(Int64)) AS log_index
, CAST(contract AS Nullable(String)) AS contract
        FROM s3(
            'https://storage.googleapis.com/agora-optimism-public-usw1/v1/snapshot/voting_power_snaps',
            'csv'
        )
        
        """
    assert actual == expected


def test_agora_schemas_auto_dt():
    actual = OVERWRITE_SCHEMAS[0].select()
    expected = """
        SELECT
            toDate(now()) AS dt
, CAST(proposal_id AS Nullable(String)) AS proposal_id
, CAST(contract AS Nullable(String)) AS contract
, CAST(proposer AS Nullable(String)) AS proposer
, CAST(description AS Nullable(String)) AS description
, CAST(ordinal AS Nullable(Int64)) AS ordinal
, CAST(created_block AS Nullable(Int64)) AS created_block
, CAST(start_block AS Nullable(Int64)) AS start_block
, CAST(end_block AS Nullable(Int64)) AS end_block
, CAST(queued_block AS Nullable(Int64)) AS queued_block
, CAST(cancelled_block AS Nullable(Int64)) AS cancelled_block
, CAST(executed_block AS Nullable(Int64)) AS executed_block
, CAST(proposal_data AS Nullable(String)) AS proposal_data
, CAST(proposal_data_raw AS Nullable(String)) AS proposal_data_raw
, CAST(proposal_type AS Nullable(String)) AS proposal_type
, CAST(proposal_type_data AS Nullable(String)) AS proposal_type_data
, CAST(proposal_results AS Nullable(String)) AS proposal_results
, CAST(created_transaction_hash AS Nullable(String)) AS created_transaction_hash
, CAST(cancelled_transaction_hash AS Nullable(String)) AS cancelled_transaction_hash
, CAST(queued_transaction_hash AS Nullable(String)) AS queued_transaction_hash
, CAST(executed_transaction_hash AS Nullable(String)) AS executed_transaction_hash
, CAST(proposal_type_id AS Nullable(Int64)) AS proposal_type_id
        FROM s3(
            'https://storage.googleapis.com/agora-optimism-public-usw1/v1/snapshot/proposals_v2',
            'csv'
        )
        
        """
    assert actual == expected


def test_agora_schemas_create():
    actual = INCREMENTAL_SCHEMAS[0].create()
    expected = """
        CREATE TABLE IF NOT EXISTS transforms_agora.ingest_voting_power_snaps_v1
        (
            `id` Nullable(String),
`delegate` Nullable(String),
`balance` Nullable(String),
`block_number` Nullable(Int64),
`ordinal` Nullable(Int64),
`transaction_index` Nullable(Int64),
`log_index` Nullable(Int64),
`contract` Nullable(String),
INDEX block_number_idx block_number TYPE minmax GRANULARITY 1
        )
        ENGINE = ReplacingMergeTree
        ORDER BY (id, block_number, transaction_index, log_index)
        """
    assert actual == expected


def test_agora_schemas_create_auto_dt():
    actual = OVERWRITE_SCHEMAS[0].create()
    expected = """
        CREATE TABLE IF NOT EXISTS transforms_agora.ingest_proposals_v1
        (
            `dt` Date,
`proposal_id` Nullable(String),
`contract` Nullable(String),
`proposer` Nullable(String),
`description` Nullable(String),
`ordinal` Nullable(Int64),
`created_block` Nullable(Int64),
`start_block` Nullable(Int64),
`end_block` Nullable(Int64),
`queued_block` Nullable(Int64),
`cancelled_block` Nullable(Int64),
`executed_block` Nullable(Int64),
`proposal_data` Nullable(String),
`proposal_data_raw` Nullable(String),
`proposal_type` Nullable(String),
`proposal_type_data` Nullable(String),
`proposal_results` Nullable(String),
`created_transaction_hash` Nullable(String),
`cancelled_transaction_hash` Nullable(String),
`queued_transaction_hash` Nullable(String),
`executed_transaction_hash` Nullable(String),
`proposal_type_id` Nullable(Int64),
INDEX dt_idx dt TYPE minmax GRANULARITY 1
        )
        ENGINE = ReplacingMergeTree
        ORDER BY (proposal_id)
        """
    assert actual == expected
