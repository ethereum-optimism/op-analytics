WITH blockinfo AS (
    SELECT DISTINCT
        b.number AS block_number,
        b.timestamp AS block_timestamp
    FROM blockbatch_gcs.read_date(
        rootpath = 'ingestion/blocks_v1',
        chain    = 'op',
        dt       = { dtparam: Date }
    ) AS b
    WHERE b.number IS NOT NULL
)
SELECT
    toDate(bxb.block_timestamp)     AS dt,
    p.proposal_id                   AS proposal_id,
    toDateTime(bxb.block_timestamp) AS executed_block_ts
FROM transforms_governance.ingest_proposals_v1 AS p
INNER JOIN blockinfo bxb
ON p.executed_block = bxb.block_number
WHERE p.executed_block IS NOT NULL
SETTINGS use_hive_partitioning = 1;