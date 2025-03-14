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
    toDate(bsb.block_timestamp)       AS dt,
    p.proposal_id                     AS proposal_id,
    toDateTime(bsb.block_timestamp)   AS start_block_ts
FROM transforms_governance.ingest_proposals_v1 AS p
INNER JOIN blockinfo bsb
ON p.start_block = bsb.block_number
WHERE p.start_block IS NOT NULL
SETTINGS use_hive_partitioning = 1;