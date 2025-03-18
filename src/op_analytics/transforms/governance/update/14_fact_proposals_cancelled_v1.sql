-- Get the blocks for "dt"
WITH blocks AS (
  SELECT
    b.dt
    , b.number AS block_number
    , b.timestamp AS block_timestamp
  FROM
    blockbatch_gcs.read_date(
      rootpath = 'ingestion/blocks_v1'
      , chain = 'op'
      , dt = { dtparam: Date }
    ) AS b
  WHERE b.number IS NOT NULL
)

SELECT
    b.dt     AS dt,
    p.proposal_id                   AS proposal_id,
    toDateTime(b.block_timestamp) AS cancelled_block_ts
FROM transforms_governance.ingest_proposals_v1 AS p
INNER JOIN blocks b
ON p.cancelled_block = b.block_number
WHERE p.cancelled_block IS NOT NULL

SETTINGS use_hive_partitioning = 1