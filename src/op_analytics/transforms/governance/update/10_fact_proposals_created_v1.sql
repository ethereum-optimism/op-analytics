-- Get the blocks for "dt"
WITH blocks AS (
  SELECT
    b.dt
    , b.number AS block_number
    , fromUnixTimestamp(b.timestamp) AS block_timestamp
  FROM
    blockbatch_gcs.read_date(
      rootpath = 'ingestion/blocks_v1'
      ,chain = 'op'
      ,dt = { dtparam: Date }
    ) b
  WHERE b.number IS NOT NULL
)

-- Created proposals.
SELECT
  b.dt AS dt
  , p.proposal_id AS proposal_id
  , p.ordinal AS ordinal
  , p.created_block AS created_block
  , b.block_timestamp AS created_block_ts
  , p.proposer AS proposal_creator
  , p.description AS proposal_description
  , p.proposal_type AS proposal_type
  , p.proposal_results AS proposal_results
  , JSONExtractString(p.proposal_type_data, 'name') AS proposal_name
  , toUInt64OrZero(JSONExtractString(p.proposal_type_data, 'quorum')) / 100 AS quorum_perc
  , toUInt32OrZero(JSONExtractString(p.proposal_type_data, 'proposal_type_id')) AS proposal_type_id
  , toUInt64OrZero(JSONExtractString(p.proposal_type_data, 'approval_threshold')) / 100 AS approval_threshold_perc

FROM transforms_governance.ingest_proposals_v1 AS p
INNER JOIN blocks AS b
  ON p.created_block = b.block_number
WHERE p.created_block IS NOT NULL

SETTINGS use_hive_partitioning = 1
