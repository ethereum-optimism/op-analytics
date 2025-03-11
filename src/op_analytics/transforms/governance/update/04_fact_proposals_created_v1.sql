/**

Decode Agora's raw proposals_v2 data dump into a comprehensive table of proposals.

*/


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

-- Get the proposal starts for "dt"
, proposals AS (
  SELECT
    p.proposal_id
    , p.ordinal
    , p.created_block
    , p.proposer AS proposal_creator
    , p.description AS proposal_description
    , p.proposal_type
    , p.proposal_results
    , JSONExtractString(p.proposal_type_data, 'name') AS proposal_name
    , toUInt64OrZero(JSONExtractString(p.proposal_type_data, 'quorum')) / 100 AS quorum_perc
    , toUInt32OrZero(JSONExtractString(p.proposal_type_data, 'proposal_type_id')) AS proposal_type_id
    , toUInt64OrZero(JSONExtractString(p.proposal_type_data, 'approval_threshold')) / 100 AS approval_threshold_perc
  FROM
    dailydata_gcs.read_date(
      rootpath = 'agora/proposals_v1'
      , dt = '2000-01-01'
    ) AS p
  WHERE
    p.created_block IN (SELECT b.block_number FROM blocks AS b)
)

-- Join 
SELECT
  b.dt AS dt
  , p.proposal_id
  , p.ordinal
  , p.created_block
  , b.block_timestamp AS created_block_ts
  , p.proposal_creator
  , p.proposal_description
  , p.proposal_type
  , p.proposal_results
  , p.proposal_name
  , p.quorum_perc
  , p.proposal_type_id
  , p.approval_threshold_perc

FROM proposals AS p
INNER JOIN blocks AS b
  ON p.created_block = b.block_number

SETTINGS use_hive_partitioning = 1
