/**

Time series of votes on proposals.
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

-- Get the votes for "dt"
, votes AS (
  SELECT
    v.block_number
    , v.transaction_hash
    , v.voter AS voter_address
    , v.proposal_id
    , v.reason
    , CASE
      WHEN v.support = 0 THEN 'against'
      WHEN v.support = 1 THEN 'for'
      WHEN v.support = 2 THEN 'abstain'
    END AS decision
    , toDecimal256(v.weight, 18) / 1e18 AS voting_power
  FROM transforms_governance.ingest_votes_v1 AS v
  WHERE
    v.block_number IN (SELECT b.block_number FROM blocks AS b)
)

-- Join
SELECT
  b.dt AS dt
  , b.block_timestamp AS block_timestamp
  , v.block_number
  , v.transaction_hash
  , v.voter_address
  , v.proposal_id
  , v.reason
  , v.decision
  , v.voting_power
FROM votes AS v
INNER JOIN blocks AS b
  ON v.block_number = b.block_number

SETTINGS use_hive_partitioning = 1   -- noqa: PRS
