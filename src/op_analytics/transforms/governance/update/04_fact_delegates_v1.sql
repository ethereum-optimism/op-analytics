/**

Time series of all delegates and their voting power.
*/

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

-- Get the voting power for the blocks in "dt"
, voting_power AS (
  SELECT
    d.block_number
    , d.delegate
    , d.balance AS voting_power
    , d.transaction_index
    , d.log_index
  FROM transforms_governance.ingest_voting_power_snaps_v1 AS d
  WHERE d.block_number IN (SELECT b.block_number FROM blocks AS b)
)

SELECT
  b.dt AS dt
  , b.block_timestamp
  , vp.block_number
  , vp.delegate
  , vp.voting_power
  , vp.transaction_index
  , vp.log_index

FROM voting_power AS vp
INNER JOIN blocks AS b
  ON vp.block_number = b.block_number

SETTINGS use_hive_partitioning = 1  -- noqa: PRS