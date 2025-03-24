-- Get the blocks for "dt"
WITH blocks AS (
  SELECT
    b.dt
    , b.number AS block_number
    , fromUnixTimestamp(b.timestamp) AS block_timestamp
  FROM
    blockbatch_gcs.read_date(
      rootpath = 'ingestion/blocks_v1'
      , chain = 'op'
      , dt = { dtparam: Date }
    ) b
  WHERE b.number IS NOT NULL
)

SELECT
  b.dt AS dt
  , p.proposal_id AS proposal_id
  , b.block_timestamp AS end_block_ts
  , toUInt256OrZero(trimBoth(simpleJSONExtractRaw(simpleJSONExtractRaw(p.proposal_results, 'standard'), '0'))) AS total_against_votes
  , toUInt256OrZero(trimBoth(simpleJSONExtractRaw(simpleJSONExtractRaw(p.proposal_results, 'standard'), '1'))) AS total_for_votes
  , toUInt256OrZero(trimBoth(simpleJSONExtractRaw(simpleJSONExtractRaw(p.proposal_results, 'standard'), '2'))) AS total_abstain_votes
  , (total_against_votes + total_for_votes + total_abstain_votes) AS total_votes
  , if(total_votes = 0, 0, total_against_votes / total_votes) AS pct_against
  , if(total_votes = 0, 0, total_for_votes / total_votes) AS pct_for
  , if(total_votes = 0, 0, total_abstain_votes / total_votes) AS pct_abstain

FROM transforms_governance.ingest_proposals_v1 AS p
INNER JOIN blocks AS b
  ON p.end_block = b.block_number

WHERE p.end_block IS NOT NULL

SETTINGS use_hive_partitioning = 1