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
    toDate(beb.block_timestamp) AS dt,
    p.proposal_id               AS proposal_id,
    toDateTime(beb.block_timestamp) AS end_block_ts,

    -- From your original logic:
    ifNull(
      toDecimal256OrNull(
        extract(ifNull(p.proposal_results, ''), '"0":\\s*([^,}]+)'),
        18
      ) / 1e18,
      0
    ) AS total_against_votes,

    ifNull(
      toDecimal256OrNull(
        extract(ifNull(p.proposal_results, ''), '"1":\\s*([^,}]+)'),
        18
      ) / 1e18,
      0
    ) AS total_for_votes,

    ifNull(
      toDecimal256OrNull(
        extract(ifNull(p.proposal_results, ''), '"2":\\s*([^,}]+)'),
        18
      ) / 1e18,
      0
    ) AS total_abstain_votes,

    (total_against_votes + total_for_votes + total_abstain_votes) AS total_votes,
    if(total_votes = 0, 0, total_against_votes / total_votes)     AS pct_against,
    if(total_votes = 0, 0, total_for_votes / total_votes)         AS pct_for,
    if(total_votes = 0, 0, total_abstain_votes / total_votes)     AS pct_abstain

FROM transforms_governance.ingest_proposals_v1 AS p
INNER JOIN blockinfo beb
ON p.end_block = beb.block_number
WHERE p.end_block IS NOT NULL
SETTINGS use_hive_partitioning = 1;