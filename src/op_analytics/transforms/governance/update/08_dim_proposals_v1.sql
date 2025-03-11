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
      b.rootpath = 'ingestion/blocks_v1'
      , b.chain = 'op'
      , b.dt = { dtparam: b.Date }
    ) AS b
  WHERE b.number IS NOT NULL
)

with blockinfo as (
    select distinct
        b.dt
        ,b.number as block_number
        ,b.timestamp as block_timestamp
    from blockbatch_gcs.read_date(
        rootpath = 'ingestion/blocks_v1'
        ,chain    = 'op'
        ,dt       = { dtparam: Date }
    ) b
    WHERE b.number IS NOT NULL
)
,computed as (
  select
    p.ordinal,
    p.created_block,
    p.start_block,
    p.end_block,
    p.cancelled_block,
    p.proposer as proposal_creator,
    p.description as proposal_description,
    p.proposal_type,
    p.proposal_results,
    JSONExtractString(p.proposal_type_data, 'name') as name,
    toUInt64OrZero(JSONExtractString(p.proposal_type_data, 'quorum')) / 100 as quorum_perc,
    toUInt32OrZero(JSONExtractString(p.proposal_type_data, 'proposal_type_id')) as proposal_type_id,
    toUInt64OrZero(JSONExtractString(p.proposal_type_data, 'approval_threshold')) / 100 as approval_threshold_perc,
    case 
      when p.executed_block is not null then 1 
      else 0 
    end as is_executed,
    case 
      when p.cancelled_block is not null then 1 
      else 0 
    end as is_cancelled,
    p.proposal_id,
    toDateTime(bcb.block_timestamp) as created_block_ts,
    toDateTime(bsb.block_timestamp) as start_block_ts,
    toDateTime(beb.block_timestamp) as end_block_ts,
    toDateTime(bxb.block_timestamp) as cancelled_block_ts,
    ifNull(toDecimal256OrNull(extract(ifNull(p.proposal_results, ''), '"0":\\s*([^,}]+)'), 18) / 1e18, 0) as total_against_votes,
    ifNull(toDecimal256OrNull(extract(ifNull(p.proposal_results, ''), '"1":\\s*([^,}]+)'), 18) / 1e18, 0) as total_for_votes,
    ifNull(toDecimal256OrNull(extract(ifNull(p.proposal_results, ''), '"2":\\s*([^,}]+)'), 18) / 1e18,0) as total_abstain_votes,
    nullIf(JSON_VALUE(p.proposal_results, '$.approval'), 'null') as approval_null
  from dailydata_gcs.read_date(
         rootpath = 'agora/proposals_v1',
         dt       = '2000-01-01'
       ) p
    left join blockinfo bcb on p.created_block = bcb.block_number
    left join blockinfo bsb on p.start_block = bsb.block_number
    left join blockinfo beb on p.end_block = beb.block_number
    left join blockinfo bxb on p.cancelled_block = bxb.block_number
    where created_block_ts in (select block_number from blockinfo) and p.dt = { dtparam: Date }
  settings use_hive_partitioning = 1
)

select
    proposal_id,
    ordinal,
    created_block_ts,
    start_block_ts,
    end_block_ts,
    cancelled_block_ts,
    proposal_creator,
    proposal_description,
    proposal_type,
    proposal_results,
    name,
    quorum_perc,
    proposal_type_id,
    approval_threshold_perc,
    is_executed,
    is_cancelled,
    total_against_votes,
    total_for_votes,
    total_abstain_votes,
  (total_against_votes + total_for_votes + total_abstain_votes) as total_votes,
  if(total_votes = 0, 0, total_against_votes / total_votes) as pct_against,
  if(total_votes = 0, 0, total_for_votes / total_votes) as pct_for,
  if(total_votes = 0, 0, total_abstain_votes / total_votes) as pct_abstain
from computed
settings use_hive_partitioning = 1;