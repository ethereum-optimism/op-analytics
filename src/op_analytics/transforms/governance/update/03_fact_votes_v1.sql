/**

Time series of votes on proposals.

*/

WITH blocks AS (
  SELECT
      dt
      , toUInt64(assumeNotNull(b.number)) AS block_number
      , b.timestamp AS block_timestamp
  FROM 
  blockbatch_gcs.read_date(
      rootpath = 'ingestion/blocks_v1'
      ,chain = 'op'
      ,dt = { dtparam: Date }
  ) b
  WHERE b.number IS NOT NULL AND 
)

select
    b.dt
    , b.timestamp
    ,v.block_number as block_number
    ,v.transaction_hash as transaction_hash
    ,v.voter as voter_address
    ,v.proposal_id as proposal_id
    ,case when v.support = 0 then 'against'
          when v.support = 1 then 'for'
          when v.support = 2 then 'abstain'
          else null
    end as decision
    ,v.reason as reason
    ,toDecimal256(v.weight, 18)/1e18 as voting_power
from dailydata_gcs.read_date(
    rootpath = 'agora/votes_v1',
    dt = '2000-01-01'
    ) v
inner join blocks t
on v.block_number = t.block_number
settings use_hive_partitioning = 1;