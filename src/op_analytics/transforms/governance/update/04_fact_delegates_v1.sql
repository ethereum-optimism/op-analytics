/**

Time series of all delegates and their voting power.

*/

WITH blocks AS (
  SELECT
      b.dt
      , b.number AS block_number
      , b.timestamp AS block_timestamp
  FROM 
  blockbatch_gcs.read_date(
      rootpath = 'ingestion/blocks_v1'
      ,chain = 'op'
      ,dt = { dtparam: Date }
  ) b
  WHERE b.number IS NOT NULL 
)

select
    b.dt as dt
    ,cast(t.block_timestamp as datetime) as block_timestamp
    ,d.block_number as block_number
    ,d.delegate as delegate
    ,d.balance as voting_power
    ,d.transaction_index as transaction_index
    ,d.log_index as log_index
from transforms_governance.ingest_voting_power_snaps_v1 d
inner join blocks b
on d.block_number = b.block_number
where t.dt = { dtparam: Date }
settings use_hive_partitioning = 1