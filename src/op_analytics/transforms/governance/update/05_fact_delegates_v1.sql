/**

Time series of all delegates and their voting power.

*/

select
    t.dt as dt
    ,cast(t.block_timestamp as datetime) as block_timestamp
    ,d.block_number as block_number
    ,d.delegate as delegate
    ,d.balance as voting_power
    ,d.transaction_index as transaction_index
    ,d.log_index as log_index
from dailydata_gcs.read_date(
    rootpath = 'agora/voting_power_snaps_v1',
    dt       = '2000-01-01'
) d
inner join blockbatch_gcs.read_date(
    rootpath = 'ingestion/transactions_v1'
    ,chain = 'op'
    ,dt = { dtparam: Date }
  ) t
on d.transaction_index = t.transaction_index
and d.block_number = t.block_number
settings use_hive_partitioning = 1;