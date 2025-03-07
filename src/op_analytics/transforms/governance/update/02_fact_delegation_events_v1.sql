/**

Join DelegateVotesChanged event logs with delegation event data ingested from Agora's gcs bucket.

*/

select
    date_trunc('day', v.block_timestamp) as dt
    ,v.block_timestamp as block_timestamp
    ,d.block_number as block_number
    ,d.transaction_hash as transaction_hash
    ,d.delegator as delegator --Address that delegates to a delegate
    ,v.delegate as delegate --Delegate that receives the delegation
    ,d.from_delegate as undelegated_from --The delegate the address used to delegate to up until this delegation event; if there are > 1 logs with the same undelegated_from address per block, the sum of teh delegation amounts should be 0
    ,case when d.from_delegate = '0x0000000000000000000000000000000000000000' then 1 --If 0x0000000000000000000000000000000000000000 then first-time delegation to this delegate from this address
    else 0
    end as is_first_time_delegation
    ,case when d.from_delegate != d.to_delegate then 1
    when d.from_delegate = d.to_delegate then 0 --If to_delegate = from_delegate then address has already been delegating some OP to this delegate
    else null
    end as is_delegate_change
    ,case when d.delegator = d.to_delegate then 1 --Self-delegations are flagged when delegator is identical with delegate
    else 0
    end as is_self_delegation
    ,v.log_index as log_index --Important to differentiate between undelegation and delegation events happening in the same block via separate logs (e.g. undelegate 50OP from Delegate A and delegate those 50OP to Delegate B)
    ,v.previous_balance/1e18 as previous_balance
    ,v.new_balance/1e18 as new_balance
    ,(v.new_balance - v.previous_balance)/1e18 as delegation_amount
from dailydata_gcs.read_date(
    rootpath = 'agora/delegate_changed_events_v1',
    dt       = '2000-01-01'
) d
left join transforms_governance.fact_delegate_votes_changed_v1 v
on d.transaction_hash = v.transaction_hash
and toUInt64(assumeNotNull(d.block_number)) = v.block_number
and v.block_number is not null
settings use_hive_partitioning = 1;