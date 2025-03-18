/**

Join DelegateVotesChanged event logs with delegation event data ingested from Agora's gcs bucket.

*/

WITH blocks AS (
  SELECT block_number
  FROM transforms_governance.fact_delegate_votes_changed_v1
  WHERE dt = { dtparam: Date }
)


SELECT
  date_trunc('day', v.block_timestamp) AS dt
  , v.block_timestamp
  , d.block_number
  , d.transaction_hash

  -- Address that delegates to a delegate
  , d.delegator

  -- Delegate that receives the delegation
  , v.delegate

  -- The delegate the address used to delegate to up until this delegation event; 
  -- if there are > 1 logs with the same undelegated_from address per block, 
  -- the sum of teh delegation amounts should be 0
  , d.from_delegate AS undelegated_from

  , v.log_index AS log_index

  --If 0x0000000000000000000000000000000000000000 then first-time delegation to this delegate from this address
  , CASE
    WHEN d.from_delegate = '0x0000000000000000000000000000000000000000' THEN 1
    ELSE 0
  END AS is_first_time_delegation

  --If to_delegate = from_delegate then address has already been delegating some OP to this delegate
  , CASE
    WHEN d.from_delegate != d.to_delegate THEN 1
    WHEN d.from_delegate = d.to_delegate THEN 0
  END AS is_delegate_change

  -- Self-delegations are flagged when delegator is identical with delegate
  -- Important to differentiate between undelegation and delegation events happening in the same block 
  -- via separate logs (e.g. undelegate 50OP from Delegate A and delegate those 50OP to Delegate B)
  , CASE
    WHEN d.delegator = d.to_delegate THEN 1
    ELSE 0
  END AS is_self_delegation

  , v.previous_balance / 1e18 AS previous_balance
  , v.new_balance / 1e18 AS new_balance
  , (v.new_balance - v.previous_balance) / 1e18 AS delegation_amount


FROM transforms_governance.ingest_delegate_changed_events_v1 AS d

LEFT JOIN transforms_governance.fact_delegate_votes_changed_v1 AS v
  ON
    d.transaction_hash = v.transaction_hash
    AND toUInt64(assumeNotNull(d.block_number)) = v.block_number
    AND v.block_number IS NOT null

WHERE
  d.block_number IN (blocks)
  AND v.dt = { dtparam: Date }

SETTINGS use_hive_partitioning = 1
