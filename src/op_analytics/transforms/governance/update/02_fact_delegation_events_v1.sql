/**

Join DelegateVotesChanged event logs with Agora's delegate_changed_events.

*/

-- Get the blocks for "dt"
WITH blocks AS (
  SELECT
    b.dt
    , toUInt64(assumeNotNull(b.number)) AS block_number
    , fromUnixTimestamp(b.timestamp) AS block_timestamp
  FROM
    blockbatch_gcs.read_date(
      rootpath = 'ingestion/blocks_v1'
      , chain = 'op'
      , dt = { dtparam: Date }
    ) AS b
  WHERE b.number IS NOT NULL
)


-- Get the votes changed for "dt"
, votes_changed AS (
  SELECT
    dt
    , transaction_hash
    , block_number
    , block_timestamp
    , delegate
    , log_index
    , previous_balance AS previous_balance
    , new_balance AS new_balance
    , delegation_amount AS delegation_amount

  FROM transforms_governance.fact_delegate_votes_changed_v1
  WHERE dt = { dtparam: Date }
)

, changed_events AS (
  SELECT
    toUInt64(d.block_number) AS block_number
    , d.transaction_hash AS transaction_hash

    -- Address that delegates to a delegate
    , d.delegator

    -- The delegate the address used to delegate to up until this delegation event; 
    -- if there are > 1 logs with the same undelegated_from address per block, 
    -- the sum of teh delegation amounts should be 0
    , d.from_delegate AS undelegated_from
    , d.to_delegate AS delegated_to

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

  FROM transforms_governance.ingest_delegate_changed_events_v1 AS d
  WHERE d.block_number IN (SELECT b.block_number FROM blocks AS b)
)


SELECT
  b.dt AS dt
  , b.block_timestamp AS block_timestamp
  , d.block_number AS block_number
  , d.transaction_hash AS transaction_hash
  , d.delegator
  , d.undelegated_from
  , d.delegated_to

  , d.is_first_time_delegation
  , d.is_delegate_change
  , d.is_self_delegation

  , v.log_index AS votes_changed_log_index
  , v.previous_balance AS delegated_to_previous_balance
  , v.new_balance AS delegated_to_new_balance
  , v.delegation_amount AS delegated_to_amount

-- Join to obtain the block timestamp.
FROM changed_events AS d
LEFT JOIN blocks AS b
  ON d.block_number = b.block_number

-- Join to obtain the balance changes.
LEFT JOIN votes_changed AS v
  ON
    d.transaction_hash = v.transaction_hash
    AND d.block_number = v.block_number
    AND d.delegated_to = v.delegate

SETTINGS join_use_nulls = 1, use_hive_partitioning = 1  -- noqa: PRS
