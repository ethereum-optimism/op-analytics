/**

Decode DelegateVotesChanged event logs.

This is not sourced Agora. We are reading the raw OP Mainnet logs.
*/

SELECT
  b.dt AS dt
  , fromUnixTimestamp(b.timestamp) AS block_timestamp
  , l.block_number
  , l.transaction_hash
  , l.log_index

  --Decode the delegate address from topic1
  , concat('0x', lower(substr(l.indexed_args[1], 27))) AS delegate

  --Extract previous_balance
  , reinterpretAsUInt256(
    reverse(
      unhex(
        substring(substring(l.data, 3), 1, 64)
      )
    )
  ) AS previous_balance

  --Extract new_balance
  , reinterpretAsUInt256(
    reverse(
      unhex(
        substring(substring(l.data, 3), 65, 64)
      )
    )
  ) AS new_balance

  --Calculate the balance difference to get the (un)delegated OP amount
  , new_balance - previous_balance AS delegation_amount


FROM
  blockbatch_gcs.read_date(
    rootpath = 'ingestion/logs_v1'
    , chain = 'op'
    , dt = { dtparam: Date }
  ) AS l
INNER JOIN
  blockbatch_gcs.read_date(
    rootpath = 'ingestion/blocks_v1'
    , chain = 'op'
    , dt = { dtparam: Date }
  ) AS b
  ON l.block_number = b.number

WHERE
  -- DelegateVotesChanged
  l.topic0 = '0xdec2bacdd2f05b59de34da9b523dff8be42e5e38e818c82fdb0bae774387a724'
  AND l.address = '0x4200000000000000000000000000000000000042'

SETTINGS use_hive_partitioning = 1  -- noqa: PRS
