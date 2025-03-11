/**

Decode DelegateVotesChanged event logs.

*/

SELECT
  t.dt AS dt
  , t.timestamp AS block_timestamp
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
  , reinterpretAsInt256(
    reverse(
      unhex(
        substring(substring(l.data, 3), 65, 64)
      )
    )
  ) - reinterpretAsInt256(
    reverse(
      unhex(
        substring(substring(l.data, 3), 1, 64)
      )
    )
  ) AS delegation_amount


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
  ) AS t
  ON l.block_number = t.number

WHERE
  l.topic0 = '0xdec2bacdd2f05b59de34da9b523dff8be42e5e38e818c82fdb0bae774387a724' --Decode DelegateVotesChanged logs
  AND l.address = '0x4200000000000000000000000000000000000042'

SETTINGS use_hive_partitioning = 1
