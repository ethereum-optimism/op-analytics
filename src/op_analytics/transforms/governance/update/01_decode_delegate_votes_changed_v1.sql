/**

Decode DelegateVotesChanged event logs.

*/

insert into _placeholder_

select
    t.block_timestamp,
    l.block_number,
    l.transaction_hash,
    l.log_index,

    --Split into topics
    assumeNotNull(splitByChar(',', ifNull(l.topics, ''))) as arr,

    --Decode the delegate address from topic1
    concat('0x', lower(substr(arr[2], 27))) as delegate,

    --Extract previous_balance
    (
        reinterpretAsUInt256(
            reverse(
                unhex(
                    substring(substring(l.data, 3), 1, 64)
                )
            )
        ) / 1e18
    ) as previous_balance,

    --Extract new_balance
    (
        reinterpretAsUInt256(
            reverse(
                unhex(
                    substring(substring(l.data, 3), 65, 64)
                )
            )
        ) / 1e18
    ) as new_balance,

    --Calculate the balance difference to get the (un)delegated OP amount
    (
        (
            reinterpretAsInt256(
                reverse(
                    unhex(
                        substring(substring(l.data, 3), 65, 64)
                    )
                )
            )
          - reinterpretAsInt256(
                reverse(
                    unhex(
                        substring(substring(l.data, 3), 1, 64)
                    )
                )
            )
        ) / 1e18
    ) as delegation_amount

from blockbatch_gcs.read_date(
    rootpath = 'ingestion/logs_v1'
    ,chain = 'op'
    ,dt = '*-*-*'
  ) l
inner join blockbatch_gcs.read_date(
    rootpath = 'ingestion/transactions_v1'
    ,chain = 'op'
    ,dt = '*-*-*'
  ) t
on l.transaction_hash = t.hash
and l.block_number = t.block_number
where assumeNotNull(splitByChar(',', ifNull(l.topics, '')))[1] = '0xdec2bacdd2f05b59de34da9b523dff8be42e5e38e818c82fdb0bae774387a724' --Decode DelegateVotesChanged logs
and lower(l.address) = '0x4200000000000000000000000000000000000042'
settings use_hive_partitioning = 1;