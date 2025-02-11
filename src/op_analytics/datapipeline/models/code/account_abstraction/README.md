# Account Abstraction Data Pipeline


## Overview

The pipeline is split into two steps (1) pre-filtering, (2) decoding + enrichment. 

Pre-filtering involves filtering raw logs to find events emitted by the AA EntryPoint contract.
From the filtered logs we get a set of AA transactions. We keep traces for those transactions 
(excluding delegatecall traces).

The raw logs are filtered using topic0, where the filter condition is the set of log selectors 
for events in the EntryPoint v0_6_0 and v0_7_0 contracts

The filtered logs and filtered traces are decoded and further enriched to produce the final output
tables. 


## Output Table Descriptions

### `useroperationevent_logs` Table

Refer to [`useroperationevent_logs.sql.j2`](../../templates/account_abstraction/useroperationevent_logs.sql.j2). 

This table is created by reading the pre-filtered EntryPoint logs, filtering to only 
`UserOperationEvent` logs and then decoding the event parameters.

The following columns are obtained from decoding:

* `userophash`. Indexed `topic1`.
* `sender`. Indexed `topic2`.
* `paymaster`. Indexed `topic3`.
* `decoded_json`. Decoded non-indexed parameters as a json string. Example: `{"nonce":"23","success":true,"actualGasCost":"1154461303716","actualGasUsed":"245964"}`



#### Schema

| column_name       | column_type |
|-------------------|-------------|
| dt                | DATE        |
| chain             | VARCHAR     |
| chain_id          | INTEGER     |
| network           | VARCHAR     |
| block_timestamp   | UINTEGER    |
| block_number      | BIGINT      |
| block_hash        | VARCHAR     |
| transaction_hash  | VARCHAR     |
| transaction_index | BIGINT      |
| log_index         | BIGINT      |
| contract_address  | VARCHAR     |
| userophash        | VARCHAR     |
| sender            | VARCHAR     |
| paymaster         | VARCHAR     |
| decoded_json      | VARCHAR     |


#### Sample Data


```
┌────────────┬─────────┬──────────┬─────────┬─────────────────┬──────────────┬────────────────────────────────────────────────────────────────────┬────────────────────────────────────────────────────────────────────┬───────────────────┬───────────┬────────────────────────────────────────────┬────────────────────────────────────────────────────────────────────┬────────────────────────────────────────────┬────────────────────────────────────────────┬────────────────────────────────────────────────────────────────────────────────────────┐
│     dt     │  chain  │ chain_id │ network │ block_timestamp │ block_number │                             block_hash                             │                          transaction_hash                          │ transaction_index │ log_index │              contract_address              │                             userophash                             │                   sender                   │                 paymaster                  │                                      decoded_json                                      │
│    date    │ varchar │  int32   │ varchar │     uint32      │    int64     │                              varchar                               │                              varchar                               │       int64       │   int64   │                  varchar                   │                              varchar                               │                  varchar                   │                  varchar                   │                                        varchar                                         │
├────────────┼─────────┼──────────┼─────────┼─────────────────┼──────────────┼────────────────────────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────┼───────────────────┼───────────┼────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────┼────────────────────────────────────────────┼────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────┤
│ 2025-02-01 │ base    │     8453 │ mainnet │      1738431529 │     25821091 │ 0x1e914a9c01661fcc72bb0e277fe4fa3898cbdecff9225df8d658760ecda59e5d │ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │               141 │       319 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0xc3cde5aba1d69292868339030c7a4af4a796498a73e9534c213693314983f54e │ 0xbc10fc416af0bbad9f3be804c028d02f42729be9 │ 0x2faeb0760d4230ef2ac21496bb4f0b47d634fd4c │ {"nonce":"23","success":true,"actualGasCost":"1154461303716","actualGasUsed":"245964"} │
│ 2025-02-01 │ base    │     8453 │ mainnet │      1738431529 │     25821091 │ 0x1e914a9c01661fcc72bb0e277fe4fa3898cbdecff9225df8d658760ecda59e5d │ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │               141 │       322 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0xe78b37705a4a3bde14b2c82e3b8e98f33e8a1dbc9ee2961cc6f200844f6b9316 │ 0x2eabe2605c7c6b4318096026205c8b63fd6f833f │ 0x0000000000000000000000000000000000000000 │ {"nonce":"23","success":true,"actualGasCost":"532333837539","actualGasUsed":"107681"}  │
│ 2025-02-01 │ base    │     8453 │ mainnet │      1738431529 │     25821091 │ 0x1e914a9c01661fcc72bb0e277fe4fa3898cbdecff9225df8d658760ecda59e5d │ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │               141 │       325 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x6c4613999ba020b91c57a7a37dc99ee75c3e6071b7e8901fe6652fd745875a77 │ 0xb174c63cc8272a81a17fa215489be1348b6a7bf9 │ 0x0000000000000000000000000000000000000000 │ {"nonce":"24","success":true,"actualGasCost":"532244852397","actualGasUsed":"107663"}  │
│ 2025-02-01 │ base    │     8453 │ mainnet │      1738431529 │     25821091 │ 0x1e914a9c01661fcc72bb0e277fe4fa3898cbdecff9225df8d658760ecda59e5d │ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │               141 │       328 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x88387e3489b655c00e83ec999e1571f11ac23ab6d328b4a8e71d5c192e7be821 │ 0x2534b7395f210c35c7b2b4ace6855c645704e2d3 │ 0x0000000000000000000000000000000000000000 │ {"nonce":"25","success":true,"actualGasCost":"532155867255","actualGasUsed":"107645"}  │
│ 2025-02-01 │ base    │     8453 │ mainnet │      1738431529 │     25821091 │ 0x1e914a9c01661fcc72bb0e277fe4fa3898cbdecff9225df8d658760ecda59e5d │ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │               141 │       331 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0xa3170ba7b4fa4810bc60aeb2045cf81f6d25ad7098ca68113ede150234c07ebb │ 0x074b61bba4871f869f7bdb1aff1da17f497673b1 │ 0x0000000000000000000000000000000000000000 │ {"nonce":"1","success":true,"actualGasCost":"531557689356","actualGasUsed":"107524"}   │
└────────────┴─────────┴──────────┴─────────┴─────────────────┴──────────────┴────────────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────┴───────────────────┴───────────┴────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────┴────────────────────────────────────────────┴────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────────────────────────┘
```


### `enriched_entrypoint_traces` Table 

Refer to [`enriched_entrypoint_traces.sql.j2`](../../templates/account_abstraction/enriched_entrypoint_traces.sql.j2)

This table includes all the pre-filtered traces from EntryPoint event emitting transactions. 

The table is enriched by decoding the `innerHandleOp` traces to obtain the `sender` for each UserOp
and then joining this back to all `innerHandleOp` subtraces. 

For each subtrace of an `innerHandleOp` trace we include the following enriched columns:

* `matched_userop_trace_address`. The trace address of the parent `innerHandleOp` trace.
* `matched_userop_sender`.  The `sender` value that was obtained by decoding the parent `innerHandleOp` trace.
* `is_from_matched_userop_sender`.  A convenience column meant for readers to filter out traces where the userop sender is the trace from address.


#### Schema


| column_name                     | column_type |
|---------------------------------|-------------|
| dt                              | DATE        |
| chain                           | VARCHAR     |
| chain_id                        | INTEGER     |
| network                         | VARCHAR     |
| block_timestamp                 | UINTEGER    |
| block_number                    | BIGINT      |
| block_hash                      | VARCHAR     |
| transaction_hash                | VARCHAR     |
| transaction_index               | BIGINT      |
| from_address                    | VARCHAR     |
| to_address                      | VARCHAR     |
| value_lossless                  | VARCHAR     |
| input                           | VARCHAR     |
| output                          | VARCHAR     |
| trace_type                      | VARCHAR     |
| call_type                       | VARCHAR     |
| reward_type                     | VARCHAR     |
| gas                             | BIGINT      |
| gas_used                        | BIGINT      |
| subtraces                       | BIGINT      |
| trace_address                   | VARCHAR     |
| error                           | VARCHAR     |
| status                          | BIGINT      |
| trace_root                      | INTEGER     |
| method_id                       | VARCHAR     |
| innerhandleop_decodeerror       | VARCHAR     |
| innerhandleop_opinfo_sender     | VARCHAR     |
| innerhandleop_opinfo_paymaster  | VARCHAR     |
| innerhandleop_opinfo_userophash | VARCHAR     |
| innerhandleop_opinfo            | VARCHAR     |
| innerhandleop_context           | VARCHAR     |
| innerhandleop_calldata          | VARCHAR     |
| matched_userop_trace_address    | VARCHAR     |
| matched_userop_sender           | VARCHAR     |
| is_from_matched_userop_sender   | BOOLEAN     |

#### Sample Data: Standard AA Bundle

Abbreviated result meant to highlight the enriched columns:
```
┌────────────────────────────────────────────────────────────────────┬────────────┬───────────────┬──────────────────────────────┬────────────────────────────────────────────┬───────────────────────────────┬────────────────────────────────────────────┬────────────────────────────────────────────┬────────────────────────────────────────────┐
│                          transaction_hash                          │ trace_root │ trace_address │ matched_userop_trace_address │           matched_userop_sender            │ is_from_matched_userop_sender │        innerhandleop_opinfo_sender         │                from_address                │                 to_address                 │
│                              varchar                               │   int32    │    varchar    │           varchar            │                  varchar                   │            boolean            │                  varchar                   │                  varchar                   │                  varchar                   │
├────────────────────────────────────────────────────────────────────┼────────────┼───────────────┼──────────────────────────────┼────────────────────────────────────────────┼───────────────────────────────┼────────────────────────────────────────────┼────────────────────────────────────────────┼────────────────────────────────────────────┤
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │         -1 │               │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0x3680e234283e149c859f9c173327676eb31e6f2c │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          0 │ 0             │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0xbc10fc416af0bbad9f3be804c028d02f42729be9 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          0 │ 0,0,0         │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0xbc10fc416af0bbad9f3be804c028d02f42729be9 │ 0x0000000000000000000000000000000000000001 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          1 │ 1             │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x2faeb0760d4230ef2ac21496bb4f0b47d634fd4c │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          1 │ 1,0           │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0x2faeb0760d4230ef2ac21496bb4f0b47d634fd4c │ 0x0000000000000000000000000000000000000001 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          2 │ 2             │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x2eabe2605c7c6b4318096026205c8b63fd6f833f │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          2 │ 2,0,0         │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0x2eabe2605c7c6b4318096026205c8b63fd6f833f │ 0x0000000000000000000000000000000000000001 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          3 │ 3             │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0xb174c63cc8272a81a17fa215489be1348b6a7bf9 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          3 │ 3,0,0         │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0xb174c63cc8272a81a17fa215489be1348b6a7bf9 │ 0x0000000000000000000000000000000000000001 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          4 │ 4             │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x2534b7395f210c35c7b2b4ace6855c645704e2d3 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          4 │ 4,0,0         │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0x2534b7395f210c35c7b2b4ace6855c645704e2d3 │ 0x0000000000000000000000000000000000000001 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          5 │ 5             │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x074b61bba4871f869f7bdb1aff1da17f497673b1 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          5 │ 5,0,0         │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0x074b61bba4871f869f7bdb1aff1da17f497673b1 │ 0x0000000000000000000000000000000000000001 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          6 │ 6             │ 6                            │ 0xbc10fc416af0bbad9f3be804c028d02f42729be9 │ false                         │ 0xbc10fc416af0bbad9f3be804c028d02f42729be9 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          6 │ 6,0           │ 6                            │ 0xbc10fc416af0bbad9f3be804c028d02f42729be9 │ false                         │ NULL                                       │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0xbc10fc416af0bbad9f3be804c028d02f42729be9 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          6 │ 6,0,0,0       │ 6                            │ 0xbc10fc416af0bbad9f3be804c028d02f42729be9 │ true                          │ NULL                                       │ 0xbc10fc416af0bbad9f3be804c028d02f42729be9 │ 0xe55fee191604cdbeb874f87a28ca89aed401c303 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          6 │ 6,1           │ 6                            │ 0xbc10fc416af0bbad9f3be804c028d02f42729be9 │ false                         │ NULL                                       │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x2faeb0760d4230ef2ac21496bb4f0b47d634fd4c │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          7 │ 7             │ 7                            │ 0x2eabe2605c7c6b4318096026205c8b63fd6f833f │ false                         │ 0x2eabe2605c7c6b4318096026205c8b63fd6f833f │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          7 │ 7,0           │ 7                            │ 0x2eabe2605c7c6b4318096026205c8b63fd6f833f │ false                         │ NULL                                       │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x2eabe2605c7c6b4318096026205c8b63fd6f833f │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          7 │ 7,0,0,0       │ 7                            │ 0x2eabe2605c7c6b4318096026205c8b63fd6f833f │ true                          │ NULL                                       │ 0x2eabe2605c7c6b4318096026205c8b63fd6f833f │ 0xe55fee191604cdbeb874f87a28ca89aed401c303 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          8 │ 8             │ 8                            │ 0xb174c63cc8272a81a17fa215489be1348b6a7bf9 │ false                         │ 0xb174c63cc8272a81a17fa215489be1348b6a7bf9 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          8 │ 8,0           │ 8                            │ 0xb174c63cc8272a81a17fa215489be1348b6a7bf9 │ false                         │ NULL                                       │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0xb174c63cc8272a81a17fa215489be1348b6a7bf9 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          8 │ 8,0,0,0       │ 8                            │ 0xb174c63cc8272a81a17fa215489be1348b6a7bf9 │ true                          │ NULL                                       │ 0xb174c63cc8272a81a17fa215489be1348b6a7bf9 │ 0xe55fee191604cdbeb874f87a28ca89aed401c303 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          9 │ 9             │ 9                            │ 0x2534b7395f210c35c7b2b4ace6855c645704e2d3 │ false                         │ 0x2534b7395f210c35c7b2b4ace6855c645704e2d3 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          9 │ 9,0           │ 9                            │ 0x2534b7395f210c35c7b2b4ace6855c645704e2d3 │ false                         │ NULL                                       │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x2534b7395f210c35c7b2b4ace6855c645704e2d3 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │          9 │ 9,0,0,0       │ 9                            │ 0x2534b7395f210c35c7b2b4ace6855c645704e2d3 │ true                          │ NULL                                       │ 0x2534b7395f210c35c7b2b4ace6855c645704e2d3 │ 0xe55fee191604cdbeb874f87a28ca89aed401c303 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │         10 │ 10            │ 10                           │ 0x074b61bba4871f869f7bdb1aff1da17f497673b1 │ false                         │ 0x074b61bba4871f869f7bdb1aff1da17f497673b1 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │         10 │ 10,0          │ 10                           │ 0x074b61bba4871f869f7bdb1aff1da17f497673b1 │ false                         │ NULL                                       │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x074b61bba4871f869f7bdb1aff1da17f497673b1 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │         10 │ 10,0,0,0      │ 10                           │ 0x074b61bba4871f869f7bdb1aff1da17f497673b1 │ true                          │ NULL                                       │ 0x074b61bba4871f869f7bdb1aff1da17f497673b1 │ 0xe55fee191604cdbeb874f87a28ca89aed401c303 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │         11 │ 11            │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x3680e234283e149c859f9c173327676eb31e6f2c │
├────────────────────────────────────────────────────────────────────┴────────────┴───────────────┴──────────────────────────────┴────────────────────────────────────────────┴───────────────────────────────┴────────────────────────────────────────────┴────────────────────────────────────────────┴────────────────────────────────────────────┤
│ 30 rows                                                                                                                                                                                                                                                                                                                                  9 columns │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```


#### Sample Data: Case when the EntryPoint contract is called within an outer transaction

Here `chain='base'`.

```
┌────────────────────────────────────────────────────────────────────┬────────────┬───────────────┬──────────────────────────────┬────────────────────────────────────────────┬───────────────────────────────┬────────────────────────────────────────────┬────────────────────────────────────────────┬────────────────────────────────────────────┐
│                          transaction_hash                          │ trace_root │ trace_address │ matched_userop_trace_address │           matched_userop_sender            │ is_from_matched_userop_sender │        innerhandleop_opinfo_sender         │                from_address                │                 to_address                 │
│                              varchar                               │   int32    │    varchar    │           varchar            │                  varchar                   │            boolean            │                  varchar                   │                  varchar                   │                  varchar                   │
├────────────────────────────────────────────────────────────────────┼────────────┼───────────────┼──────────────────────────────┼────────────────────────────────────────────┼───────────────────────────────┼────────────────────────────────────────────┼────────────────────────────────────────────┼────────────────────────────────────────────┤
│ 0x6e7b82e957641c9178e0aa7d84d9ca170898d984928e271c714b2758ae57e461 │         -1 │               │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0xf70da97812cb96acdf810712aa562db8dfa3dbef │ 0xb90ed4c123843cbfd66b11411ee7694ef37e6e72 │
│ 0x6e7b82e957641c9178e0aa7d84d9ca170898d984928e271c714b2758ae57e461 │          0 │ 0,0           │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0xb90ed4c123843cbfd66b11411ee7694ef37e6e72 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │
│ 0x6e7b82e957641c9178e0aa7d84d9ca170898d984928e271c714b2758ae57e461 │          0 │ 0,0,0         │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x4a913953a57658ad6703967871167f22ce6e6f5a │
│ 0x6e7b82e957641c9178e0aa7d84d9ca170898d984928e271c714b2758ae57e461 │          0 │ 0,0,0,0,0     │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0x4a913953a57658ad6703967871167f22ce6e6f5a │ 0x0000000000000000000000000000000000000002 │
│ 0x6e7b82e957641c9178e0aa7d84d9ca170898d984928e271c714b2758ae57e461 │          0 │ 0,0,0,0,1     │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0x4a913953a57658ad6703967871167f22ce6e6f5a │ 0x0000000000000000000000000000000000000002 │
│ 0x6e7b82e957641c9178e0aa7d84d9ca170898d984928e271c714b2758ae57e461 │          0 │ 0,0,0,0,2     │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0x4a913953a57658ad6703967871167f22ce6e6f5a │ 0x0000000000000000000000000000000000000100 │
│ 0x6e7b82e957641c9178e0aa7d84d9ca170898d984928e271c714b2758ae57e461 │          0 │ 0,0,1         │ 0,0,1                        │ 0x4a913953a57658ad6703967871167f22ce6e6f5a │ false                         │ 0x4a913953a57658ad6703967871167f22ce6e6f5a │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │
│ 0x6e7b82e957641c9178e0aa7d84d9ca170898d984928e271c714b2758ae57e461 │          0 │ 0,0,1,0       │ 0,0,1                        │ 0x4a913953a57658ad6703967871167f22ce6e6f5a │ false                         │ NULL                                       │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x4a913953a57658ad6703967871167f22ce6e6f5a │
│ 0x6e7b82e957641c9178e0aa7d84d9ca170898d984928e271c714b2758ae57e461 │          0 │ 0,0,1,0,0,0   │ 0,0,1                        │ 0x4a913953a57658ad6703967871167f22ce6e6f5a │ true                          │ NULL                                       │ 0x4a913953a57658ad6703967871167f22ce6e6f5a │ 0x4a913953a57658ad6703967871167f22ce6e6f5a │
│ 0x6e7b82e957641c9178e0aa7d84d9ca170898d984928e271c714b2758ae57e461 │          0 │ 0,0,2         │ NULL                         │ NULL                                       │ NULL                          │ NULL                                       │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x4a913953a57658ad6703967871167f22ce6e6f5a │
├────────────────────────────────────────────────────────────────────┴────────────┴───────────────┴──────────────────────────────┴────────────────────────────────────────────┴───────────────────────────────┴────────────────────────────────────────────┴────────────────────────────────────────────┴────────────────────────────────────────────┤
│ 10 rows                                                                                                                                                                                                                                                                                                                                  9 columns │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

```


### Sample Data: All fields for a single row that IS an `innerHandleOp` trace.

Large values are truncated.

```
┌─────────────────────────────────┬─────────┬──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│             Column              │  Type   │                                                                                                                                                          Row 1                                                                                                                                                           │
├─────────────────────────────────┼─────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ dt                              │ date    │                                                                                                                                                                                                                                                                                                               2025-02-01 │
│ chain                           │ varchar │                                                                                                                                                                                                                                                                                                                     base │
│ chain_id                        │ int32   │                                                                                                                                                                                                                                                                                                                     8453 │
│ network                         │ varchar │                                                                                                                                                                                                                                                                                                                  mainnet │
│ block_timestamp                 │ uint32  │                                                                                                                                                                                                                                                                                                               1738431529 │
│ block_number                    │ int64   │                                                                                                                                                                                                                                                                                                                 25821091 │
│ block_hash                      │ varchar │                                                                                                                                                                                                                                                       0x1e914a9c01661fcc72bb0e277fe4fa3898cbdecff9225df8d658760ecda59e5d │
│ transaction_hash                │ varchar │                                                                                                                                                                                                                                                       0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │
│ transaction_index               │ int64   │                                                                                                                                                                                                                                                                                                                      141 │
│ from_address                    │ varchar │                                                                                                                                                                                                                                                                               0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │
│ to_address                      │ varchar │                                                                                                                                                                                                                                                                               0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │
│ value_lossless                  │ varchar │                                                                                                                                                                                                                                                                                                                        0 │
│ input                           │ varchar │  0x1d73275600000000000000000000000000000000000000000000000000000000000001c0000000000000000000000000bc10fc416af0bbad9f3be804c028d02f42729be900000000000000000000000000000000000000000000000000000000000000170000000000000000000000000000000000000000000000000000000000009bb800000000000000000000000000000000000000000000… │
│ output                          │ varchar │                                                                                                                                                                                                                                                       0x0000000000000000000000000000000000000000000000000000010ccb4183a4 │
│ trace_type                      │ varchar │                                                                                                                                                                                                                                                                                                                     call │
│ call_type                       │ varchar │                                                                                                                                                                                                                                                                                                                     call │
│ reward_type                     │ varchar │                                                                                                                                                                                                                                                                                                                          │
│ gas                             │ int64   │                                                                                                                                                                                                                                                                                                                   892147 │
│ gas_used                        │ int64   │                                                                                                                                                                                                                                                                                                                    48116 │
│ subtraces                       │ int64   │                                                                                                                                                                                                                                                                                                                        2 │
│ trace_address                   │ varchar │                                                                                                                                                                                                                                                                                                                        6 │
│ error                           │ varchar │                                                                                                                                                                                                                                                                                                                          │
│ status                          │ int64   │                                                                                                                                                                                                                                                                                                                        1 │
│ trace_root                      │ int32   │                                                                                                                                                                                                                                                                                                                        6 │
│ method_id                       │ varchar │                                                                                                                                                                                                                                                                                                               0x1d732756 │
│ innerhandleop_decodeerror       │ varchar │                                                                                                                                                                                                                                                                                                                     NULL │
│ innerhandleop_opinfo_sender     │ varchar │                                                                                                                                                                                                                                                                               0xbc10fc416af0bbad9f3be804c028d02f42729be9 │
│ innerhandleop_opinfo_paymaster  │ varchar │                                                                                                                                                                                                                                                                               0x2faeb0760d4230ef2ac21496bb4f0b47d634fd4c │
│ innerhandleop_opinfo_userophash │ varchar │                                                                                                                                                                                                                                                       0xc3cde5aba1d69292868339030c7a4af4a796498a73e9534c213693314983f54e │
│ innerhandleop_opinfo            │ varchar │                                                         {"mUserOp": {"nonce": "23", "callGasLimit": "39864", "verificationGasLimit": "64832", "preVerificationGas": "153504", "maxFeePerGas": "11756815", "maxPriorityFeePerGas": "1000000"}, "prefund": "4560045293160", "contextOffset": "3360", "preOpGas": "203539"} │
│ innerhandleop_context           │ varchar │  0x000000000000000000000000bc10fc416af0bbad9f3be804c028d02f42729be9c3cde5aba1d69292868339030c7a4af4a796498a73e9534c213693314983f54e00000000000000000000000000000000eb35ca708c8a4700ad82f02947b87df000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000… │
│ innerhandleop_calldata          │ varchar │  0xb61d27f6000000000000000000000000e55fee191604cdbeb874f87a28ca89aed401c303000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000600000000000000000000000000000000000000000000000000000000000000004183ff085000000000000000000000000000000000000… │
│ matched_userop_trace_address    │ varchar │                                                                                                                                                                                                                                                                                                                        6 │
│ matched_userop_sender           │ varchar │                                                                                                                                                                                                                                                                               0xbc10fc416af0bbad9f3be804c028d02f42729be9 │
│ is_from_matched_userop_sender   │ boolean │                                                                                                                                                                                                                                                                                                                    false │
└─────────────────────────────────┴─────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Sample Data: All fields for a single row that IS NOT an `innerHandleOp` trace.

Large values are truncated.

```
┌─────────────────────────────────┬─────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│             Column              │  Type   │                                                                                                                               Row 1                                                                                                                                │
├─────────────────────────────────┼─────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ dt                              │ date    │                                                                                                                                                                                                                                                         2025-02-01 │
│ chain                           │ varchar │                                                                                                                                                                                                                                                               base │
│ chain_id                        │ int32   │                                                                                                                                                                                                                                                               8453 │
│ network                         │ varchar │                                                                                                                                                                                                                                                            mainnet │
│ block_timestamp                 │ uint32  │                                                                                                                                                                                                                                                         1738431529 │
│ block_number                    │ int64   │                                                                                                                                                                                                                                                           25821091 │
│ block_hash                      │ varchar │                                                                                                                                                                                                 0x1e914a9c01661fcc72bb0e277fe4fa3898cbdecff9225df8d658760ecda59e5d │
│ transaction_hash                │ varchar │                                                                                                                                                                                                 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │
│ transaction_index               │ int64   │                                                                                                                                                                                                                                                                141 │
│ from_address                    │ varchar │                                                                                                                                                                                                                         0x074b61bba4871f869f7bdb1aff1da17f497673b1 │
│ to_address                      │ varchar │                                                                                                                                                                                                                         0x0000000000000000000000000000000000000001 │
│ value_lossless                  │ varchar │                                                                                                                                                                                                                                                                  0 │
│ input                           │ varchar │ 0x015a2068426eab06eea0e2c9f44e1dc59d3a9b96c7d1393edcd68b8fc59fa83b000000000000000000000000000000000000000000000000000000000000001b2dbbe87d249df2a55e3c01468acd24a4ee047e15398886a1bb6790f3a242527265c2a3ea4f34fefb0e02c04fd1143db3b5e3c62bcfaf8123935699f3eeedb08e │
│ output                          │ varchar │                                                                                                                                                                                                 0x0000000000000000000000005f0537676fefec7c8a9772bb23ab9d32c64b5608 │
│ trace_type                      │ varchar │                                                                                                                                                                                                                                                               call │
│ call_type                       │ varchar │                                                                                                                                                                                                                                                         staticcall │
│ reward_type                     │ varchar │                                                                                                                                                                                                                                                                    │
│ gas                             │ int64   │                                                                                                                                                                                                                                                              58946 │
│ gas_used                        │ int64   │                                                                                                                                                                                                                                                               3000 │
│ subtraces                       │ int64   │                                                                                                                                                                                                                                                                  0 │
│ trace_address                   │ varchar │                                                                                                                                                                                                                                                              5,0,0 │
│ error                           │ varchar │                                                                                                                                                                                                                                                                    │
│ status                          │ int64   │                                                                                                                                                                                                                                                                  1 │
│ trace_root                      │ int32   │                                                                                                                                                                                                                                                                  5 │
│ method_id                       │ varchar │                                                                                                                                                                                                                                                         0x015a2068 │
│ innerhandleop_decodeerror       │ varchar │                                                                                                                                                                                                                                                               NULL │
│ innerhandleop_opinfo_sender     │ varchar │                                                                                                                                                                                                                                                               NULL │
│ innerhandleop_opinfo_paymaster  │ varchar │                                                                                                                                                                                                                                                               NULL │
│ innerhandleop_opinfo_userophash │ varchar │                                                                                                                                                                                                                                                               NULL │
│ innerhandleop_opinfo            │ varchar │                                                                                                                                                                                                                                                               NULL │
│ innerhandleop_context           │ varchar │                                                                                                                                                                                                                                                               NULL │
│ innerhandleop_calldata          │ varchar │                                                                                                                                                                                                                                                               NULL │
│ matched_userop_trace_address    │ varchar │                                                                                                                                                                                                                                                               NULL │
│ matched_userop_sender           │ varchar │                                                                                                                                                                                                                                                               NULL │
│ is_from_matched_userop_sender   │ boolean │                                                                                                                                                                                                                                                               NULL │
└─────────────────────────────────┴─────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```