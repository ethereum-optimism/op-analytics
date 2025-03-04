# Account Abstraction Data Pipeline


## Overview

The pipeline is split into two steps (1) pre-filtering, (2) decoding + enrichment.

Pre-filtering searches raw logs to find events emitted by the AA EntryPoint contract. As a filter
condition we use the set of topic0 hashs on all indexed EntryPoint v0_6_0 and v0_7_0 events.

The filtered logs give us the set of AA transactions. We keep traces for those transactions
(excluding delegatecall traces).

In part 2 the pre-filtered logs and traces are decoded and further enriched to produce the final
output tables.

## Output Table Schemas


### `useroperationevent_logs` Table

| Column Name       | Column Type | Description
|-------------------|-------------|--
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
| userophash        | VARCHAR     |  topic1
| sender            | VARCHAR     |  topic2
| paymaster         | VARCHAR     |  topic3
| nonce             | VARCHAR     |  Decoded from log data
| success           | BOOLEAN     |  Decoded from log data
| actualGasCost     | VARCHAR     |  Decoded from log data
| actualGasUsed     | VARCHAR     |  Decoded from log data


### `enriched_entrypoint_traces` Table


| Column Name                 | Column Type | Description
|-----------------------------|-------------|--
| dt                          | DATE        |
| chain                       | VARCHAR     |
| chain_id                    | INTEGER     |
| network                     | VARCHAR     |
| block_timestamp             | UINTEGER    |
| block_number                | BIGINT      |
| block_hash                  | VARCHAR     |
| transaction_hash            | VARCHAR     |
| transaction_index           | BIGINT      |
| from_address                | VARCHAR     |
| to_address                  | VARCHAR     |
| value                       | VARCHAR     |
| input                       | VARCHAR     |
| output                      | VARCHAR     |
| trace_type                  | VARCHAR     |
| call_type                   | VARCHAR     |
| reward_type                 | VARCHAR     |
| gas                         | BIGINT      |
| gas_used                    | BIGINT      |
| subtraces                   | BIGINT      |
| trace_address               | VARCHAR     |
| error                       | VARCHAR     |
| status                      | BIGINT      |
| trace_root                  | INTEGER     |
| method_id                   | VARCHAR     |
| tx_from_address             | VARCHAR     |  Usually this is the blundler contract address.
| bundler_address             | VARCHAR     |  Address that calls the handleOp method on the EntryPoint.
| entrypoint_contract_address | VARCHAR     |
| entrypoint_contract_version | VARCHAR     |  Decoded from innerHandleOp.
| innerhandleop_trace_address | VARCHAR     |  Trace address of the innerHandleOp call.
| is_innerhandleop            | BOOLEAN     |  Convenience boolean flag.
| is_from_sender              | BOOLEAN     |  Convenience boolean flag.
| userop_sender               | VARCHAR     |  Decoded from innerHandleOp.  Used to join back to the UserOperationEvent log.
| userop_paymaster            | VARCHAR     |  Decoded from innerHandleOp.  Used to join back to the UserOperationEvent log.
| userop_hash                 | VARCHAR     |  Decoded from innerHandleOp.  Used to join back to the UserOperationEvent log.
| userop_calldata             | VARCHAR     |  Decoded from innerHandleOp.
| innerhandleop_decodeerror   | VARCHAR     |  Decoded from innerHandleOp.
| innerhandleop_opinfo        | VARCHAR     |  Decoded from innerHandleOp.
| innerhandleop_context       | VARCHAR     |  Decoded from innerHandleOp.
| useropevent_nonce           | VARCHAR     |  Joined from useroperationevent_logs.
| useropevent_success         | BOOLEAN     |  Joined from useroperationevent_logs.
| useropevent_actualgascost   | VARCHAR     |  Joined from useroperationevent_logs.
| useropevent_actualgasused   | VARCHAR     |  Joined from useroperationevent_logs.
| userop_idx                  | BIGINT      |  The row_number() of traces where from_address = sender ordered by trace_addres. This is equal to 1 for the userOp call.
| useropevent_actualgascost_eth | DOUBLE      | Actual gas cost converted to double for convenience.


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

#### Sample row

```python
{
    "dt": datetime.date(2024, 9, 17),
    "chain": "base",
    "chain_id": 8453,
    "network": "mainnet",
    "block_timestamp": 1726531547,
    "block_number": 19871100,
    "block_hash": "0x2070ae0725b59739488aaa77096d55c616d366ec8c021700a189f6cd14e4162b",
    "transaction_hash": "0xeb8aed49895870a10eaee7fc6b38d00e6081816e1c7309fd6829f9299a386b58",
    "transaction_index": 40,
    "log_index": 71,
    "contract_address": "0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789",
    "userophash": "0x643741a0cac24596076e66f13b742da1ef5f0ec49a217571e72f5f2986e2cd6c",
    "sender": "0x118bd3dc35b5ff4d5a20b8f48b1824f471ebc563",
    "paymaster": "0x0000000000000000000000000000000000000000",
    "nonce": "69",
    "success": True,
    "actualGasCost": "1192842115198",
    "actualGasUsed": "225523",
}
```


### `enriched_entrypoint_traces` Table

Refer to [`enriched_entrypoint_traces.sql.j2`](../../templates/account_abstraction/enriched_entrypoint_traces.sql.j2)

The `innerHandleOp` traces are decoded and joined to the `useroperationevent_logs` table. This result
is then self-joined back to the entrypoint contract traces which result in enrichment of all traces
with the corresponding fields from the `innerHandleOp` and the emitted `UserOperationEvent`.
The self join applies only to traces that are subtraces of the `innerHandleOp`.

Only traces that are subtraces of an `innerHandleOp` are kept in the table. Below we show an example.


### Sample row

This example also has the `is_from_sender = true` boolean flag, which means that the trace
`from_address` matches the UserOp `sender` address.


```python
{
    "dt": datetime.date(2024, 9, 17),
    "chain": "base",
    "chain_id": 8453,
    "network": "mainnet",
    "block_timestamp": 1726531547,
    "block_number": 19871100,
    "block_hash": "0x2070ae0725b59739488aaa77096d55c616d366ec8c021700a189f6cd14e4162b",
    "transaction_hash": "0xeb8aed49895870a10eaee7fc6b38d00e6081816e1c7309fd6829f9299a386b58",
    "transaction_index": 40,
    "from_address": "0x118bd3dc35b5ff4d5a20b8f48b1824f471ebc563",
    "to_address": "0x80625310db240631a91f61659ccc550ea9761742",
    "value": "0",
    "input": "0x29c911c9000000000000000000000000118bd3dc35b5ff4d5a20b8f48b1824f471ebc563000000000000000000000000000000000000000000000000000000000000119d0000000000000000000000000000000000000000000000000000000066e8d5e3000000000000000000000000000000000000000000000000000000000000001c4c315768fd4d0e235d8b90a5a972caafb5dfe134baf56bdeb241ddd93392fd290b1f611e6c488670168466aabea54791faa68dc9c66bb0e2a69b3a840926029a",
    "output": "",
    "trace_type": "call",
    "call_type": "call",
    "reward_type": "",
    "gas": 132775,
    "gas_used": 123267,
    "subtraces": 3,
    "trace_address": "1,0,0,0",
    "error": "",
    "status": 1,
    "trace_root": 1,
    "method_id": "0x29c911c9",
    "tx_from_address": "0xc4a4e8ae10b82a954519ca2ecc9efc8f77819e86",
    "bundler_address": "0xc4a4e8ae10b82a954519ca2ecc9efc8f77819e86",
    "entrypoint_contract_address": "0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789",
    "is_innerhandleop": False,
    "is_from_sender": True,
    "userop_sender": "0x118bd3dc35b5ff4d5a20b8f48b1824f471ebc563",
    "userop_paymaster": "0x0000000000000000000000000000000000000000",
    "userop_hash": "0x643741a0cac24596076e66f13b742da1ef5f0ec49a217571e72f5f2986e2cd6c",
    "userop_calldata": "0xb61d27f600000000000000000000000080625310db240631a91f61659ccc550ea97617420000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000c429c911c9000000000000000000000000118bd3dc35b5ff4d5a20b8f48b1824f471ebc563000000000000000000000000000000000000000000000000000000000000119d0000000000000000000000000000000000000000000000000000000066e8d5e3000000000000000000000000000000000000000000000000000000000000001c4c315768fd4d0e235d8b90a5a972caafb5dfe134baf56bdeb241ddd93392fd290b1f611e6c488670168466aabea54791faa68dc9c66bb0e2a69b3a840926029a00000000000000000000000000000000000000000000000000000000",
    "innerhandleop_decodeerror": None,
    "innerhandleop_opinfo": '{"mUserOp": {"nonce": "69", "callGasLimit": "141709", "verificationGasLimit": "43943", "preVerificationGas": "57052", "maxFeePerGas": "6130913", "maxPriorityFeePerGas": "1000000"}, "prefund": "1487997108752", "contextOffset": "96", "preOpGas": "96837"}',
    "innerhandleop_context": "0x",
    "innerhandleop_trace_address": "1",
    "entrypoint_contract_version": "v6",
    "useropevent_nonce": "69",
    "useropevent_success": True,
    "useropevent_actualgascost": "1192842115198",
    "useropevent_actualgasused": "225523",
    "userop_idx": 1,
    "useropevent_actualgascost_eth": 1.192842115198e-06,
}
```


## Noteworthy Cases


###  Nested AA Bundles

Usually the `innerHandleOp` trace is a level 1 trace (no commas on the `trace_address`). In the
following example we have a bundle with 5`innerHandleOp` traces (`6` to `10`).

Notice that `tx_from_address == bundler_address`.

```
┌────────────────────────────────────────────────────────────────────┬────────────────────────────────────────────┬────────────────────────────────────────────┬────────────┬───────────────┬─────────────────────────────┬────────────────┬──────────────────┬─────────────────────┬────────┬─────────┬────────────────────────────────────────────┬────────────────────────────────────────────┬────────────────────────────────────────────┬────────────┬───────────────────────────────┐
│                          transaction_hash                          │              tx_from_address               │              bundler_address               │ trace_root │ trace_address │ innerhandleop_trace_address │ is_from_sender │ is_innerhandleop │ useropevent_success │ status │  error  │               userop_sender                │                from_address                │                 to_address                 │ userop_idx │ useropevent_actualgascost_eth │
│                              varchar                               │                  varchar                   │                  varchar                   │   int32    │    varchar    │           varchar           │    boolean     │     boolean      │       boolean       │ int64  │ varchar │                  varchar                   │                  varchar                   │                  varchar                   │   int64    │            double             │
├────────────────────────────────────────────────────────────────────┼────────────────────────────────────────────┼────────────────────────────────────────────┼────────────┼───────────────┼─────────────────────────────┼────────────────┼──────────────────┼─────────────────────┼────────┼─────────┼────────────────────────────────────────────┼────────────────────────────────────────────┼────────────────────────────────────────────┼────────────┼───────────────────────────────┤
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │ 0x3680e234283e149c859f9c173327676eb31e6f2c │ 0x3680e234283e149c859f9c173327676eb31e6f2c │          6 │ 6             │ 6                           │ false          │ true             │ true                │      1 │         │ 0xbc10fc416af0bbad9f3be804c028d02f42729be9 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │       NULL │            1.154461303716e-06 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │ 0x3680e234283e149c859f9c173327676eb31e6f2c │ 0x3680e234283e149c859f9c173327676eb31e6f2c │          6 │ 6,0           │ 6                           │ false          │ false            │ true                │      1 │         │ 0xbc10fc416af0bbad9f3be804c028d02f42729be9 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0xbc10fc416af0bbad9f3be804c028d02f42729be9 │       NULL │            1.154461303716e-06 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │ 0x3680e234283e149c859f9c173327676eb31e6f2c │ 0x3680e234283e149c859f9c173327676eb31e6f2c │          6 │ 6,0,0,0       │ 6                           │ true           │ false            │ true                │      1 │         │ 0xbc10fc416af0bbad9f3be804c028d02f42729be9 │ 0xbc10fc416af0bbad9f3be804c028d02f42729be9 │ 0xe55fee191604cdbeb874f87a28ca89aed401c303 │          1 │            1.154461303716e-06 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │ 0x3680e234283e149c859f9c173327676eb31e6f2c │ 0x3680e234283e149c859f9c173327676eb31e6f2c │          6 │ 6,1           │ 6                           │ false          │ false            │ true                │      1 │         │ 0xbc10fc416af0bbad9f3be804c028d02f42729be9 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x2faeb0760d4230ef2ac21496bb4f0b47d634fd4c │       NULL │            1.154461303716e-06 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │ 0x3680e234283e149c859f9c173327676eb31e6f2c │ 0x3680e234283e149c859f9c173327676eb31e6f2c │          7 │ 7             │ 7                           │ false          │ true             │ true                │      1 │         │ 0x2eabe2605c7c6b4318096026205c8b63fd6f833f │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │       NULL │             5.32333837539e-07 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │ 0x3680e234283e149c859f9c173327676eb31e6f2c │ 0x3680e234283e149c859f9c173327676eb31e6f2c │          7 │ 7,0           │ 7                           │ false          │ false            │ true                │      1 │         │ 0x2eabe2605c7c6b4318096026205c8b63fd6f833f │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x2eabe2605c7c6b4318096026205c8b63fd6f833f │       NULL │             5.32333837539e-07 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │ 0x3680e234283e149c859f9c173327676eb31e6f2c │ 0x3680e234283e149c859f9c173327676eb31e6f2c │          7 │ 7,0,0,0       │ 7                           │ true           │ false            │ true                │      1 │         │ 0x2eabe2605c7c6b4318096026205c8b63fd6f833f │ 0x2eabe2605c7c6b4318096026205c8b63fd6f833f │ 0xe55fee191604cdbeb874f87a28ca89aed401c303 │          1 │             5.32333837539e-07 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │ 0x3680e234283e149c859f9c173327676eb31e6f2c │ 0x3680e234283e149c859f9c173327676eb31e6f2c │          8 │ 8             │ 8                           │ false          │ true             │ true                │      1 │         │ 0xb174c63cc8272a81a17fa215489be1348b6a7bf9 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │       NULL │             5.32244852397e-07 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │ 0x3680e234283e149c859f9c173327676eb31e6f2c │ 0x3680e234283e149c859f9c173327676eb31e6f2c │          8 │ 8,0           │ 8                           │ false          │ false            │ true                │      1 │         │ 0xb174c63cc8272a81a17fa215489be1348b6a7bf9 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0xb174c63cc8272a81a17fa215489be1348b6a7bf9 │       NULL │             5.32244852397e-07 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │ 0x3680e234283e149c859f9c173327676eb31e6f2c │ 0x3680e234283e149c859f9c173327676eb31e6f2c │          8 │ 8,0,0,0       │ 8                           │ true           │ false            │ true                │      1 │         │ 0xb174c63cc8272a81a17fa215489be1348b6a7bf9 │ 0xb174c63cc8272a81a17fa215489be1348b6a7bf9 │ 0xe55fee191604cdbeb874f87a28ca89aed401c303 │          1 │             5.32244852397e-07 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │ 0x3680e234283e149c859f9c173327676eb31e6f2c │ 0x3680e234283e149c859f9c173327676eb31e6f2c │          9 │ 9             │ 9                           │ false          │ true             │ true                │      1 │         │ 0x2534b7395f210c35c7b2b4ace6855c645704e2d3 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │       NULL │             5.32155867255e-07 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │ 0x3680e234283e149c859f9c173327676eb31e6f2c │ 0x3680e234283e149c859f9c173327676eb31e6f2c │          9 │ 9,0           │ 9                           │ false          │ false            │ true                │      1 │         │ 0x2534b7395f210c35c7b2b4ace6855c645704e2d3 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x2534b7395f210c35c7b2b4ace6855c645704e2d3 │       NULL │             5.32155867255e-07 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │ 0x3680e234283e149c859f9c173327676eb31e6f2c │ 0x3680e234283e149c859f9c173327676eb31e6f2c │          9 │ 9,0,0,0       │ 9                           │ true           │ false            │ true                │      1 │         │ 0x2534b7395f210c35c7b2b4ace6855c645704e2d3 │ 0x2534b7395f210c35c7b2b4ace6855c645704e2d3 │ 0xe55fee191604cdbeb874f87a28ca89aed401c303 │          1 │             5.32155867255e-07 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │ 0x3680e234283e149c859f9c173327676eb31e6f2c │ 0x3680e234283e149c859f9c173327676eb31e6f2c │         10 │ 10            │ 10                          │ false          │ true             │ true                │      1 │         │ 0x074b61bba4871f869f7bdb1aff1da17f497673b1 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │       NULL │             5.31557689356e-07 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │ 0x3680e234283e149c859f9c173327676eb31e6f2c │ 0x3680e234283e149c859f9c173327676eb31e6f2c │         10 │ 10,0          │ 10                          │ false          │ false            │ true                │      1 │         │ 0x074b61bba4871f869f7bdb1aff1da17f497673b1 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x074b61bba4871f869f7bdb1aff1da17f497673b1 │       NULL │             5.31557689356e-07 │
│ 0x85dc9e8463b762edcd134a885a8575ed5c6ab0484223ff33856bb3fec838c552 │ 0x3680e234283e149c859f9c173327676eb31e6f2c │ 0x3680e234283e149c859f9c173327676eb31e6f2c │         10 │ 10,0,0,0      │ 10                          │ true           │ false            │ true                │      1 │         │ 0x074b61bba4871f869f7bdb1aff1da17f497673b1 │ 0x074b61bba4871f869f7bdb1aff1da17f497673b1 │ 0xe55fee191604cdbeb874f87a28ca89aed401c303 │          1 │             5.31557689356e-07 │
├────────────────────────────────────────────────────────────────────┴────────────────────────────────────────────┴────────────────────────────────────────────┴────────────┴───────────────┴─────────────────────────────┴────────────────┴──────────────────┴─────────────────────┴────────┴─────────┴────────────────────────────────────────────┴────────────────────────────────────────────┴────────────────────────────────────────────┴────────────┴───────────────────────────────┤
│ 16 rows                                                                                                                                                                                                                                                                                                                                                                                                                                                                       16 columns │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

We have also observed cases ([see basescan](https://basescan.org/tx/0x6e7b82e957641c9178e0aa7d84d9ca170898d984928e271c714b2758ae57e461))
where the EntryPoint contract is called from another transaction.
This results in `innerHandleOp` trace addresses that are further nested. In the following example
we see a bundle with one `innerHandleOp` at trace address `0,0,1`.

Notice that `tx_from_address != bundler_address`.
```
┌────────────────────────────────────────────────────────────────────┬────────────────────────────────────────────┬────────────────────────────────────────────┬────────────┬───────────────┬─────────────────────────────┬────────────────┬────────────┬──────────────────┬─────────────────────┬────────┬─────────┬────────────────────────────────────────────┬────────────────────────────────────────────┬────────────────────────────────────────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                          transaction_hash                          │              tx_from_address               │              bundler_address               │ trace_root │ trace_address │ innerhandleop_trace_address │ is_from_sender │ userop_idx │ is_innerhandleop │ useropevent_success │ status │  error  │               userop_sender                │                from_address                │                 to_address                 │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   input                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    │
│                              varchar                               │                  varchar                   │                  varchar                   │   int32    │    varchar    │           varchar           │    boolean     │   int64    │     boolean      │       boolean       │ int64  │ varchar │                  varchar                   │                  varchar                   │                  varchar                   │                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  varchar                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   │
├────────────────────────────────────────────────────────────────────┼────────────────────────────────────────────┼────────────────────────────────────────────┼────────────┼───────────────┼─────────────────────────────┼────────────────┼────────────┼──────────────────┼─────────────────────┼────────┼─────────┼────────────────────────────────────────────┼────────────────────────────────────────────┼────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ 0x6e7b82e957641c9178e0aa7d84d9ca170898d984928e271c714b2758ae57e461 │ 0xf70da97812cb96acdf810712aa562db8dfa3dbef │ 0xb90ed4c123843cbfd66b11411ee7694ef37e6e72 │          0 │ 0,0,1         │ 0,0,1                       │ false          │       NULL │ true             │ true                │      1 │         │ 0x4a913953a57658ad6703967871167f22ce6e6f5a │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x1d73275600000000000000000000000000000000000000000000000000000000000001c00000000000000000000000004a913953a57658ad6703967871167f22ce6e6f5a0000000000000000000000000000000000000000000021050000000000000002000000000000000000000000000000000000000000000000000000000001672e000000000000000000000000000000000000000000000000000000000008a29c00000000000000000000000000000000000000000000000000000000000279fe000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000a2130e6dff24327a7bbcc37c97d840a589965410639b8c01b7f47dd2cdb9e3e3000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000600000000000000000000000000000000000000000000000000000000000032ad500000000000000000000000000000000000000000000000000000000000002c000000000000000000000000000000000000000000000000000000000000000c42c2abd1e00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000240f0f3f24000000000000000000000000d118dd73914a2449cd997acb74eb0bc0f239c87000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000 │
│ 0x6e7b82e957641c9178e0aa7d84d9ca170898d984928e271c714b2758ae57e461 │ 0xf70da97812cb96acdf810712aa562db8dfa3dbef │ 0xb90ed4c123843cbfd66b11411ee7694ef37e6e72 │          0 │ 0,0,1,0       │ 0,0,1                       │ false          │       NULL │ false            │ true                │      1 │         │ 0x4a913953a57658ad6703967871167f22ce6e6f5a │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x4a913953a57658ad6703967871167f22ce6e6f5a │ 0x2c2abd1e00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000240f0f3f24000000000000000000000000d118dd73914a2449cd997acb74eb0bc0f239c87000000000000000000000000000000000000000000000000000000000                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 │
│ 0x6e7b82e957641c9178e0aa7d84d9ca170898d984928e271c714b2758ae57e461 │ 0xf70da97812cb96acdf810712aa562db8dfa3dbef │ 0xb90ed4c123843cbfd66b11411ee7694ef37e6e72 │          0 │ 0,0,1,0,0,0   │ 0,0,1                       │ true           │          1 │ false            │ true                │      1 │         │ 0x4a913953a57658ad6703967871167f22ce6e6f5a │ 0x4a913953a57658ad6703967871167f22ce6e6f5a │ 0x4a913953a57658ad6703967871167f22ce6e6f5a │ 0x0f0f3f24000000000000000000000000d118dd73914a2449cd997acb74eb0bc0f239c870                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 │
└────────────────────────────────────────────────────────────────────┴────────────────────────────────────────────┴────────────────────────────────────────────┴────────────┴───────────────┴─────────────────────────────┴────────────────┴────────────┴──────────────────┴─────────────────────┴────────┴─────────┴────────────────────────────────────────────┴────────────────────────────────────────────┴────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```


###  Successful userOp with failed user traces

This is an example that I don't understand very well, but I see a userOp that has two sender
traces. The first one (`3,0,0,0`) is reverted and the second one (`3,0,0,1`) succeds.

UPDATE: This might be because the userOp call is executeBatch, and then something in the batch
can be reverted.

```
┌────────────────────────────────────────────────────────────────────┬────────────────────────────────────────────┬────────────────────────────────────────────┬────────────┬───────────────┬─────────────────────────────┬────────────────┬──────────────────┬─────────────────────┬────────┬────────────────────┬────────────────────────────────────────────┬────────────────────────────────────────────┬────────────────────────────────────────────┬────────────┬───────────────────────────────┐
│                          transaction_hash                          │              tx_from_address               │              bundler_address               │ trace_root │ trace_address │ innerhandleop_trace_address │ is_from_sender │ is_innerhandleop │ useropevent_success │ status │       error        │               userop_sender                │                from_address                │                 to_address                 │ userop_idx │ useropevent_actualgascost_eth │
│                              varchar                               │                  varchar                   │                  varchar                   │   int32    │    varchar    │           varchar           │    boolean     │     boolean      │       boolean       │ int64  │      varchar       │                  varchar                   │                  varchar                   │                  varchar                   │   int64    │            double             │
├────────────────────────────────────────────────────────────────────┼────────────────────────────────────────────┼────────────────────────────────────────────┼────────────┼───────────────┼─────────────────────────────┼────────────────┼──────────────────┼─────────────────────┼────────┼────────────────────┼────────────────────────────────────────────┼────────────────────────────────────────────┼────────────────────────────────────────────┼────────────┼───────────────────────────────┤
│ 0xcb761ed55915a26aeb995a05c91a00974afeb8fa4bec142345876ede847a4574 │ 0x65061d355ae0359ec801e047e40c76051833e78c │ 0x65061d355ae0359ec801e047e40c76051833e78c │          3 │ 3             │ 3                           │ false          │ true             │ true                │      1 │                    │ 0x4b32c2c7b94a7ee71eb714f2c39217bf2a25d15b │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │       NULL │             6.56862025611e-07 │
│ 0xcb761ed55915a26aeb995a05c91a00974afeb8fa4bec142345876ede847a4574 │ 0x65061d355ae0359ec801e047e40c76051833e78c │ 0x65061d355ae0359ec801e047e40c76051833e78c │          3 │ 3,0           │ 3                           │ false          │ false            │ true                │      1 │                    │ 0x4b32c2c7b94a7ee71eb714f2c39217bf2a25d15b │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x4b32c2c7b94a7ee71eb714f2c39217bf2a25d15b │       NULL │             6.56862025611e-07 │
│ 0xcb761ed55915a26aeb995a05c91a00974afeb8fa4bec142345876ede847a4574 │ 0x65061d355ae0359ec801e047e40c76051833e78c │ 0x65061d355ae0359ec801e047e40c76051833e78c │          3 │ 3,0,0,0       │ 3                           │ true           │ false            │ true                │      0 │ execution reverted │ 0x4b32c2c7b94a7ee71eb714f2c39217bf2a25d15b │ 0x4b32c2c7b94a7ee71eb714f2c39217bf2a25d15b │ 0x000000e92d78d90000007f0082006fda09bd5f11 │          1 │             6.56862025611e-07 │
│ 0xcb761ed55915a26aeb995a05c91a00974afeb8fa4bec142345876ede847a4574 │ 0x65061d355ae0359ec801e047e40c76051833e78c │ 0x65061d355ae0359ec801e047e40c76051833e78c │          3 │ 3,0,0,1       │ 3                           │ true           │ false            │ true                │      1 │                    │ 0x4b32c2c7b94a7ee71eb714f2c39217bf2a25d15b │ 0x4b32c2c7b94a7ee71eb714f2c39217bf2a25d15b │ 0x000000e92d78d90000007f0082006fda09bd5f11 │          2 │             6.56862025611e-07 │
│ 0xcb761ed55915a26aeb995a05c91a00974afeb8fa4bec142345876ede847a4574 │ 0x65061d355ae0359ec801e047e40c76051833e78c │ 0x65061d355ae0359ec801e047e40c76051833e78c │          4 │ 4             │ 4                           │ false          │ true             │ true                │      1 │                    │ 0xa0bcd2dbe1de220e7a2cf1aca384d821bc123b51 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │       NULL │             6.10793011428e-07 │
│ 0xcb761ed55915a26aeb995a05c91a00974afeb8fa4bec142345876ede847a4574 │ 0x65061d355ae0359ec801e047e40c76051833e78c │ 0x65061d355ae0359ec801e047e40c76051833e78c │          4 │ 4,0           │ 4                           │ false          │ false            │ true                │      1 │                    │ 0xa0bcd2dbe1de220e7a2cf1aca384d821bc123b51 │ 0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789 │ 0xa0bcd2dbe1de220e7a2cf1aca384d821bc123b51 │       NULL │             6.10793011428e-07 │
│ 0xcb761ed55915a26aeb995a05c91a00974afeb8fa4bec142345876ede847a4574 │ 0x65061d355ae0359ec801e047e40c76051833e78c │ 0x65061d355ae0359ec801e047e40c76051833e78c │          4 │ 4,0,0,0       │ 4                           │ true           │ false            │ true                │      1 │                    │ 0xa0bcd2dbe1de220e7a2cf1aca384d821bc123b51 │ 0xa0bcd2dbe1de220e7a2cf1aca384d821bc123b51 │ 0xe55fee191604cdbeb874f87a28ca89aed401c303 │          1 │             6.10793011428e-07 │
└────────────────────────────────────────────────────────────────────┴────────────────────────────────────────────┴────────────────────────────────────────────┴────────────┴───────────────┴─────────────────────────────┴────────────────┴──────────────────┴─────────────────────┴────────┴────────────────────┴────────────────────────────────────────────┴────────────────────────────────────────────┴────────────────────────────────────────────┴────────────┴───────────────────────────────┘
```
