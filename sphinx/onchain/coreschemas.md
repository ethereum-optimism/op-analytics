# Core Dataset Schemas

This page has detailed mappings for all the columns included in our core dataset schemas. OP Labs
uses BigQuery types for schemas throughout. It is important for us to understand how raw data
maps to our schemas.


## Blocks
|       Name      |   JSON-RPC method  | JSON-RPC field |Goldsky Type|  Goldsky Field  |        OP Labs BigQuery Type        |          OP Labs Expression         |
|-----------------|--------------------|----------------|------------|-----------------|-------------------------------------|-------------------------------------|
|      _meta      |         --         |       --       |     --     |        --       |STRUCT<ingestion_timestamp TIMESTAMP>|                  --                 |
|      chain      |         --         |       --       |     --     |        --       |                STRING               |                chain                |
|     network     |         --         |       --       |     --     |        --       |                STRING               |               network               |
|     chain_id    |         --         |       --       |     --     |        --       |                INT64                |               chain_id              |
|        dt       |         --         |       --       |     --     |        --       |                STRING               |formatDateTime(timestamp, '%Y-%m-%d')|
|    timestamp    |eth_getBlockByNumber|    timestamp   |    long    |    timestamp    |              TIMESTAMP              |              timestamp              |
|      number     |eth_getBlockByNumber|     number     |    long    |      number     |                INT64                |    accurateCast(number, 'Int64')    |
|       hash      |eth_getBlockByNumber|      hash      |   string   |       hash      |                STRING               |         cast(hash, 'String')        |
|   parent_hash   |eth_getBlockByNumber|   parentHash   |   string   |   parent_hash   |                STRING               |             parent_hash             |
|      nonce      |eth_getBlockByNumber|      nonce     |   string   |      nonce      |                STRING               |                nonce                |
|   sha3_uncles   |eth_getBlockByNumber|   sha3Uncles   |   string   |   sha3_uncles   |                STRING               |             sha3_uncles             |
|    logs_bloom   |eth_getBlockByNumber|    logsBloom   |   string   |    logs_bloom   |                STRING               |              logs_bloom             |
|transactions_root|eth_getBlockByNumber|transactionsRoot|   string   |transactions_root|                STRING               |          transactions_root          |
|    state_root   |eth_getBlockByNumber|    stateRoot   |   string   |    state_root   |                STRING               |              state_root             |
|  receipts_root  |eth_getBlockByNumber|  receiptsRoot  |   string   |  receipts_root  |                STRING               |            receipts_root            |
| withdrawals_root|eth_getBlockByNumber| withdrawalsRoot|   string   | withdrawals_root|                STRING               |           withdrawals_root          |
|      miner      |eth_getBlockByNumber|      miner     |   string   |      miner      |                STRING               |                miner                |
|    difficulty   |eth_getBlockByNumber|   difficulty   |   double   |    difficulty   |               FLOAT64               |     cast(difficulty, 'Float64')     |
| total_difficulty|eth_getBlockByNumber| totalDifficulty|   double   | total_difficulty|               FLOAT64               |  cast(total_difficulty, 'Float64')  |
|       size      |eth_getBlockByNumber|      size      |    long    |       size      |                INT64                |                 size                |
| base_fee_per_gas|eth_getBlockByNumber|  baseFeePerGas |    long    | base_fee_per_gas|                INT64                |           base_fee_per_gas          |
|     gas_used    |eth_getBlockByNumber|     gasUsed    |    long    |     gas_used    |                INT64                |               gas_used              |
|    gas_limit    |eth_getBlockByNumber|    gasLimit    |    long    |    gas_limit    |                INT64                |              gas_limit              |
|    extra_data   |eth_getBlockByNumber|    extraData   |   string   |    extra_data   |                STRING               |              extra_data             |
|transaction_count|         --         |       --       |    long    |transaction_count|                INT64                |          transaction_count          |

## Transactions
|              Name             |     JSON-RPC method     |   JSON-RPC field   |Goldsky Type|         Goldsky Field         |        OP Labs BigQuery Type        |                       OP Labs Expression                       |
|-------------------------------|-------------------------|--------------------|------------|-------------------------------|-------------------------------------|----------------------------------------------------------------|
|             _meta             |            --           |         --         |     --     |               --              |STRUCT<ingestion_timestamp TIMESTAMP>|                               --                               |
|             chain             |            --           |         --         |     --     |               --              |                STRING               |                              chain                             |
|            network            |            --           |         --         |     --     |               --              |                STRING               |                             network                            |
|            chain_id           |            --           |         --         |     --     |               --              |                INT64                |                            chain_id                            |
|               dt              |            --           |         --         |     --     |               --              |                STRING               |           formatDateTime(block_timestamp, '%Y-%m-%d')          |
|        block_timestamp        |   eth_getBlockByNumber  |      timestamp     |    long    |        block_timestamp        |              TIMESTAMP              |                         block_timestamp                        |
|          block_number         |   eth_getBlockByNumber  |       number       |    long    |          block_number         |                INT64                |               accurateCast(block_number, 'Int64')              |
|           block_hash          |   eth_getBlockByNumber  |     block_hash     |   string   |           block_hash          |                STRING               |                   cast(block_hash, 'String')                   |
|              hash             | eth_getTransactionByHash|        hash        |   string   |              hash             |                STRING               |                      cast(hash, 'String')                      |
|             nonce             | eth_getTransactionByHash|        nonce       |    long    |             nonce             |                INT64                |                  accurateCast(nonce, 'Int64')                  |
|       transaction_index       | eth_getTransactionByHash|        hash        |    long    |       transaction_index       |                INT64                |            accurateCast(transaction_index, 'Int64')            |
|          from_address         | eth_getTransactionByHash|        from        |   string   |          from_address         |                STRING               |                  cast(from_address, 'String')                  |
|           to_address          | eth_getTransactionByHash|         to         |   string   |           to_address          |                STRING               |                   cast(to_address, 'String')                   |
|            value_64           | eth_getTransactionByHash|        value       |   decimal  |             value             |                INT64                |               accurateCastOrNull(value, 'Int64')               |
|         value_lossless        | eth_getTransactionByHash|        value       |   decimal  |             value             |                STRING               |                      cast(value, 'String')                     |
|              gas              | eth_getTransactionByHash|         gas        |   decimal  |              gas              |                INT64                |                   accurateCast(gas, 'Int64')                   |
|           gas_price           | eth_getTransactionByHash|      gasPrice      |   decimal  |           gas_price           |                INT64                |                accurateCast(gas_price, 'Int64')                |
|             input             | eth_getTransactionByHash|        input       |   string   |             input             |                STRING               |                              input                             |
|        transaction_type       | eth_getTransactionByHash|        type        |    long    |        transaction_type       |                INT64                |             accurateCast(transaction_type, 'Int32')            |
|        max_fee_per_gas        | eth_getTransactionByHash|    maxFeePerGas    |   decimal  |        max_fee_per_gas        |                INT64                |             accurateCast(max_fee_per_gas, 'Int64')             |
|    max_priority_fee_per_gas   | eth_getTransactionByHash|maxPriorityFeePerGas|   decimal  |    max_priority_fee_per_gas   |                INT64                |         accurateCast(max_priority_fee_per_gas, 'Int64')        |
|     blob_versioned_hashes     | eth_getTransactionByHash| blobVersionedHashes|     --     |               --              |            ARRAY<STRING>            |                               --                               |
|      max_fee_per_blob_gas     | eth_getTransactionByHash|  maxFeePerBlobGas  |     --     |               --              |                INT64                |                               --                               |
|  receipt_cumulative_gas_used  |eth_getTransactionReceipt|  cumulativeGasUsed |   decimal  |  receipt_cumulative_gas_used  |                INT64                |       accurateCast(receipt_cumulative_gas_used, 'Int64')       |
|        receipt_gas_used       |eth_getTransactionReceipt|       gasUsed      |   decimal  |        receipt_gas_used       |                INT64                |             accurateCast(receipt_gas_used, 'Int64')            |
|    receipt_contract_address   |eth_getTransactionReceipt|   contractAddress  |   string   |    receipt_contract_address   |                INT64                |                    receipt_contract_address                    |
|         receipt_status        |eth_getTransactionReceipt|       status       |    long    |         receipt_status        |                INT64                |              accurateCast(receipt_status, 'Int32')             |
|  receipt_effective_gas_price  |eth_getTransactionReceipt|  effectiveGasPrice |   decimal  |  receipt_effective_gas_price  |                INT64                |       accurateCast(receipt_effective_gas_price, 'Int64')       |
|       receipt_root_hash       |eth_getTransactionReceipt|        root        |     --     |               --              |                INT64                |                               --                               |
|      receipt_l1_gas_price     |eth_getTransactionReceipt|     l1GasPrice     |   decimal  |      receipt_l1_gas_price     |                INT64                |      accurateCast(receipt_l1_gas_price, 'Nullable(Int64)')     |
|      receipt_l1_gas_used      |eth_getTransactionReceipt|      l1GasUsed     |   decimal  |      receipt_l1_gas_used      |                INT64                |      accurateCast(receipt_l1_gas_used, 'Nullable(Int64)')      |
|         receipt_l1_fee        |eth_getTransactionReceipt|        l1Fee       |   decimal  |         receipt_l1_fee        |                INT64                | accurateCast(receipt_l1_fee, 'Nullable(Int64)') receipt_l1_fee |
|     receipt_l1_fee_scalar     |eth_getTransactionReceipt|     l1FeeScalar    |   decimal  |     receipt_l1_fee_scalar     |               FLOAT64               |                      receipt_l1_fee_scalar                     |
|    receipt_l1_blob_base_fee   |eth_getTransactionReceipt|    l1BlobBaseFee   |   decimal  |    receipt_l1_blob_base_fee   |                INT64                |    accurateCast(receipt_l1_blob_base_fee, 'Nullable(Int64)')   |
|receipt_l1_blob_base_fee_scalar|eth_getTransactionReceipt| l1BlobBaseFeeScalar|   decimal  |receipt_l1_blob_base_fee_scalar|                INT64                |accurateCast(receipt_l1_blob_base_fee_scalar, 'Nullable(Int64)')|
|   receipt_l1_base_fee_scalar  |eth_getTransactionReceipt|   l1BaseFeeScalar  |   decimal  |   receipt_l1_base_fee_scalar  |                INT64                |   accurateCast(receipt_l1_base_fee_scalar, 'Nullable(Int64)')  |

## Logs
|       Name      |   JSON-RPC method  |  JSON-RPC field |Goldsky Type|  Goldsky Field  |        OP Labs BigQuery Type        |                   OP Labs Expression                  |
|-----------------|--------------------|-----------------|------------|-----------------|-------------------------------------|-------------------------------------------------------|
|      _meta      |         --         |        --       |     --     |        --       |STRUCT<ingestion_timestamp TIMESTAMP>|                           --                          |
|      chain      |         --         |        --       |     --     |        --       |                STRING               |                         chain                         |
|     network     |         --         |        --       |     --     |        --       |                STRING               |                        network                        |
|     chain_id    |         --         |        --       |     --     |        --       |                INT64                |                        chain_id                       |
|        dt       |         --         |        --       |     --     |        --       |                STRING               |      formatDateTime(block_timestamp, '%Y-%m-%d')      |
| block_timestamp |eth_getBlockByNumber|    timestamp    |    long    | block_timestamp |              TIMESTAMP              |                    block_timestamp                    |
|   block_number  |     eth_getLogs    |      number     |    long    |   block_number  |                INT64                |          accurateCast(block_number, 'Int64')          |
|    block_hash   |     eth_getLogs    |    block_hash   |   string   |    block_hash   |                STRING               |               cast(block_hash, 'String')              |
| transaction_hash|     eth_getLogs    | transaction_hash|   string   | transaction_hash|                STRING               |            cast(transaction_hash, 'String')           |
|transaction_index|     eth_getLogs    |transaction_index|    long    |transaction_index|                INT64                |        accurateCast(transaction_index, 'Int64')       |
|    log_index    |     eth_getLogs    |       hash      |    long    |transaction_index|                INT64                |            accurateCast(log_index, 'Int64')           |
|     address     |     eth_getLogs    |     address     |   string   |     address     |                STRING               |                cast(address, 'String')                |
|      topics     |     eth_getLogs    |      topics     |   string   |      topics     |                STRING               |                 cast(topics, 'String')                |
|       data      |     eth_getLogs    |       data      |   string   |       data      |                STRING               |                  cast(data, 'String')                 |
|      topic0     |         --         |        --       |     --     |        --       |                STRING               |         splitByChar(',', topics)[1] as topic0         |
|   indexed_args  |         --         |        --       |     --     |        --       |            ARRAY<STRING>            |arraySlice(splitByChar(',', topics), 2) as indexed_args|

## Traces
|       Name      |JSON-RPC method|JSON-RPC field|Goldsky Type|  Goldsky Field  |        OP Labs BigQuery Type        |             OP Labs Expression            |
|-----------------|---------------|--------------|------------|-----------------|-------------------------------------|-------------------------------------------|
|      _meta      |       --      |      --      |     --     |        --       |STRUCT<ingestion_timestamp TIMESTAMP>|                     --                    |
|      chain      |       --      |      --      |     --     |        --       |                STRING               |                   chain                   |
|     network     |       --      |      --      |     --     |        --       |                STRING               |                  network                  |
|     chain_id    |       --      |      --      |     --     |        --       |                INT64                |                  chain_id                 |
|        dt       |       --      |      --      |     --     |        --       |                STRING               |formatDateTime(block_timestamp, '%Y-%m-%d')|
| block_timestamp |       --      |      --      |    long    | block_timestamp |              TIMESTAMP              |              block_timestamp              |
|   block_number  |       --      |      --      |    long    |   block_number  |                INT64                |    accurateCast(block_number, 'Int64')    |
|    block_hash   |       --      |      --      |   string   |    block_hash   |                STRING               |         cast(block_hash, 'String')        |
| transaction_hash|       --      |      --      |   string   | transaction_hash|                STRING               |      cast(transaction_hash, 'String')     |
|transaction_index|       --      |      --      |    long    |transaction_index|                INT64                |  accurateCast(transaction_index, 'Int64') |
|   from_address  |       --      |      --      |   string   |   from_address  |                STRING               |        cast(from_address, 'String')       |
|    to_address   |       --      |      --      |   string   |    to_address   |                STRING               |         cast(to_address, 'String')        |
|     value_64    |       --      |      --      |   decimal  |      value      |                INT64                |     accurateCastOrNull(value, 'Int64')    |
|  value_lossless |       --      |      --      |   decimal  |      value      |                STRING               |           cast(value, 'String')           |
|      input      |       --      |      --      |   string   |      input      |                STRING               |                   input                   |
|      output     |       --      |      --      |   string   |      output     |                STRING               |                   output                  |
|    trace_type   |       --      |      --      |   string   |    trace_type   |                STRING               |         cast(trace_type, 'String')        |
|    call_type    |       --      |      --      |   string   |    call_type    |                STRING               |         cast(call_type, 'String')         |
|   reward_type   |       --      |      --      |   string   |   reward_type   |                STRING               |        cast(reward_type, 'String')        |
|       gas       |       --      |      --      |    long    |       gas       |                INT64                |         accurateCast(gas, 'Int64')        |
|     gas_used    |       --      |      --      |    long    |     gas_used    |                INT64                |      accurateCast(gas_used, 'Int64')      |
|    subtraces    |       --      |      --      |    long    |    subtraces    |                INT64                |      accurateCast(subtraces, 'Int64')     |
|  trace_address  |       --      |      --      |   string   |  trace_address  |                STRING               |       cast(trace_address, 'String')       |
|      error      |       --      |      --      |   string   |      error      |                STRING               |           cast(error, 'String')           |
|      status     |       --      |      --      |    long    |      status     |                INT64                |       accurateCast(status, 'Int64')       |