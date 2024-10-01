# Schemas

Here are the schemas!


## Blocks
|       Name       |   JSON-RPC method  | JSON-RPC field |Goldsky Type|  Goldsky Field  |        OP Labs Compatible Expr        |
|------------------|--------------------|----------------|------------|-----------------|---------------------------------------|
|ingestion_metadata|         --         |       --       |     --     |        --       |                   --                  |
|       chain      |         --         |       --       |     --     |        --       |                   --                  |
|      network     |         --         |       --       |     --     |        --       |                   --                  |
|     chain_id     |         --         |       --       |     --     |        --       |                   --                  |
|     timestamp    |eth_getBlockByNumber|    timestamp   |    long    |    timestamp    |               timestamp               |
|      number      |eth_getBlockByNumber|     number     |    long    |      number     |accurateCast(number, 'Int64') AS number|
|       hash       |eth_getBlockByNumber|      hash      |   string   |       hash      |                  hash                 |
|    parent_hash   |eth_getBlockByNumber|   parentHash   |   string   |   parent_hash   |              parent_hash              |
|       nonce      |eth_getBlockByNumber|      nonce     |   string   |      nonce      |                 nonce                 |
|    sha3_uncles   |eth_getBlockByNumber|   sha3Uncles   |   string   |   sha3_uncles   |              sha3_uncles              |
|    logs_bloom    |eth_getBlockByNumber|    logsBloom   |   string   |    logs_bloom   |               logs_bloom              |
| transactions_root|eth_getBlockByNumber|transactionsRoot|   string   |transactions_root|           transactions_root           |
|    state_root    |eth_getBlockByNumber|    stateRoot   |   string   |    state_root   |               state_root              |
|   receipts_root  |eth_getBlockByNumber|  receiptsRoot  |   string   |  receipts_root  |             receipts_root             |
| withdrawals_root |eth_getBlockByNumber| withdrawalsRoot|   string   | withdrawals_root|            withdrawals_root           |
|       miner      |eth_getBlockByNumber|      miner     |   string   |      miner      |                 miner                 |
|    difficulty    |eth_getBlockByNumber|   difficulty   |   double   |    difficulty   |               difficulty              |
| total_difficulty |eth_getBlockByNumber| totalDifficulty|   double   | total_difficulty|            total_difficulty           |
|       size       |eth_getBlockByNumber|      size      |    long    |       size      |                  size                 |
| base_fee_per_gas |eth_getBlockByNumber|  baseFeePerGas |    long    | base_fee_per_gas|            base_fee_per_gas           |
|     gas_used     |eth_getBlockByNumber|     gasUsed    |    long    |     gas_used    |                gas_used               |
|     gas_limit    |eth_getBlockByNumber|    gasLimit    |    long    |    gas_limit    |               gas_limit               |
|    extra_data    |eth_getBlockByNumber|    extraData   |   string   |    extra_data   |               extra_data              |
| transaction_count|         --         |       --       |    long    |transaction_count|           transaction_count           |

## Transactions
|              Name             |     JSON-RPC method     |   JSON-RPC field   |Goldsky Type|         Goldsky Field         |                                      OP Labs Compatible Expr                                      |
|-------------------------------|-------------------------|--------------------|------------|-------------------------------|---------------------------------------------------------------------------------------------------|
|       ingestion_metadata      |            --           |         --         |     --     |               --              |                                                 --                                                |
|             chain             |            --           |         --         |     --     |               --              |                                                 --                                                |
|            network            |            --           |         --         |     --     |               --              |                                                 --                                                |
|            chain_id           |            --           |         --         |     --     |               --              |                                                 --                                                |
|        block_timestamp        |   eth_getBlockByNumber  |      timestamp     |    long    |        block_timestamp        |                                          block_timestamp                                          |
|          block_number         |   eth_getBlockByNumber  |       number       |    long    |          block_number         |                        accurateCast(block_number, 'Int64') AS block_number                        |
|           block_hash          |   eth_getBlockByNumber  |     block_hash     |   string   |           block_hash          |                                             block_hash                                            |
|              hash             | eth_getTransactionByHash|        hash        |   string   |              hash             |                                                hash                                               |
|             nonce             | eth_getTransactionByHash|        nonce       |    long    |             nonce             |                               accurateCast(nonce, 'Int64') AS nonce                               |
|       transaction_index       | eth_getTransactionByHash|        hash        |    long    |       transaction_index       |                   accurateCast(transaction_index, 'Int64') AS transaction_index                   |
|          from_address         | eth_getTransactionByHash|        from        |   string   |          from_address         |                                            from_address                                           |
|           to_address          | eth_getTransactionByHash|         to         |   string   |           to_address          |                                             to_address                                            |
|             value             | eth_getTransactionByHash|        value       |   decimal  |             value             |                            accurateCastOrNull(value, 'Int64') AS value                            |
|         value_lossless        | eth_getTransactionByHash|        value       |   decimal  |             value             |                              cast(value, 'String') AS value_lossless                              |
|              gas              | eth_getTransactionByHash|         gas        |   decimal  |              gas              |                                 accurateCast(gas, 'Int64') AS gas                                 |
|           gas_price           | eth_getTransactionByHash|      gasPrice      |   decimal  |           gas_price           |                           accurateCast(gas_price, 'Int64') AS gas_price                           |
|             input             | eth_getTransactionByHash|        input       |   string   |             input             |                                               input                                               |
|        transaction_type       | eth_getTransactionByHash|        type        |    long    |        transaction_type       |                    accurateCast(transaction_type, 'Int32') AS transaction_type                    |
|        max_fee_per_gas        | eth_getTransactionByHash|    maxFeePerGas    |   decimal  |        max_fee_per_gas        |                     accurateCast(max_fee_per_gas, 'Int64') AS max_fee_per_gas                     |
|    max_priority_fee_per_gas   | eth_getTransactionByHash|maxPriorityFeePerGas|   decimal  |    max_priority_fee_per_gas   |            accurateCast(max_priority_fee_per_gas, 'Int64') AS max_priority_fee_per_gas            |
|     blob_versioned_hashes     | eth_getTransactionByHash| blobVersionedHashes|     --     |               --              |                                                 --                                                |
|      max_fee_per_blob_gas     | eth_getTransactionByHash|  maxFeePerBlobGas  |     --     |               --              |                                                 --                                                |
|  receipt_cumulative_gas_used  |eth_getTransactionReceipt|  cumulativeGasUsed |   decimal  |  receipt_cumulative_gas_used  |         accurateCast(receipt_cumulative_gas_used, 'Int64') AS receipt_cumulative_gas_used         |
|        receipt_gas_used       |eth_getTransactionReceipt|       gasUsed      |   decimal  |        receipt_gas_used       |                    accurateCast(receipt_gas_used, 'Int64') AS receipt_gas_used                    |
|    receipt_contract_address   |eth_getTransactionReceipt|   contractAddress  |   string   |    receipt_contract_address   |                                      receipt_contract_address                                     |
|         receipt_status        |eth_getTransactionReceipt|       status       |    long    |         receipt_status        |                      accurateCast(receipt_status, 'Int32') AS receipt_status                      |
|  receipt_effective_gas_price  |eth_getTransactionReceipt|  effectiveGasPrice |   decimal  |  receipt_effective_gas_price  |         accurateCast(receipt_effective_gas_price, 'Int64') AS receipt_effective_gas_price         |
|       receipt_root_hash       |eth_getTransactionReceipt|        root        |     --     |               --              |                                                 --                                                |
|      receipt_l1_gas_price     |eth_getTransactionReceipt|     l1GasPrice     |   decimal  |      receipt_l1_gas_price     |           accurateCast(receipt_l1_gas_price, 'Nullable(Int64)') AS receipt_l1_gas_price           |
|      receipt_l1_gas_used      |eth_getTransactionReceipt|      l1GasUsed     |   decimal  |      receipt_l1_gas_used      |            accurateCast(receipt_l1_gas_used, 'Nullable(Int64)') AS receipt_l1_gas_used            |
|         receipt_l1_fee        |eth_getTransactionReceipt|        l1Fee       |   decimal  |         receipt_l1_fee        |                   accurateCast(receipt_l1_fee, 'Nullable(Int64)') receipt_l1_fee                  |
|     receipt_l1_fee_scalar     |eth_getTransactionReceipt|     l1FeeScalar    |   decimal  |     receipt_l1_fee_scalar     |                                       receipt_l1_fee_scalar                                       |
|    receipt_l1_blob_base_fee   |eth_getTransactionReceipt|    l1BlobBaseFee   |   decimal  |    receipt_l1_blob_base_fee   |       accurateCast(receipt_l1_blob_base_fee, 'Nullable(Int64)') AS receipt_l1_blob_base_fee       |
|receipt_l1_blob_base_fee_scalar|eth_getTransactionReceipt| l1BlobBaseFeeScalar|   decimal  |receipt_l1_blob_base_fee_scalar|accurateCast(receipt_l1_blob_base_fee_scalar, 'Nullable(Int64)') AS receipt_l1_blob_base_fee_scalar|
|   receipt_l1_base_fee_scalar  |eth_getTransactionReceipt|   l1BaseFeeScalar  |   decimal  |   receipt_l1_base_fee_scalar  |     accurateCast(receipt_l1_base_fee_scalar, 'Nullable(Int64)') AS receipt_l1_base_fee_scalar     |