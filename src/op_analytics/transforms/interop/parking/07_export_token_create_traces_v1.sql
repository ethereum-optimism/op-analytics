/**

Contract creation traces for the token types we are interested in:

- SuperchainERC20
- NTT
- OFT

*/
INSERT INTO _placeholder_

-- erc7802_contracts
(
SELECT
  dt
  , chain
  , chain_id
  , network
  , 'SuperchainERC20' AS token_type
  , block_timestamp
  , block_number
  , transaction_hash
  , transaction_index
  , tr_from_address
  , tx_from_address
  , contract_address
  , trace_address
  , trace_type
  , gas
  , gas_used
FROM
  transforms_interop.fact_erc7802_create_traces_v1
WHERE
  dt >= '2024-10-01'
  AND (chain_id, contract_address)
  IN (SELECT
    chain_id
    , contract_address
  FROM transforms_interop.dim_ntt_contract_address_v1)
)

UNION ALL

-- ntt_contracts
(
SELECT
  dt
  , chain
  , chain_id
  , network
  , 'NTT' AS token_type
  , block_timestamp
  , block_number
  , transaction_hash
  , transaction_index
  , tr_from_address
  , tx_from_address
  , contract_address
  , trace_address
  , trace_type
  , gas
  , gas_used
FROM
  blockbatch.contract_creation__create_traces_v1
WHERE
  dt >= '2024-10-01'
  AND (chain_id, contract_address)
  IN (SELECT
    chain_id
    , contract_address
  FROM transforms_interop.dim_ntt_contract_address_v1)
)

-- oft_contracts
(
SELECT
  dt
  , chain
  , chain_id
  , network
  , 'OFT' AS token_type
  , block_timestamp
  , block_number
  , transaction_hash
  , transaction_index
  , tr_from_address
  , tx_from_address
  , contract_address
  , trace_address
  , trace_type
  , gas
  , gas_used
FROM
  blockbatch.contract_creation__create_traces_v1
WHERE
  dt >= '2024-10-01'
  AND (chain_id, contract_address)
  IN (SELECT
    chain_id
    , contract_address
  FROM transforms_interop.dim_oft_contract_address_v1)
)

