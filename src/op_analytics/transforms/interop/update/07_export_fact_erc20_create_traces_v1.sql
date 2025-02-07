/**

ERC-20 contract creation traces broken down by the type of contract created.

*/
INSERT INTO _placeholder_

WITH

erc20_tokens AS (
  SELECT
    chain_id
    , contract_address
  FROM transforms_interop.dim_erc20_first_seen_v1 FINAL
)

-- OFT Token Contracts
, oft_tokens AS (
  SELECT
    chain_id
    , contract_address
  FROM transforms_interop.dim_erc20_oft_first_seen_v1 FINAL
)

-- NTT Token Contracts
, ntt_tokens AS (
  SELECT
    chain_id
    , contract_address
  FROM transforms_interop.dim_erc20_ntt_first_seen_v1 FINAL
)

-- All create traces associated with ERC-20 Transfer events.
-- With boolean flags for filtering token types.
SELECT
  dt
  , chain
  , chain_id
  , network
  , block_timestamp
  , block_number
  , transaction_hash
  , transaction_index
  , tr_from_address
  , tx_from_address
  , contract_address
  , tx_to_address
  , trace_address
  , trace_type
  , toString(gas) AS gas
  , toString(gas_used) AS gas_used
  , value
  , code
  , output
  , call_type
  , reward_type
  , subtraces
  , error
  , status
  , tx_method_id
  , code_bytelength
  -- True if the contract has the crosschainBurn/crosschainMint methods.
  -- Computed as part of the fact_erc20_create_traces_v1 table.
  , is_erc7802

  -- Associated with observed ERC-20 Transfers.
  , (chain_id, contract_address) IN (erc20_tokens) AS has_transfers

  -- Associated with observed OFT transactions.
  , (chain_id, contract_address) IN (oft_tokens) AS has_oft_events

  -- Associated with observed NTT transactions.
  , (chain_id, contract_address) IN (ntt_tokens) AS has_ntt_events

FROM
  transforms_interop.fact_erc20_create_traces_v1 FINAL
WHERE
  dt >= '2024-10-01'
