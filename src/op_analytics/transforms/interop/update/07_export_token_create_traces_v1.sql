/**

ERC-20 contract creation traces.

*/
INSERT INTO _placeholder_

WITH

erc20_tokens AS (
  SELECT
    chain_id
    , contract_address
  FROM transforms_interop.dim_erc20_first_seen_v1
)

-- OFT Token Contracts
, oft_tokens AS (
  SELECT
    chain_id
    , contract_address
  FROM transforms_interop.dim_oft_first_seen_v1
)

-- NTT Token Contracts
, ntt_tokens AS (
  SELECT
    chain_id
    , contract_address
  FROM transforms_interop.dim_ntt_first_seen_v1
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
  , trace_address
  , trace_type
  , gas
  , gas_used

  -- ERC-7802 methods:
  -- crosschainBurn(address,uint256): 0x2b8c49e3
  -- crosschainMint(address,uint256): 0x18bf5077
  , (position(output, '2b8c49e3') AND position(output, '18bf5077')) > 0 AS is_erc7802

  , (chain_id, contract_address) IN (erc20_tokens) AS has_erc20

  -- Associated with observed OFT transactions.
  , (chain_id, contract_address) IN (oft_tokens) AS has_oft

  -- Associated with observed NTT transactions.
  , (chain_id, contract_address) IN (ntt_tokens) AS has_ntt

FROM
  blockbatch.contract_creation__create_traces_v1
WHERE
  dt >= '2024-10-01'

  -- Contract bytecode has the ERC-20 Transfer hash.
  AND position(output, 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')
