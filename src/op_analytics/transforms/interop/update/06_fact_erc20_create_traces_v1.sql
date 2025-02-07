/**

Contract creation traces for contracts that implement crosschainBurn/crosschainMint.

*/
INSERT INTO _placeholder_

WITH erc20_tokens AS (
  SELECT
    chain_id
    , contract_address
  FROM transforms_interop.dim_erc20_first_seen_v1 FINAL
)

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
  , gas
  , gas_used
  , value
  , code
  , call_type
  , reward_type
  , subtraces
  , error
  , status
  , tx_method_id
  , code_bytelength
  -- ERC-7802 methods:
  -- crosschainBurn(address,uint256): 0x2b8c49e3
  -- crosschainMint(address,uint256): 0x18bf5077
  , (position(output, '2b8c49e3') AND position(output, '18bf5077')) > 0 AS is_erc7802
FROM
  blockbatch.contract_creation__create_traces_v1
WHERE
  dt = { dtparam: Date }

  -- Contract address has emitted ERC-20 Transfer events.
  AND (chain_id, contract_address) IN (erc20_tokens)
