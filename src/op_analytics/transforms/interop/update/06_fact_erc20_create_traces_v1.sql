/**

Contract creation traces for contracts that implement crosschainBurn/crosschainMint.

*/
INSERT INTO _placeholder_


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
  , output
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

  -- Contract bytecode has the ERC-20 Transfer and Approval events.
  AND position(output, 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')
  AND position(output, '8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925')
