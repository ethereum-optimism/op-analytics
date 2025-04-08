/**

Contract creation traces for contracts that implement crosschainBurn/crosschainMint.

*/
INSERT INTO _placeholder_

WITH erc20_tokens AS (
  SELECT
    chain_id
    , contract_address
    , first_seen
  FROM transforms_interop.dim_erc20_first_seen_v1 FINAL
)

SELECT
  traces.dt
  , traces.chain
  , traces.chain_id
  , traces.network
  , traces.block_timestamp
  , traces.block_number
  , traces.transaction_hash
  , traces.transaction_index
  , traces.tr_from_address
  , traces.tx_from_address
  , traces.contract_address
  , traces.tx_to_address
  , traces.trace_address
  , traces.trace_type
  , traces.gas
  , traces.gas_used
  , traces.value
  , traces.code
  , traces.call_type
  , traces.reward_type
  , traces.subtraces
  , traces.error
  , traces.status
  , traces.tx_method_id
  , traces.code_bytelength
  -- ERC-7802 methods:
  -- crosschainBurn(address,uint256): 0x2b8c49e3
  -- crosschainMint(address,uint256): 0x18bf5077
  , (position(traces.output, '2b8c49e3') AND position(traces.output, '18bf5077')) > 0 AS is_erc7802
  , transfers.first_seen
FROM
  blockbatch.contract_creation__create_traces_v1 AS traces

-- Inner join to select only contract addresses that have
-- emitted ERC-20 Transfer events.
INNER JOIN erc20_tokens AS transfers
  ON
    traces.chain_id = transfers.chain_id
    AND traces.contract_address = transfers.contract_address
WHERE
  traces.dt = { dtparam: Date }

