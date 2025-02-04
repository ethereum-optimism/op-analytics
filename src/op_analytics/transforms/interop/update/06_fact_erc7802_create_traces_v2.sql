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
  , trace_address
  , trace_type
  , gas
  , gas_used
FROM
  blockbatch_gcs.read_date(
    rootpath = 'blockbatch/contract_creation/create_traces_v1'
    , chain = '*'
    , dt = { dtparam: Date }
  )
WHERE
  dt = { dtparam: Date }
  AND (
    -- crosschainBurn(address,uint256): 0x2b8c49e3
    -- crosschainMint(address,uint256): 0x18bf5077
    (output LIKE '%2b8c49e3%' AND output LIKE '%18bf5077%')
  )
