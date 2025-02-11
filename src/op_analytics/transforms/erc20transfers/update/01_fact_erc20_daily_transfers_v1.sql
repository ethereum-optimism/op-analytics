SELECT
  chain
  , chain_id
  , dt
  , network
  , contract_address
  , COUNT(*) AS num_transfers
  , COUNT(DISTINCT transaction_hash) AS num_transactions
  , COUNT(DISTINCT block_number) AS num_blocks
  , COUNT(DISTINCT from_address) AS num_from_addresses
  , COUNT(DISTINCT to_address) AS num_to_addresses
  , SUM(amount) AS amount_raw
FROM blockbatch.token_transfers__erc20_transfers_v1
WHERE
  dt = {dtparam:Date}
GROUP BY 1, 2, 3, 4, 5
