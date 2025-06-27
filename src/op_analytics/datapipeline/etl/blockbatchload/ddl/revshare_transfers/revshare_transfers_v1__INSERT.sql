-- Native transfers
WITH native_transfers AS (
  SELECT DISTINCT
    t.chain
    , t.dt
    , t.network
    , t.chain_id
    , t.block_timestamp
    , t.block_number
    , t.block_hash
    , t.transaction_hash
    , t.transaction_index
    , t.trace_address
    , lower(t.from_address) AS from_address
    , lower(t.to_address) AS to_address
    , CAST(t.amount_lossless AS UINT256) AS amount
    , t.amount_lossless
    , t.transfer_type
    , NULL AS token_address
    , f.chain AS revshare_from_chain
    , f.chain_id AS revshare_from_chain_id
    , [f.address] AS revshare_from_addresses
    , f.tokens AS token_addresses
    , f.expected_chains AS from_expected_chains
    , ta.expected_chains AS to_expected_chains
    , ta.description AS to_address_description
  FROM INPUT_BLOCKBATCH('blockbatch/native_transfers/native_transfers_v1') AS t
  INNER JOIN revshare_from_addresses AS f
    ON lower(f.address) = lower(t.from_address)
      AND t.chain = f.expected_chains[1]
      AND (f.end_date IS NULL OR t.dt <= toDate(f.end_date))
  INNER JOIN revshare_to_addresses AS ta
    ON lower(ta.address) = lower(t.to_address)
      AND t.chain = ta.expected_chains[1]
      AND (ta.end_date IS NULL OR t.dt <= toDate(ta.end_date))
  WHERE t.amount_lossless IS NOT NULL
),

-- ERC20 transfers
erc20_transfers AS (
  SELECT DISTINCT
    t.chain
    , t.dt
    , t.network
    , t.chain_id
    , t.block_timestamp
    , t.block_number
    , t.block_hash
    , t.transaction_hash
    , t.transaction_index
    , t.trace_address
    , lower(t.from_address) AS from_address
    , lower(t.to_address) AS to_address
    , CAST(t.amount_lossless AS UINT256) AS amount
    , t.amount_lossless
    , 'token' AS transfer_type
    , lower(t.contract_address) AS token_address
    , f.chain AS revshare_from_chain
    , f.chain_id AS revshare_from_chain_id
    , [f.address] AS revshare_from_addresses
    , f.tokens AS token_addresses
    , f.expected_chains AS from_expected_chains
    , ta.expected_chains AS to_expected_chains
    , ta.description AS to_address_description
  FROM INPUT_BLOCKBATCH('blockbatch/token_transfers/erc20_transfers_v1') AS t
  INNER JOIN revshare_from_addresses AS f
    ON lower(f.address) = lower(t.from_address)
      AND t.chain = f.expected_chains[1]
      AND hasAny(f.tokens, [lower(t.contract_address)])
      AND (f.end_date IS NULL OR t.dt <= toDate(f.end_date))
  INNER JOIN revshare_to_addresses AS ta
    ON lower(ta.address) = lower(t.to_address)
      AND t.chain = ta.expected_chains[1]
      AND (ta.end_date IS NULL OR t.dt <= toDate(ta.end_date))
  WHERE t.amount_lossless IS NOT NULL
)

-- Combine native and ERC20 transfers
SELECT
  *
FROM native_transfers

UNION ALL

SELECT
  *
FROM erc20_transfers 
