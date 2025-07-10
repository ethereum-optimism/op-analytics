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
    , t.amount
    , t.transfer_type
    , NULL AS token_address
    , f.chain AS revshare_from_chain
    , f.chain_id AS revshare_from_chain_id
    , f.address IS NOT NULL AS is_revshare_transfer

  FROM INPUT_BLOCKBATCH('blockbatch/native_transfers/native_transfers_v1') AS t
  INNER JOIN datasources_revshareconfig.revshare_to_addresses AS ta
    ON
      lower(ta.address) = lower(t.to_address)
      AND (has(ta.expected_chains, t.chain) OR has(ta.expected_chains, 'all'))
      AND (ta.end_date IS NULL OR t.dt <= toDate(ta.end_date))
  LEFT JOIN datasources_revshareconfig.revshare_from_addresses AS f
    ON
      lower(f.address) = lower(t.from_address)
      AND has(f.expected_chains, t.chain)
      AND (f.end_date IS NULL OR t.dt <= toDate(f.end_date))

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
    , NULL AS trace_address
    , lower(t.from_address) AS from_address
    , lower(t.to_address) AS to_address
    , t.amount
    , 'token' AS transfer_type
    , lower(t.contract_address) AS token_address
    , f.chain AS revshare_from_chain
    , f.chain_id AS revshare_from_chain_id
    , f.address IS NOT NULL AS is_revshare_transfer

  FROM INPUT_BLOCKBATCH('blockbatch/token_transfers/erc20_transfers_v1') AS t
  INNER JOIN datasources_revshareconfig.revshare_to_addresses AS ta
    ON
      lower(ta.address) = lower(t.to_address)
      AND (has(ta.expected_chains, t.chain) OR has(ta.expected_chains, 'all'))
      AND (ta.end_date IS NULL OR t.dt <= toDate(ta.end_date))
  LEFT JOIN datasources_revshareconfig.revshare_from_addresses AS f
    ON
      lower(f.address) = lower(t.from_address)
      AND has(f.expected_chains, t.chain)
      AND hasAny(f.tokens, [lower(t.contract_address)])
      AND (f.end_date IS NULL OR t.dt <= toDate(f.end_date))

)

-- Combine native and ERC20 transfers
SELECT
  *
FROM native_transfers

UNION ALL

SELECT
  *
FROM erc20_transfers 
