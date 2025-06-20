WITH from_addresses AS (
  SELECT
    'base' AS revshare_from_chain,
    ['0x09C7bAD99688a55a2e83644BFAed09e62bDcCcBA', '0x45969D00739d518f0Dde41920B67cE30395135A0'] AS revshare_from_addresses,
    ['native'] AS token_addresses,
    ['base'] AS expected_chains
  UNION ALL
  SELECT
    'zora' AS revshare_from_chain,
    ['0xe900b3Edc1BA0430CFa9a204A1027B90825ac951'] AS revshare_from_addresses,
    ['native'] AS token_addresses,
    ['ethereum'] AS expected_chains
  UNION ALL
  SELECT
    'op' AS revshare_from_chain,
    ['0x99c9fc46f92e8a1c0dec1b1747d010903e884be1'] AS revshare_from_addresses,
    ['native'] AS token_addresses,
    ['ethereum'] AS expected_chains
  UNION ALL
  SELECT
    'mode' AS revshare_from_chain,
    ['0xed4811010a86f7c39134fbc20206d906ad1176b6'] AS revshare_from_addresses,
    ['native'] AS token_addresses,
    ['ethereum'] AS expected_chains
  UNION ALL
  SELECT
    'raas_conduit' AS revshare_from_chain,
    ['0x13f37e6b638ca83e7090bb3722b3ae04bf884019', '0x4a4962275DF8C60a80d3a25faEc5AA7De116A746', '0x09315fc454919a37d02d320272fc23a0653f67f9'] AS revshare_from_addresses,
    ['native'] AS token_addresses,
    ['ethereum'] AS expected_chains
  UNION ALL
  SELECT
    'lyra' AS revshare_from_chain,
    ['0x793e01dCf6F9759Bf26dd7869b03129e64217537'] AS revshare_from_addresses,
    ['native'] AS token_addresses,
    ['ethereum'] AS expected_chains
  UNION ALL
  SELECT
    'raas_gelato' AS revshare_from_chain,
    ['0xBeA2Bc852a160B8547273660E22F4F08C2fa9Bbb'] AS revshare_from_addresses,
    ['native'] AS token_addresses,
    ['ethereum'] AS expected_chains
  UNION ALL
  SELECT
    'raas_altlayer' AS revshare_from_chain,
    ['0x83AF9DD63534BF02921c8bE11f7428Ac70B05A1c'] AS revshare_from_addresses,
    ['native'] AS token_addresses,
    ['ethereum'] AS expected_chains
  UNION ALL
  SELECT
    'redstone' AS revshare_from_chain,
    ['0x7B60C79860ab5DB0368751210Ff29784d3DC18bF'] AS revshare_from_addresses,
    ['native'] AS token_addresses,
    ['ethereum'] AS expected_chains
  UNION ALL
  SELECT
    'swanchain' AS revshare_from_chain,
    ['0xe945D527De9c5121EdA9cF48e23CDF691894D4c0'] AS revshare_from_addresses,
    ['native'] AS token_addresses,
    ['ethereum'] AS expected_chains
  UNION ALL
  SELECT
    'kroma' AS revshare_from_chain,
    ['0x9d89Bca142498FE047bD3E169De8eD028AFaB07F'] AS revshare_from_addresses,
    ['native'] AS token_addresses,
    ['ethereum'] AS expected_chains
  UNION ALL
  SELECT
    'world' AS revshare_from_chain,
    ['0xb2aa0c2c4fd6bfcbf699d4c787cd6cc0dc461a9d'] AS revshare_from_addresses,
    ['native'] AS token_addresses,
    ['ethereum'] AS expected_chains
  UNION ALL
  SELECT
    'shape' AS revshare_from_chain,
    ['0xee1af3f99af8c5b93512fbe2a3f0dd5568ce087f'] AS revshare_from_addresses,
    ['native'] AS token_addresses,
    ['ethereum'] AS expected_chains
  UNION ALL
  SELECT
    'soneium' AS revshare_from_chain,
    ['0x641af675C39E56E5611686Ae07921AFb7d5C1f39', '0xF07b3169ffF67A8AECdBb18d9761AEeE34591112'] AS revshare_from_addresses,
    ['native'] AS token_addresses,
    ['ethereum'] AS expected_chains
  UNION ALL
  SELECT
    'ink' AS revshare_from_chain,
    ['0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0'] AS revshare_from_addresses,
    ['native'] AS token_addresses,
    ['ethereum'] AS expected_chains
  UNION ALL
  SELECT
    'unichain' AS revshare_from_chain,
    ['0x4300c0D3c0d3c0d3c0d3c0d3c0d3C0D3c0d30001'] AS revshare_from_addresses,
    ['native'] AS token_addresses,
    ['unichain'] AS expected_chains
),

to_addresses AS (
  SELECT
    '0x9c3631dDE5c8316bE5B7554B0CcD2631C15a9A05' AS to_address,
    'OP Receiver - Base' AS description,
    NULL AS end_date,
    ['base', 'ethereum'] AS expected_chains
  UNION ALL
  SELECT
    '0xa3d596eafab6b13ab18d40fae1a962700c84adea' AS to_address,
    'OP Receiver' AS description,
    NULL AS end_date,
    ['ethereum'] AS expected_chains
  UNION ALL
  SELECT
    '0x391716d440C151C42cdf1C95C1d83A5427Bca52C' AS to_address,
    'OP Receiver' AS description,
    '2024-07-04' AS end_date,
    ['ethereum'] AS expected_chains
  UNION ALL
  SELECT
    '0xC2eC5Dd60CBD5242b8cdeB9d0d37Ff6024584631' AS to_address,
    'OP Receiver' AS description,
    NULL AS end_date,
    ['ethereum'] AS expected_chains
  UNION ALL
  SELECT
    '0x4300C0D3C0D3C0D3C0d3C0d3c0d3C0d3C0d30002' AS to_address,
    'OP Receiver - Unichain' AS description,
    NULL AS end_date,
    ['unichain', 'ethereum'] AS expected_chains
  UNION ALL
  SELECT
    '0x16A27462B4D61BDD72CbBabd3E43e11791F7A28c' AS to_address,
    'OP Receiver - Soneium' AS description,
    NULL AS end_date,
    ['soneium', 'ethereum'] AS expected_chains
),

-- Native transfers
native_transfers AS (
  SELECT DISTINCT
    t.chain,
    t.dt,
    t.network,
    t.chain_id,
    t.block_timestamp,
    t.block_number,
    t.block_hash,
    t.transaction_hash,
    t.transaction_index,
    t.trace_address,
    t.from_address,
    t.to_address,
    CAST(t.amount_lossless AS UINT256) AS amount,
    t.amount_lossless,
    t.transfer_type,
    NULL AS token_address,
    f.revshare_from_chain,
    f.revshare_from_addresses,
    f.token_addresses,
    f.expected_chains AS from_expected_chains,
    ta.expected_chains AS to_expected_chains,
    ta.description AS to_address_description
  FROM INPUT_BLOCKBATCH('blockbatch/native_transfers/native_transfers_v1') t
    INNER JOIN from_addresses f
      ON hasAny(f.revshare_from_addresses, [t.from_address])
        AND t.chain = f.expected_chains[1]
    INNER JOIN to_addresses ta
      ON t.to_address = ta.to_address
        AND t.chain = ta.expected_chains[1]
        AND (ta.end_date IS NULL OR t.dt <= toDate(ta.end_date))
  WHERE t.amount_lossless IS NOT NULL
),

-- ERC20 transfers
erc20_transfers AS (
  SELECT DISTINCT
    t.chain,
    t.dt,
    t.network,
    t.chain_id,
    t.block_timestamp,
    t.block_number,
    t.block_hash,
    t.transaction_hash,
    t.transaction_index,
    t.trace_address,
    t.from_address,
    t.to_address,
    CAST(t.amount_lossless AS UINT256) AS amount,
    t.amount_lossless,
    'token' AS transfer_type,
    t.contract_address AS token_address,
    f.revshare_from_chain,
    f.revshare_from_addresses,
    f.token_addresses,
    f.expected_chains AS from_expected_chains,
    ta.expected_chains AS to_expected_chains,
    ta.description AS to_address_description
  FROM INPUT_BLOCKBATCH('blockbatch/token_transfers/erc20_transfers_v1') t
    INNER JOIN from_addresses f
      ON hasAny(f.revshare_from_addresses, [t.from_address])
        AND t.chain = f.expected_chains[1]
        AND t.contract_address = f.token_addresses[1]
    INNER JOIN to_addresses ta
      ON t.to_address = ta.to_address
        AND t.chain = ta.expected_chains[1]
        AND (ta.end_date IS NULL OR t.dt <= toDate(ta.end_date))
  WHERE t.amount_lossless IS NOT NULL
)

-- Combine native and ERC20 transfers
SELECT
  chain,
  dt,
  network,
  chain_id,
  block_timestamp,
  block_number,
  block_hash,
  transaction_hash,
  transaction_index,
  trace_address,
  from_address,
  to_address,
  amount,
  amount_lossless,
  transfer_type,
  token_address,
  revshare_from_chain,
  revshare_from_addresses,
  token_addresses,
  from_expected_chains,
  to_expected_chains,
  to_address_description
FROM native_transfers

UNION ALL

SELECT
  chain,
  dt,
  network,
  chain_id,
  block_timestamp,
  block_number,
  block_hash,
  transaction_hash,
  transaction_index,
  trace_address,
  from_address,
  to_address,
  amount,
  amount_lossless,
  transfer_type,
  token_address,
  revshare_from_chain,
  revshare_from_addresses,
  token_addresses,
  from_expected_chains,
  to_expected_chains,
  to_address_description
FROM erc20_transfers 