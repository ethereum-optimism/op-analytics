WITH filtered_events AS (
    SELECT lower(topic) AS topic, description
    FROM (
                  SELECT '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925' as topic, 'ERC20 Approval' AS description
        UNION ALL SELECT'0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31' as topic, 'ERC721/ERC1155 Approval' AS description
        UNION ALL SELECT'0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c' as topic, 'WETH Wrap' AS description
        UNION ALL SELECT'0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65' as topic, 'WETH Unwrap' AS description
        ) a 
)

, raw_transactions AS (
SELECT 
DATE_TRUNC('day',block_timestamp) AS dt,
  chain AS blockchain,
  chain_id, --db chain_id
  '@name@' as name,
  '@layer@' AS layer,
COUNT(*) AS num_raw_txs,
COUNTIf(receipt_status = 1) AS num_success_txs

FROM @blockchain@_transactions
WHERE gas_price > 0
AND block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '@trailing_days@ days')
AND is_deleted = 0 --not deleted
GROUP BY 1,2,3,4,5
)

, log_transactions AS (
SELECT 
DATE_TRUNC('day',block_timestamp) AS dt,
  chain_id, --db chain_id
COUNT(DISTINCT transaction_hash) AS num_qualified_txs

FROM @blockchain@_logs
WHERE substring(topics, 1, position(topics, ',') - 1) NOT IN (SELECT topic FROM filtered_events)
AND block_timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '@trailing_days@ days')
AND is_deleted = 0 --not deleted
GROUP BY 1,2
)

SELECT
r.dt, r.blockchain, r.name, r.chain_id, r.layer, num_raw_txs, num_success_txs, num_qualified_txs
FROM raw_transactions r 
    LEFT JOIN log_transactions l 
        ON r.chain_id = l.chain_id
        AND r.dt = l.dt
