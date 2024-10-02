WITH filtered_events AS (
    SELECT lower(topic) AS topic, description
    FROM (
                  SELECT '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925' as topic, 'ERC20 Approval' AS description
        UNION ALL SELECT'0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31' as topic, 'ERC721/ERC1155 Approval' AS description
        UNION ALL SELECT'0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c' as topic, 'WETH Wrap' AS description
        UNION ALL SELECT'0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65' as topic, 'WETH Unwrap' AS description
        ) a 
)

, block_ranges AS (
SELECT min(number) AS min_num, max(number) AS max_num
    from @blockchain@_blocks
    where timestamp >= DATE_TRUNC('day', NOW() - INTERVAL '@trailing_days@ days')
        and timestamp < toDate(NOW())
)

SELECT 
DATE_TRUNC('day',block_timestamp) AS dt,
  chain AS blockchain,
  chain_id, --db chain_id
  '@name@' as name,
  '@layer@' AS layer,
COUNT(*) AS num_raw_txs,
COUNTIf(receipt_status = 1) AS num_success_txs,
COUNTIf(is_qualified = 1) AS num_qualified_txs
FROM (
  SELECT block_timestamp, chain, chain_id, hash, receipt_status
    , CASE WHEN (l.transaction_hash IS NOT NULL) AND (l.transaction_hash != '') THEN 1 ELSE 0 END AS is_qualified
  FROM @blockchain@_transactions t
  LEFT JOIN (
      SELECT chain, block_number,block_timestamp,transaction_hash FROM @blockchain@_logs l 
      WHERE is_deleted = 0 --not deleted
      AND substring(l.topics, 1, position(l.topics, ',') - 1) NOT IN (SELECT topic FROM filtered_events)
      AND l.block_number >= (SELECT min_num FROM block_ranges)
      AND l.block_number < (SELECT max_num FROM block_ranges)
      GROUP BY 1,2,3,4
    ) l
    ON l.chain = t.chain
    AND l.block_number = t.block_number
    AND l.block_timestamp = t.block_timestamp
    AND l.transaction_hash = t.hash

  WHERE gas_price > 0
    AND t.block_number >= (SELECT min_num FROM block_ranges)
    AND t.block_number < (SELECT max_num FROM block_ranges)
  AND t.is_deleted = 0 --not deleted

  GROUP BY 1,2,3,4,5,6
)
GROUP BY 1,2,3,4,5

