WITH filtered_events AS (
    SELECT lower(topic) AS topic, description
    FROM (values
        ('0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925', 'ERC20 Approval')
        ,('0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31', 'ERC721/ERC1155 Approval')
        ,('0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c', 'WETH Wrap')
        ,('0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65', 'WETH Unwrap')
        ) a (topic, description)
)

SELECT 
DATE_TRUNC('day',block_timestamp) AS dt,
  '@blockchain@' AS blockchain,
  '@name@' as name,
  '@layer@' AS layer,
COUNT(DISTINCT tx_hash) AS num_qualified_txs

FROM @blockchain@.core.fact_event_logs
WHERE topics[0] NOT IN (SELECT topic FROM filtered_events)
AND block_timestamp >= DATE_TRUNC('day', CURRENT_TIMESTAMP() - INTERVAL '@trailing_days@ days')

GROUP BY 1,2,3,4