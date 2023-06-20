with bots AS (
    SELECT t.from_address AS sender, COUNT(*) AS num_txs
    FROM transactions2 t
--     WHERE t.block_timestamp > NOW() - interval '7' day
    GROUP BY 1
    HAVING COUNT(*) >= 500--1000.00*7.0
)
SELECT to_address,
    num_txs, 100*num_txs / SUM(num_txs) OVER () AS pct_num_txs,
    l2_gas, 100*l2_gas / SUM(l2_gas) OVER () AS pct_l2_gas,
    l1_gas, 100*l1_gas / SUM(l1_gas) OVER () AS pct_l1_gas,
    max_bt
    
FROM (
    SELECT to_address, COUNT(*) AS num_txs, SUM(gas) AS l2_gas, SUM(receipt_l1_gas_used) AS l1_gas, MAX(block_timestamp) AS max_bt
        FROM transactions2
    WHERE to_address NOT IN (SELECT from_address FROM transactions2) --filter eoas
    AND to_address != '0x4200000000000000000000000000000000000015'
    AND from_address NOT IN (SELECT sender from bots)
    GROUP BY 1
    ) a
  ORDER BY num_txs DESC