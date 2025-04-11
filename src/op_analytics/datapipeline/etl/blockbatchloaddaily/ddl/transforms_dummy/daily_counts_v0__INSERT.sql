SELECT
  dt
  , count(*) AS num_addresses
  , sum(count_transactions) AS num_transactions

FROM INPUT_CLICKHOUSE('blockbatch_daily/aggtxs/daily_address_summary_v1')
GROUP BY 1
