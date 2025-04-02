SELECT
  -- Descriptors
  dt
  , chain
  , chain_id
  , network

  , uniq(from_address) AS distinct_from_addresses
  , sum(sum_l2_gas_used) AS total_l2_gas_used


FROM transforms_fees.agg_daily_from_address_summary FINAL
WHERE
  dt = { dtparam: Date }
GROUP BY
    dt
    , chain
    , chain_id
    , network