WITH
    -- Step 1: Select daily stats per wallet for the specific target date/chain list
    DailyWalletActivity AS (
        SELECT
            dt, chain, chain_id, network, from_address,
            -- Select all relevant columns from the schema needed for aggregation
            count_transactions, count_success_transactions, count_distinct_blocks,
            count_distinct_success_blocks, count_distinct_active_hours,
            count_distinct_to_address, count_distinct_success_to_address,
            count_distinct_method_id, active_block_range, active_nonce_range,
            active_time_range, sum_l2_gas_used, sum_success_l2_gas_used,
            sum_l1_gas_used_unified, sum_success_l1_gas_used_unified, sum_tx_fee_native,
            sum_success_tx_fee_native, sum_l2_fee_native, sum_success_l2_fee_native,
            sum_l2_base_fee_native, sum_success_l2_base_fee_native, sum_l2_priority_fee_native,
            sum_success_l2_priority_fee_native, sum_l2_legacy_extra_fee_native,
            sum_success_l2_legacy_extra_fee_native, sum_l1_fee_native, sum_success_l1_fee_native,
            sum_l1_base_fee_native, sum_success_l1_base_fee_native, sum_l1_blob_fee_native,
            sum_success_l1_blob_fee_native, avg_l2_gas_price_gwei, avg_l2_priority_gas_price_gwei,
            avg_l2_legacy_extra_gas_price_gwei, avg_l1_base_gas_price_gwei,
            avg_l1_blob_base_gas_price_gwei, sum_input_zero_bytes, sum_success_input_zero_bytes,
            sum_input_bytes_length, sum_success_input_bytes_length, sum_input_nonzero_bytes,
            sum_success_input_nonzero_bytes, sum_estimated_size, sum_success_estimated_size
        FROM transforms_fees.agg_daily_from_address_summary FINAL
        WHERE dt = {dtparam: Date}
          -- Optional: Filter for the list of specified chains using IN operator
        --   AND chain IN ('base', 'op')
          AND count_transactions > 0
    ),

    -- Step 2: Assign cohorts based on DAILY activity, pass through all selected metrics
    DailyWalletCohorts AS (
        SELECT
            *, -- Select all columns from DailyWalletActivity
            -- Assign cohorts based on the final agreed logic
            multiIf(
                count_transactions >= 501, '>500_tx_per_day',    -- Tier5: Automated (High Freq)
                count_transactions >= 51,  '51-500_tx_per_day',  -- Tier4: Automated (Low Freq)
                count_transactions >= 16,  '16-50_tx_per_day',   -- Tier3: Power users
                count_transactions >= 2,   '2-15_tx_per_day',    -- Tier2: Organic activity
                count_transactions = 1,    '1_tx_per_day',       -- Tier1: Organic activity (Single)
                'undefined' -- Fallback
            ) AS cohort
        FROM DailyWalletActivity
    )

-- Step 3: Aggregate ALL metrics using GROUPING SETS for both cohort and baseline levels
SELECT
    dt, chain, chain_id, network,
    -- Use grouping() function to determine level and assign 'Baseline' label
    -- grouping(cohort) = 1 means this row is aggregated across cohorts (it's a baseline row)
    if(grouping(cohort) = 1, 'Baseline', cohort) AS final_cohort,

    -- Aggregate Calculations (Performed ONCE thanks to GROUPING SETS)
    -- Base Metrics
    count(DISTINCT from_address) AS wallet_count,
    avg(count_transactions) AS avg_tx_count_per_wallet,
    sum(count_transactions) AS total_tx_count,
    sum(count_success_transactions) AS total_success_tx_count,
    avg(count_success_transactions) AS avg_success_tx_count_per_wallet,
    sum(count_success_transactions) / nullIf(sum(count_transactions), 0) AS avg_success_rate,
    -- Aggregated Distinct Counts (Averaged per Wallet)
    avg(count_distinct_blocks) AS avg_distinct_blocks_per_wallet,
    avg(count_distinct_success_blocks) AS avg_distinct_success_blocks_per_wallet,
    avg(count_distinct_active_hours) AS avg_distinct_active_hours_per_wallet,
    avg(count_distinct_to_address) AS avg_distinct_to_address_per_wallet,
    avg(count_distinct_success_to_address) AS avg_distinct_success_to_address_per_wallet,
    avg(count_distinct_method_id) AS avg_distinct_method_id_per_wallet,
    -- Aggregated Ranges (Averaged per Wallet)
    avg(active_block_range) AS avg_active_block_range_per_wallet,
    avg(active_nonce_range) AS avg_active_nonce_range_per_wallet,
    avg(active_time_range) AS avg_active_time_range_sec_per_wallet,
    -- Aggregated Gas Usage (Totals & Avg Per Wallet)
    sum(sum_l2_gas_used) AS total_l2_gas_used,                      avg(sum_l2_gas_used) AS avg_l2_gas_used_per_wallet,
    sum(sum_success_l2_gas_used) AS total_success_l2_gas_used,      avg(sum_success_l2_gas_used) AS avg_success_l2_gas_used_per_wallet,
    sum(sum_l1_gas_used_unified) AS total_l1_gas_used,              avg(sum_l1_gas_used_unified) AS avg_l1_gas_used_per_wallet,
    sum(sum_success_l1_gas_used_unified) AS total_success_l1_gas_used, avg(sum_success_l1_gas_used_unified) AS avg_success_l1_gas_used_per_wallet,
    -- Aggregated Fees (Totals & Avg Per Wallet)
    sum(sum_tx_fee_native) AS total_tx_fee_native,                  avg(sum_tx_fee_native) AS avg_tx_fee_native_per_wallet,
    sum(sum_success_tx_fee_native) AS total_success_tx_fee_native,  avg(sum_success_tx_fee_native) AS avg_success_tx_fee_native_per_wallet,
    sum(sum_l2_fee_native) AS total_l2_fee_native,                  avg(sum_l2_fee_native) AS avg_l2_fee_native_per_wallet,
    sum(sum_success_l2_fee_native) AS total_success_l2_fee_native,  avg(sum_success_l2_fee_native) AS avg_success_l2_fee_native_per_wallet,
    sum(sum_l2_base_fee_native) AS total_l2_base_fee_native,        avg(sum_l2_base_fee_native) AS avg_l2_base_fee_native_per_wallet,
    sum(sum_success_l2_base_fee_native) AS total_success_l2_base_fee_native, avg(sum_success_l2_base_fee_native) AS avg_success_l2_base_fee_native_per_wallet,
    sum(sum_l2_priority_fee_native) AS total_l2_priority_fee_native, avg(sum_l2_priority_fee_native) AS avg_l2_priority_fee_native_per_wallet,
    sum(sum_success_l2_priority_fee_native) AS total_success_l2_priority_fee_native, avg(sum_success_l2_priority_fee_native) AS avg_success_l2_priority_fee_native_per_wallet,
    sum(sum_l2_legacy_extra_fee_native) AS total_l2_legacy_extra_fee_native, avg(sum_l2_legacy_extra_fee_native) AS avg_l2_legacy_extra_fee_native_per_wallet,
    sum(sum_success_l2_legacy_extra_fee_native) AS total_success_l2_legacy_extra_fee_native, avg(sum_success_l2_legacy_extra_fee_native) AS avg_success_l2_legacy_extra_fee_native_per_wallet,
    sum(sum_l1_fee_native) AS total_l1_fee_native,                  avg(sum_l1_fee_native) AS avg_l1_fee_native_per_wallet,
    sum(sum_success_l1_fee_native) AS total_success_l1_fee_native,  avg(sum_success_l1_fee_native) AS avg_success_l1_fee_native_per_wallet,
    sum(sum_l1_base_fee_native) AS total_l1_base_fee_native,        avg(sum_l1_base_fee_native) AS avg_l1_base_fee_native_per_wallet,
    sum(sum_success_l1_base_fee_native) AS total_success_l1_base_fee_native, avg(sum_success_l1_base_fee_native) AS avg_success_l1_base_fee_native_per_wallet,
    sum(sum_l1_blob_fee_native) AS total_l1_blob_fee_native,        avg(sum_l1_blob_fee_native) AS avg_l1_blob_fee_native_per_wallet,
    sum(sum_success_l1_blob_fee_native) AS total_success_l1_blob_fee_native, avg(sum_success_l1_blob_fee_native) AS avg_success_l1_blob_fee_native_per_wallet,
    -- Aggregated Gas Prices (Averaged across Wallets)
    avg(avg_l2_gas_price_gwei) AS avg_l2_gas_price_gwei_across_wallets,
    avg(avg_l2_priority_gas_price_gwei) AS avg_l2_priority_gas_price_gwei_across_wallets,
    avg(avg_l2_legacy_extra_gas_price_gwei) AS avg_l2_legacy_extra_gas_price_gwei_across_wallets,
    avg(avg_l1_base_gas_price_gwei) AS avg_l1_base_gas_price_gwei_across_wallets,
    avg(avg_l1_blob_base_gas_price_gwei) AS avg_l1_blob_base_gas_price_gwei_across_wallets,
    -- Aggregated Input Data (Totals & Avg Per Wallet)
    sum(sum_input_zero_bytes) AS total_input_zero_bytes,           avg(sum_input_zero_bytes) AS avg_input_zero_bytes_per_wallet,
    sum(sum_success_input_zero_bytes) AS total_success_input_zero_bytes, avg(sum_success_input_zero_bytes) AS avg_success_input_zero_bytes_per_wallet,
    sum(sum_input_bytes_length) AS total_input_bytes_length,       avg(sum_input_bytes_length) AS avg_input_bytes_length_per_wallet,
    sum(sum_success_input_bytes_length) AS total_success_input_bytes_length, avg(sum_success_input_bytes_length) AS avg_success_input_bytes_length_per_wallet,
    sum(sum_input_nonzero_bytes) AS total_input_nonzero_bytes,     avg(sum_input_nonzero_bytes) AS avg_input_nonzero_bytes_per_wallet,
    sum(sum_success_input_nonzero_bytes) AS total_success_input_nonzero_bytes, avg(sum_success_input_nonzero_bytes) AS avg_success_input_nonzero_bytes_per_wallet,
    -- Aggregated Estimated Size (Totals & Avg Per Wallet)
    sum(sum_estimated_size) AS total_estimated_size,               avg(sum_estimated_size) AS avg_estimated_size_per_wallet,
    sum(sum_success_estimated_size) AS total_success_estimated_size, avg(sum_success_estimated_size) AS avg_success_estimated_size_per_wallet,
    -- Key Ratios per Transaction
    sum(sum_tx_fee_native) / nullIf(sum(count_transactions), 0) AS avg_tx_fee_native_per_tx,
    sum(sum_l2_gas_used) / nullIf(sum(count_transactions), 0) AS avg_l2_gas_per_tx,
    sum(sum_l1_gas_used_unified) / nullIf(sum(count_transactions), 0) AS avg_l1_gas_per_tx

FROM DailyWalletCohorts
-- Use GROUPING SETS to get aggregates for both cohort level and baseline level in one go
GROUP BY GROUPING SETS (
    (dt, chain, chain_id, network, cohort), -- Grouping level 1: Detailed cohort level
    (dt, chain, chain_id, network)          -- Grouping level 2: Baseline level (aggregating over cohorts)
)
-- Order the final results
ORDER BY
    dt ASC, chain ASC, network ASC,
    -- Custom sort order using the calculated final_cohort
    multiIf(
        final_cohort = '>500_tx_per_day', 1,    -- Automated (High Freq)
        final_cohort = '51-500_tx_per_day', 2,  -- Automated (Low Freq)
        final_cohort = '16-50_tx_per_day', 3,   -- Power users
        final_cohort = '2-15_tx_per_day', 4,    -- Organic activity
        final_cohort = '1_tx_per_day', 5,       -- Organic activity (Single)
        final_cohort = 'Baseline', 6,          -- Baseline
        99 -- Fallback
    ) ASC,
    chain_id ASC