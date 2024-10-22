CREATE TABLE {view_name}
(
    address                 FixedString(42),
    wk                      DateTime,
    active_days_per_week    UInt64,
    weekly_retention_rate   Float64,
    is_active_week          UInt8,
    job_insert_time         DateTime
)

ENGINE = ReplacingMergeTree(job_insert_time)
PARTITION BY toYYYYMM(wk)
ORDER BY (address, wk)