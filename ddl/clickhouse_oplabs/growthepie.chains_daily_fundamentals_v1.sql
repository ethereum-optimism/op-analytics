CREATE DATABASE IF NOT EXISTS growthepie;

CREATE TABLE IF NOT EXISTS growthepie.chains_daily_fundamentals_v1
(
    `version` DateTime DEFAULT now(),
    `dt` Date,
    `metric_key` String,
    `origin_key` String,
    `value` Float64,
    
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (dt, metric_key, origin_key)
