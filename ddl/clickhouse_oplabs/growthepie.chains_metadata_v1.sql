CREATE DATABASE IF NOT EXISTS growthepie;

CREATE TABLE IF NOT EXISTS growthepie.chains_metadata_v1 (
    `dt` Date,
    `name` String,
    `url_key` String,
    `chain_type` Nullable(String),
    `caip2` Nullable(String),
    `evm_chain_id` Nullable(Int64),
    `deployment` Nullable(String),
    `name_short` Nullable(String),
    `description` Nullable(String),
    `da_layer` Nullable(String),
    `symbol` Nullable(String),
    `bucket` Nullable(String),
    `ecosystem` Array(Nullable(String)),
    `colors` Tuple(
        light Array(Nullable(String)),
        dark Array(Nullable(String)),
        darkTextOnBackground Nullable(Bool)
    ),
    `logo` Tuple(
        body Nullable(String),
        width Nullable(Int64),
        height Nullable(Int64)
    ),
    `technology` Nullable(String),
    `purpose` Nullable(String),
    `launch_date` Nullable(String),
    `enable_contracts` Nullable(Bool),
    `l2beat_stage` Tuple(
        stage Nullable(String),
        hex Nullable(String)
    ),
    `l2beat_link` Nullable(String),
    `l2beat_id` Nullable(String),
    `raas` Nullable(String),
    `stack` Tuple(
        label Nullable(String),
        url Nullable(String)
    ),
    `website` Nullable(String),
    `twitter` Nullable(String),
    `block_explorer` Nullable(String),
    `block_explorers` Tuple(
        Etherscan Nullable(String),
        Blockscout Nullable(String),
        Arbiscan Nullable(String),
        BaseScan Nullable(String),
        `Blast Explorer` Nullable(String),
        Immutascan Nullable(String),
        LineaScan Nullable(String),
        `Loopring Explorer` Nullable(String),
        `Optimistic Etherscan` Nullable(String),
        PolygonScan Nullable(String),
        `Rhino Explorer` Nullable(String),
        ScrollScan Nullable(String),
        StarkScan Nullable(String),
        Voyager Nullable(String),
        Taikoscan Nullable(String),
        `Alchemy Explorer` Nullable(String),
        `zkSync Explorer` Nullable(String)
    ),
    `rhino_listed` Nullable(Bool),
    `rhino_naming` Nullable(String),
    `origin_key` Nullable(String)
) ENGINE = ReplacingMergeTree(dt)
ORDER BY
    (dt, name, url_key)