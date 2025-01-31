CREATE DATABASE IF NOT EXISTS etl_dashboard;


CREATE VIEW etl_dashboard.blockbatch_markers_status AS 

WITH

-- Aggregated ingestion markers
ingestion_markers AS (
    SELECT
        root_path,
        chain,
        dt,
        min_blocks
    FROM 
        etl_monitor.blockbatch_markers_agged(
            dtmin = { dtmin: Date }, 
            dtmax = { dtmax: Date }, 
            prefix = 'ingestion/blocks_v1%')
),

blockbatch_root_paths AS (
    SELECT DISTINCT root_path FROM  etl_monitor.blockbatch_markers WHERE root_path LIKE 'blockbatch/%'
),

-- Build the expectation by cross joining with the expected models
expected_markers AS (
    SELECT
        chain,
        dt,
        min_blocks AS min_blocks_expected,
        b.root_path AS root_path
    FROM ingestion_markers CROSS JOIN blockbatch_root_paths b
),

-- Aggregated model markers
model_markers AS (
    SELECT
        root_path,
        chain,
        dt,
        covered_blocks,
        min_blocks AS min_blocks_model
    FROM 
        etl_monitor.blockbatch_markers_agged(
            dtmin = { dtmin: Date }, 
            dtmax = { dtmax: Date },
            prefix = 'blockbatch/%')
),

completion AS (
    SELECT
        root_path as model,
        chain,
        dt,
        covered_blocks,
        round(100.0 * covered_blocks / multiIf(
            chain='swan', 17280,
            chain='ham', 86400,
            chain='ink', 86400,
            chain='ink_sepolia', 86400,
            chain='unichain', 86400,
            chain='unichain_sepolia', 86400,
            43200
        ), 2) as pct_covered_blocks,
        round(100.0 * length(arrayIntersect(min_blocks_expected, min_blocks_model)) / length(min_blocks_expected), 2) as pct_expected
        
    FROM 
    expected_markers 
    LEFT JOIN model_markers USING(root_path, chain, dt)
)

SELECT
    model,
    chain,
    dt,
    covered_blocks,
    pct_covered_blocks,
    pct_expected,
    if(pct_covered_blocks >= 100, pct_expected,  least(pct_covered_blocks, pct_expected)) as pct_complete
FROM completion
OrDER BY model, dt
