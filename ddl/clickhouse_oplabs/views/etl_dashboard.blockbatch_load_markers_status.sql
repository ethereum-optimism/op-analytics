CREATE OR REPLACE VIEW etl_dashboard.blockbatch_load_markers_status AS

WITH

-- Aggregated ingestion markers
ingestion_markers AS (
  SELECT
    root_path
    , chain
    , dt
    , min_blocks
  FROM
    etl_dashboard.blockbatch_markers_agged(
      dtmin = { dtmin: Date } -- noqa: CP02
      , dtmax = { dtmax: Date } -- noqa: CP02
      , prefix = 'ingestion/blocks_v1%'
    )
)

, blockbatch_root_paths AS (
  SELECT DISTINCT root_path FROM etl_monitor.blockbatch_markers_datawarehouse
)

-- Build the expectation by cross joining with the expected models
, expected_markers AS (
  SELECT
    i.chain
    , i.dt
    , i.min_blocks AS min_blocks_expected
    , b.root_path AS root_path -- noqa: AL09
  FROM ingestion_markers AS i CROSS JOIN blockbatch_root_paths AS b
)

-- Aggregated model markers
, model_markers AS (
  SELECT
    root_path
    , chain
    , dt
    , min_blocks AS min_blocks_model
  FROM
    etl_dashboard.blockbatch_load_markers_agged(
      dtmin = { dtmin: Date } -- noqa: CP02
      , dtmax = { dtmax: Date } -- noqa: CP02
      , prefix = 'blockbatch/%'
    )
)

, completion AS (
  SELECT
    root_path AS model
    , chain
    , dt
    , round(100.0 * length(arrayIntersect(min_blocks_expected, min_blocks_model)) / length(min_blocks_expected), 2) AS pct_expected

  FROM
    expected_markers
  LEFT JOIN
    model_markers
    ON expected_markers.root_path = model_markers.root_path AND expected_markers.chain = model_markers.chain AND expected_markers.dt = model_markers.dt
)

SELECT
model
, chain
, dt
, pct_expected AS pct_complete
FROM completion
ORDER BY model, dt
