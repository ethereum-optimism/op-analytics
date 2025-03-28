CREATE OR REPLACE VIEW etl_dashboard.blockbatch_loaddaily_markers_status AS

WITH

-- Aggregated ingestion markers
ingestion_markers AS (
  SELECT
    dt
    , groupUniqArray(chain) AS chains
  FROM
    etl_dashboard.blockbatch_markers_agged(
      dtmin = { dtmin: Date } -- noqa: CP02
      , dtmax = { dtmax: Date } -- noqa: CP02
      , prefix = 'ingestion/blocks_v1%'
    )
  GROUP BY
    dt
)

, blockbatch_root_paths AS (
  SELECT DISTINCT root_path FROM etl_monitor.blockbatch_daily
)

-- Build the expectation by cross joining with the expected models
, expected_markers AS (
  SELECT
    i.dt
    , i.chains AS chains_expected
    , b.root_path AS root_path -- noqa: AL09
  FROM ingestion_markers AS i CROSS JOIN blockbatch_root_paths AS b
)

-- Aggregated model markers
, model_markers AS (
  SELECT
    root_path
    , dt
    , chains AS chains_observed
  FROM
    etl_dashboard.blockbatch_loaddaily_markers_agged(
      dtmin = { dtmin: Date } -- noqa: CP02
      , dtmax = { dtmax: Date } -- noqa: CP02
    )
)

, completion AS (
  SELECT
    root_path AS model
    , dt
    , round(100.0 * length(arrayIntersect(chains_expected, chains_observed)) / length(chains_expected), 2) AS pct_expected

  FROM
    expected_markers
  LEFT JOIN model_markers
    ON
      expected_markers.root_path = model_markers.root_path
      AND expected_markers.dt = model_markers.dt
)

SELECT
model
, dt
, pct_expected AS pct_complete
FROM completion
ORDER BY model, dt
