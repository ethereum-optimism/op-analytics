CREATE OR REPLACE VIEW etl_dashboard.blockbatch_loaddaily_markers_missing AS

WITH

datechain_root_paths AS (
  SELECT DISTINCT root_path
  FROM
    etl_monitor.blockbatch_daily
  WHERE chain != 'ALL'
)

, date_root_paths AS (
  SELECT DISTINCT root_path
  FROM
    etl_monitor.blockbatch_daily
  WHERE chain = 'ALL'
)


-- Build the expectation by cross joining with the expected models
, datechain_expected_markers AS (
  SELECT
    i.chain
    , i.dt
    , i.updated_at
    , b.root_path AS root_path -- noqa: AL09
  FROM (
    SELECT
      chain
      , dt
      , max(updated_at) AS updated_at
    FROM
      etl_dashboard.blockbatch_markers_deduped(
        dtmin = { dtmin: Date } -- noqa: CP02
        , dtmax = { dtmax: Date } -- noqa: CP02
        , prefix = 'ingestion/blocks_v1%'
      )
    GROUP BY
      chain
      , dt
  ) AS i
  CROSS JOIN datechain_root_paths AS b
)


, date_expected_markers AS (
  SELECT
    'ALL' AS chain
    , i.dt
    , i.updated_at
    , b.root_path AS root_path -- noqa: AL09
  FROM (
    SELECT
      dt
      , max(updated_at) AS updated_at
    FROM
      etl_dashboard.blockbatch_markers_deduped(
        dtmin = { dtmin: Date } -- noqa: CP02
        , dtmax = { dtmax: Date } -- noqa: CP02
        , prefix = 'ingestion/blocks_v1%'
      )
    GROUP BY
      dt
  ) AS i
  CROSS JOIN date_root_paths AS b
)

, expected_markers AS (
  SELECT * FROM datechain_expected_markers
  UNION ALL
  SELECT * FROM date_expected_markers
)

, observed_markers AS (
  SELECT
    root_path
    , chain
    , dt
    , updated_at
  FROM
    etl_dashboard.blockbatch_loaddaily_markers_deduped(
      dtmin = { dtmin: Date } -- noqa: CP02
      , dtmax = { dtmax: Date } -- noqa: CP02
    )
)


-- Find expected markers that are not on the observed markers table.
SELECT
  e.root_path
  , e.chain
  , e.dt
  , e.updated_at AS ingested_at
FROM
  expected_markers AS e
LEFT ANTI JOIN observed_markers AS o USING (root_path, chain, dt)
