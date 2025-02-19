CREATE OR REPLACE VIEW etl_dashboard.blockbatch_markers_missing AS

WITH
ingestion_markers AS (
  SELECT
    root_path
    , chain
    , dt
    , min_block
    , max_block
    , row_count
    , updated_at
  FROM
    etl_dashboard.blockbatch_markers_deduped(
      dtmin = { dtmin: Date } -- noqa: CP02
      , dtmax = { dtmax: Date } -- noqa: CP02
      , prefix = 'ingestion/blocks_v1%'
    )
)

, blockbatch_root_paths AS (
  SELECT DISTINCT root_path
  FROM
    etl_monitor.blockbatch_markers
  WHERE
    root_path LIKE 'blockbatch/%'
)

-- Build the expectation by cross joining with the expected models
, expected_markers AS (
  SELECT
    i.chain
    , i.dt
    , i.min_block
    , i.max_block
    , i.row_count
    , i.updated_at
    , b.root_path AS root_path -- noqa: AL09
  FROM
    ingestion_markers AS i
  CROSS JOIN blockbatch_root_paths AS b
)

, observed_markers AS (
  SELECT
    root_path
    , chain
    , dt
    , min_block
    , max_block
    , row_count
    , updated_at
  FROM
    etl_dashboard.blockbatch_markers_deduped(
      dtmin = { dtmin: Date } -- noqa: CP02
      , dtmax = { dtmax: Date } -- noqa: CP02
      , prefix = 'blockbatch/%'
    )
)

-- Find expected markers that are not on the observed markers table.
SELECT
  e.root_path
  , e.chain
  , e.dt
  , e.min_block
  , e.max_block
  , e.updated_at AS ingested_at
  , e.row_count AS ingestion_row_count
  , o.row_count AS observed_row_count
FROM
  expected_markers AS e
LEFT ANTI JOIN observed_markers AS o USING (root_path, chain, dt, min_block, max_block)
