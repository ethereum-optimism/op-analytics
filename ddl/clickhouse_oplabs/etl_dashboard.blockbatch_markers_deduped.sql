CREATE OR REPLACE VIEW etl_dashboard.blockbatch_markers_deduped AS
SELECT
  root_path
  , chain
  , dt
  , min_block
  , max_block
  , row_count
FROM (
  SELECT
    root_path
    , chain
    , dt
    , min_block
    , max_block
    , row_count
    , row_number() OVER (PARTITION BY root_path, chain, dt, min_block ORDER BY updated_at DESC) AS rownum
  FROM etl_monitor.blockbatch_markers
  WHERE
    dt >= { dtmin: Date } AND dt <= { dtmax: Date } -- noqa: CP02
    AND root_path LIKE { prefix: String } -- noqa: CP02
)
WHERE rownum = 1
