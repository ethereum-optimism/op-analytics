CREATE DATABASE IF NOT EXISTS etl_dashboard;

CREATE OR REPLACE VIEW etl_dashboard.blockbatch_markers_deduped AS
SELECT
  root_path
  , chain
  , dt
  , min_block
  , max_block
  , row_count
  , rownum
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
    dt >= { dtmin: Date } AND dt <= { dtmax: Date }
    AND root_path LIKE { prefix: String }
)
WHERE rownum = 1
