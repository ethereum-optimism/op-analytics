CREATE OR REPLACE VIEW etl_dashboard.blockbatch_load_markers_deduped AS
SELECT
  root_path
  , chain
  , dt
  , min_block
  , row_count
  , updated_at
FROM (
  SELECT
    root_path
    , chain
    , dt
    , min_block
    , loaded_row_count AS row_count
    , updated_at
    , row_number() OVER (PARTITION BY root_path, chain, dt, min_block ORDER BY updated_at DESC) AS rnum
  FROM etl_monitor.blockbatch_markers_datawarehouse
  WHERE
    dt >= { dtmin: Date } AND dt <= { dtmax: Date } -- noqa: CP02
    AND root_path LIKE { prefix: String } -- noqa: CP02
)
WHERE rnum = 1
