CREATE OR REPLACE VIEW etl_dashboard.blockbatch_loaddaily_markers_deduped AS
SELECT
  root_path
  , chain
  , dt
  , row_count
  , updated_at
FROM (
  SELECT
    root_path
    , chain
    , dt
    , loaded_row_count AS row_count
    , updated_at
    , row_number() OVER (PARTITION BY root_path, chain, dt ORDER BY updated_at DESC) AS rnum
  FROM etl_monitor.blockbatch_daily
  WHERE
    dt >= { dtmin: Date } AND dt <= { dtmax: Date } -- noqa: CP02
)
WHERE rnum = 1
