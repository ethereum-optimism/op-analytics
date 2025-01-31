CREATE OR REPLACE VIEW etl_dashboard.blockbatch_markers_agged AS
SELECT
  root_path
  , chain
  , dt
  , sum(row_count) AS total_rows
  , sum(max_block - min_block) AS covered_blocks
  , groupuniqarray(min_block) AS min_blocks
FROM
  etl_monitor.blockbatch_markers_deduped(
    dtmin = { dtmin: date }
    , dtmax = { dtmax: date }
    , prefix = { prefix: string }
  )
GROUP BY 1, 2, 3
