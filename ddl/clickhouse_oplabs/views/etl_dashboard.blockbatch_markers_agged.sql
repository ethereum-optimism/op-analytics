CREATE OR REPLACE VIEW etl_dashboard.blockbatch_markers_agged AS
SELECT
  root_path
  , chain
  , dt
  , sum(row_count) AS total_rows
  , sum(max_block - min_block) AS covered_blocks
  , groupUniqArray(min_block) AS min_blocks
FROM
  etl_dashboard.blockbatch_markers_deduped(
    dtmin = { dtmin: Date } -- noqa: CP02
    , dtmax = { dtmax: Date } -- noqa: CP02
    , prefix = { prefix: String } -- noqa: CP02
  )
GROUP BY 1, 2, 3
