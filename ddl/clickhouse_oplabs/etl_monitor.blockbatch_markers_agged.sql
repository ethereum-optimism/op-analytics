CREATE VIEW etl_monitor.blockbatch_markers_agged AS
SELECT
  root_path
  , chain
  , dt
  , sum(row_count) AS total_rows
  , sum(max_block - min_block) AS covered_blocks
  , groupUniqArray(min_block) AS min_blocks
FROM (
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
      dt >= { dtmin: Date } AND dt <= { dtmax: Date }
      AND root_path LIKE { prefix: String }
  )
  WHERE rownum = 1
)
GROUP BY 1, 2, 3
