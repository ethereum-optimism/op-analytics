CREATE VIEW etl_monitor.blockbatch_markers_agged AS
SELECT
  root_path
  , chain
  , dt
  , SUM(row_count) AS total_rows
  , SUM(max_block - min_block) AS covered_blocks
  , GROUPUNIQARRAY(min_block) AS min_blocks
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
      , ROW_NUMBER() OVER (PARTITION BY root_path, chain, dt ORDER BY updated_at DESC) AS rownum
    FROM etl_monitor.blockbatch_markers
    WHERE
      dt >= { dtmin: date } AND dt <= { dtmax: date }
      AND root_path LIKE { prefix: string }
  )
  WHERE rownum = 1
)
GROUP BY 1, 2, 3
