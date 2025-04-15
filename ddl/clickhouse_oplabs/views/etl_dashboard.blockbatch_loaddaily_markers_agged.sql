CREATE OR REPLACE VIEW etl_dashboard.blockbatch_loaddaily_markers_agged AS
SELECT
  root_path
  , dt
  , sum(row_count) AS total_rows
  , groupUniqArray(chain) AS chains
FROM
  etl_dashboard.blockbatch_loaddaily_markers_deduped(
    dtmin = { dtmin: Date } -- noqa: CP02
    , dtmax = { dtmax: Date } -- noqa: CP02
  )
GROUP BY 1, 2
