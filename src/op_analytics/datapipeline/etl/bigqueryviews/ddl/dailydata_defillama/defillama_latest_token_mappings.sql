SELECT
  token,
  MAX_BY(token_category, dt) AS token_category,
  MAX_BY(project, dt) AS project,
  MAX_BY(source_protocol, dt) AS source_protocol,
  MAX(dt) AS max_dt
 FROM `oplabs-tools-data.dailydata_defillama.dim_token_mappings_v1` 
WHERE dt = '2000-01-01' --default dt per coreutils/partitioned/dailydata.py
GROUP BY 1