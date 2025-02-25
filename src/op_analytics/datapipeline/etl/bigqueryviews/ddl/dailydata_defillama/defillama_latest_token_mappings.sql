SELECT
  token,
  MAX_BY(token_category, dt) AS token_category,
  MAX_BY(project, dt) AS project,
  MAX_BY(source_protocol, dt) AS source_protocol,
  MAX(dt) AS max_dt
 FROM `oplabs-tools-data.dailydata_defillama.dim_token_mappings_v1` 
WHERE dt >= '1900-01-01'
GROUP BY 1