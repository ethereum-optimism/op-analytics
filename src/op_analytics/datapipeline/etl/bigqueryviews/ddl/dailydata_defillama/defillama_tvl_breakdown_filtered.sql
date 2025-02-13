SELECT
  tvl.dt
  , tvl.chain
  , COALESCE(md.alignment, 'Other') AS alignment
  , tvl.protocol_category
  , tvl.parent_protocol
  , tvl.protocol_name
  , tvl.protocol_slug
  , tvl.is_double_counted
  , tvl.is_protocol_misrepresented
  , COALESCE(tm.token_category, 'Other') AS token_category
  , tvl.token
  , tm.project AS token_source_project
  , tm.source_protocol AS token_source_protocol
  , tvl.app_token_tvl
  , tvl.app_token_tvl_usd

FROM `oplabs-tools-data.dailydata_defillama.protocol_token_tvl_breakdown_v1` tvl 
LEFT JOIN `oplabs-tools-data.dailydata_defillama.token_mappings` tm 
  ON tvl.token = tm.token

-- NOTE: Chain metadata is something we would like to modify to another source
-- (i.e. gsheet + registry) in the future.
LEFT JOIN `oplabs-tools-data.api_table_uploads.op_stack_chain_metadata` md 
  ON tvl.chain = md.display_name

WHERE tvl.to_filter_out = 0
