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
  -- Include all other columns in the tvl table.
  , tvl.* EXCEPT (
    dt
    , chain
    , protocol_category
    , parent_protocol
    , protocol_name
    , protocol_slug
    , is_double_counted
    , is_protocol_misrepresented
    , token
    , app_token_tvl
    , app_token_tvl_usd
  )

FROM `oplabs-tools-data.dailydata_defillama.tvl_flows_breakdown_filtered_v1` tvl 
LEFT JOIN `oplabs-tools-data.dailydata_defillama.latest_dim_token_mappings_v1` tm 
  ON tvl.token = tm.token

-- NOTE: Chain metadata is something we would like to modify to another source
-- (i.e. gsheet + registry) in the future.
LEFT JOIN `oplabs-tools-data.api_table_uploads.op_stack_chain_metadata` md 
  ON tvl.chain = md.display_name

WHERE tvl.to_filter_out = 0
AND tvl.dt >= '2018-01-01'
