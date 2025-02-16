-- Forward fact table inserts to the dim table.

CREATE MATERIALIZED VIEW chainsmeta.mv_erc20_token_metadata

TO chainsmeta.dim_erc20_token_metadata_v1 AS
SELECT
    chain
    , chain_id
    , contract_address
    --
    , decimals
    , symbol
    , name
    , total_supply
    , block_number
    , block_timestamp
FROM chainsmeta.fact_erc20_token_metadata_v1
