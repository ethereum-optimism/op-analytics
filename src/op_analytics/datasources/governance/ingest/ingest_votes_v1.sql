INSERT INTO _placeholder_
            
SELECT
  CAST(transaction_hash AS String) AS transaction_hash
  , CAST(proposal_id AS String) AS proposal_id
  , CAST(voter AS Nullable(String)) AS voter
  , CAST(support AS Nullable(Int64)) AS support
  , CAST(weight AS Nullable(String)) AS weight
  , CAST(reason AS Nullable(String)) AS reason
  , CAST(block_number AS Int64) AS block_number
  , CAST(params AS Nullable(String)) AS params
  , CAST(start_block AS Nullable(Int64)) AS start_block
  , CAST(description AS Nullable(String)) AS description
  , CAST(proposal_data AS Nullable(String)) AS proposal_data
  , CAST(proposal_type AS Nullable(String)) AS proposal_type
  , CAST(contract AS Nullable(String)) AS contract
  , CAST(chain_id AS Nullable(Int64)) AS chain_id
FROM 
  s3(
    'https://storage.googleapis.com/agora-optimism-public-usw1/v1/snapshot/votes'
    , 'CSVWithNames'
  )
-- Incremental ingestion (only ingest new data).
-- We assume that block_numbers are never added to the source out of order.
WHERE block_number > (
  -- NOTE: max() returns the default value of 0 if the table is empty.
  SELECT max(p.block_number) FROM _placeholder_ AS p
)
