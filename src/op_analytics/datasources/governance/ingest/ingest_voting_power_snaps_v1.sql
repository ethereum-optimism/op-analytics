INSERT INTO _placeholder_

SELECT
  CAST(id AS String) AS id
  , CAST(delegate AS Nullable(String)) AS delegate
  , CAST(balance AS Nullable(String)) AS balance
  , CAST(block_number AS Int64) AS block_number
  , CAST(ordinal AS Nullable(Int64)) AS ordinal
  , CAST(transaction_index AS Int64) AS transaction_index
  , CAST(log_index AS Int64) AS log_index
  , CAST(contract AS Nullable(String)) AS contract
FROM 
  s3(
    'https://storage.googleapis.com/agora-optimism-public-usw1/v1/snapshot/voting_power_snaps'
    ,'CSVWithNames'
  )
-- Incremental ingestion (only ingest new data).
-- We assume that block_numbers are never added to the source out of order.
WHERE block_number > (
  -- NOTE: max() returns the default value of 0 if the table is empty.
  SELECT max(p.block_number) FROM _placeholder_ AS p
)
