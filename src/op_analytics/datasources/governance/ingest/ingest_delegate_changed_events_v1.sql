INSERT INTO transforms_governance.ingest_delegate_changed_events_v1
            
SELECT
  CAST(chain_id AS Int64) AS chain_id
  , CAST(address AS String) AS address
  , CAST(block_number AS Int64) AS block_number
  , CAST(block_hash AS Nullable(String)) AS block_hash
  , CAST(log_index AS Int64) AS log_index
  , CAST(transaction_index AS Int64) AS transaction_index
  , CAST(transaction_hash AS Nullable(String)) AS transaction_hash
  , CAST(delegator AS Nullable(String)) AS delegator
  , CAST(from_delegate AS Nullable(String)) AS from_delegate
  , CAST(to_delegate AS Nullable(String)) AS to_delegate
FROM 
  s3(
    'https://storage.googleapis.com/agora-optimism-public-usw1/v1/snapshot/delegate_changed_events'
    , 'CSVWithNames'
  )
-- Incremental ingestion (only ingest new data).
-- We assume that block_numbers are never added to the source out of order.
WHERE block_number > (
  -- NOTE: max() returns the default value of 0 if the table is empty.
  SELECT max(p.block_number) FROM _placeholder_ AS p
)
