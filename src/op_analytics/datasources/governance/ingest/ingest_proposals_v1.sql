INSERT INTO _placeholder_

SELECT
  toDate(now()) AS dt
  , CAST(proposal_id AS String) AS proposal_id
  , CAST(contract AS Nullable(String)) AS contract
  , CAST(proposer AS Nullable(String)) AS proposer
  , CAST(description AS Nullable(String)) AS description
  , CAST(ordinal AS Nullable(Int64)) AS ordinal
  , CAST(created_block AS Nullable(Int64)) AS created_block
  , CAST(start_block AS Nullable(Int64)) AS start_block
  , CAST(end_block AS Nullable(Int64)) AS end_block
  , CAST(queued_block AS Nullable(Int64)) AS queued_block
  , CAST(cancelled_block AS Nullable(Int64)) AS cancelled_block
  , CAST(executed_block AS Nullable(Int64)) AS executed_block
  , CAST(proposal_data AS Nullable(String)) AS proposal_data
  , CAST(proposal_data_raw AS Nullable(String)) AS proposal_data_raw
  , CAST(proposal_type AS Nullable(String)) AS proposal_type
  , CAST(proposal_type_data AS Nullable(String)) AS proposal_type_data
  , CAST(proposal_results AS Nullable(String)) AS proposal_results
  , CAST(created_transaction_hash AS Nullable(String)) AS created_transaction_hash
  , CAST(cancelled_transaction_hash AS Nullable(String)) AS cancelled_transaction_hash
  , CAST(queued_transaction_hash AS Nullable(String)) AS queued_transaction_hash
  , CAST(executed_transaction_hash AS Nullable(String)) AS executed_transaction_hash
  , CAST(proposal_type_id AS Nullable(Int64)) AS proposal_type_id
FROM s3(
  'https://storage.googleapis.com/agora-optimism-public-usw1/v1/snapshot/proposals_v2'
  , 'CSVWithNames'
)
