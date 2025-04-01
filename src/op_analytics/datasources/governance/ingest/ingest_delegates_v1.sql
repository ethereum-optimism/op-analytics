INSERT INTO _placeholder_

SELECT
  toDate(now()) AS dt
  , CAST(delegate AS String) AS delegate
  , CAST(num_of_delegators AS Nullable(Int64)) AS num_of_delegators
  , CAST(direct_vp AS Nullable(String)) AS direct_vp
  , CAST(advanced_vp AS Nullable(String)) AS advanced_vp
  , CAST(voting_power AS Nullable(String)) AS voting_power
  , CAST(contract AS Nullable(String)) AS contract
FROM s3(
  'https://storage.googleapis.com/agora-optimism-public-usw1/v1/snapshot/delegates'
  , 'CSVWithNames'
)
