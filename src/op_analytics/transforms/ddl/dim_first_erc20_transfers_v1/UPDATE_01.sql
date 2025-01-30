/**

Update the dim_first_transfers_v1 with erc20 transfers from a given date.

*/

INSERT INTO transforms.dim_first_erc20_transfers_v1

WITH

current_state AS (
  SELECT
    chain
    , contract_address
    , first_transfer
    , row_version
  FROM transforms.dim_first_transfers_v1
)

, new_information AS (
  SELECT
    chain
    , contract_address
    , min(block_timestamp) AS first_transfer

    -- We set row_version as the time in seconds to a future timestmap, so that
    -- earlier transfers have a greater row_version. This is needed due to how
    -- the ReplacingMergeTree engine keeps only the max row_version value.
    , dateDiff('second', first_transfer, '2106-01-01 00:00:00'::DATETIME) AS row_version

  FROM blockbatch.token_transfers__erc20_transfers_v1
  WHERE
    dt = {dtparam:Date} -- noqa: LT01,CP02
  GROUP BY 1, 2
)

SELECT
  n.chain
  , n.contract_address
  , n.first_transfer
  , n.row_version
FROM new_information AS n LEFT JOIN current_state AS c
  ON n.chain = c.chain AND n.contract_address = c.contract_address
WHERE
  -- When the row does not exist on current_state ClikcHouse uses the
  -- default value (instead of NULL) so this keeps new rows only.
  c.row_version = 0

  -- This is an earlier transfer, so the new version is greater than
  -- the current version.
  OR n.row_version > c.row_version
