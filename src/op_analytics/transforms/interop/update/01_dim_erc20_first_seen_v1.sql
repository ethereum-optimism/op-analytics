/**

Update with erc20 transfers from a given date.

*/

INSERT INTO _placeholder_

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
