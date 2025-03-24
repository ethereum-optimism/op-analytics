CREATE OR REPLACE VIEW _placeholder_ AS

SELECT 
  *
FROM transforms_fees.agg_daily_transactions_grouping_sets FINAL
WHERE 
  from_address != '--'
  AND to_address != '--'
  AND method_id != '--'
