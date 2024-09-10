CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
ENGINE = ReplacingMergeTree(insert_time)
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (chain_id, transaction_hash, trace_address, trace_type, block_number)
AS

-- Native Traces
SELECT

transaction_hash,
block_timestamp,
block_number,
trace_address, 
call_type, trace_type,
value as amount_raw,
from_address AS transfer_from_address,
to_address AS transfer_to_address,
substring(input,1,10) AS input_method_id,
chain_id, chain, network, insert_time

FROM {chain}_traces

WHERE value > 0
AND (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
and is_deleted = 0
and status = 1

