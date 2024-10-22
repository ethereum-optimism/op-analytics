INSERT INTO {view_name}
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
AND block_timestamp BETWEEN '{start_date}' AND '{end_date}'

SETTINGS max_execution_time = 3000