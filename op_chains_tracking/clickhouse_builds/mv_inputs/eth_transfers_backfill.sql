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
chain_id, chain, network

FROM {chain}_traces

WHERE value > 0
AND (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
and is_deleted = 0
and status = 1
AND block_timestamp BETWEEN '{start_date}' AND '{end_date}'

UNION ALL

-- Legacy Bridge Transfers in Logs
SELECT
transaction_hash,
block_timestamp,
block_number,
toString(log_index) AS trace_address,
'log' as call_type, 'log' as trace_type,
reinterpretAsUInt256(reverse(unhex(substring(data, 3)))) AS amount_raw,
'0x' || lower(substring(splitByChar(',', topics)[2], 27, 40)) AS transfer_from_address,
'0x' || lower(substring(splitByChar(',', topics)[3], 27, 40)) AS transfer_to_address,
NULL AS input_method_id,
chain_id, chain, network
FROM {chain}_logs

WHERE address = lower('0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000')
and splitByChar(',', topics)[1] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
AND splitByChar(',', topics)[4] = '' --not an nft transfer
and is_deleted = 0
AND block_timestamp BETWEEN '{start_date}' AND '{end_date}'