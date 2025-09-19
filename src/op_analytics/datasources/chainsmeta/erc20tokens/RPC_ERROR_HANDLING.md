# RPC Error Handling Improvements

## Problem

The Dagster pipeline was failing with SSL errors when trying to connect to RPC endpoints:

```
requests.exceptions.SSLError: HTTPSConnectionPool(host='rpc.ham.fun', port=443): Max retries exceeded with url: / (Caused by SSLError(SSLError(1, '[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] sslv3 alert handshake failure (_ssl.c:1010)')))
```

The issue was that the existing retry logic only handled `RateLimit` exceptions, but SSL errors and other connection issues were not being caught and retried. This caused entire chains to fail when their RPC endpoints were unreachable.

## Solution

I implemented comprehensive RPC error handling across the codebase:

### 1. Enhanced Token-Level Error Handling (`tokens.py`)

- **Added new exception class**: `RPCConnectionError` for connection-related issues
- **Extended retry decorator**: Now retries on both `RateLimit` and `RPCConnectionError` exceptions
- **Added connection error catching**: Catches SSL, connection, timeout, and request exceptions
- **Improved error logging**: Better error messages with context

```python
@stamina.retry(on=(RateLimit, RPCConnectionError), attempts=3, wait_initial=10)
def call_rpc(self, ...):
    try:
        response = session.post(rpc_endpoint, json=self.rpc_batch())
    except (SSLError, ConnectionError, Timeout, RequestException) as ex:
        log.warning(f"RPC connection error for {self} at {rpc_endpoint}: {ex}")
        raise RPCConnectionError(f"Connection error to RPC endpoint: {ex}") from ex
```

### 2. Enhanced Chain-Level Error Handling (`chaintokens.py`)

- **Added exception handling**: Catches `RPCConnectionError` and other exceptions during token processing
- **Graceful degradation**: Continues processing other tokens even if individual tokens fail
- **Better error logging**: Logs connection errors at debug level to avoid spam

```python
try:
    token_metadata = token.call_rpc(...)
    # ... process token metadata
except RPCConnectionError as e:
    batch_failed += 1
    total_failed += 1
    log.debug(f"skipped token due to RPC connection error: {token}, {e}")
    continue
except Exception as e:
    batch_failed += 1
    total_failed += 1
    log.warning(f"skipped token due to unexpected error: {token}, {e}")
    continue
```

### 3. Enhanced Job-Level Error Handling (`execute.py`)

- **Switched to failure-tolerant execution**: Uses `run_concurrently_store_failures` instead of `run_concurrently`
- **Graceful chain skipping**: Continues processing other chains even if some chains fail
- **Comprehensive logging**: Logs failed chains but doesn't fail the entire job

```python
summary = run_concurrently_store_failures(
    function=_fetch,
    targets=targets,
    max_workers=16,
)

if summary.failures:
    for chain, error_msg in summary.failures.items():
        detail = f"failed chain={chain}: error={error_msg}"
        log.error(detail)
    
    log.warning(f"{len(summary.failures)} chains failed to execute, but continuing with successful chains")
```

### 4. Enhanced System Config Error Handling

Applied the same improvements to the system config RPC handling:

- **Updated `../systemconfig/rpc.py`**: Added `RPCConnectionError` and connection error handling
- **Updated `../systemconfig/chain.py`**: Added proper error handling for connection errors

## Benefits

1. **Resilience**: The pipeline now continues processing even when individual RPC endpoints are unreachable
2. **Better monitoring**: Failed chains are logged with detailed error messages
3. **Graceful degradation**: Partial success is better than complete failure
4. **Consistent retry logic**: All RPC calls now have consistent error handling and retry behavior

## Monitoring

The improved logging will help monitor RPC health:

- **Chain failures**: Logged as errors with chain name and error details
- **Token failures**: Logged at debug level to avoid log spam
- **Success rates**: Reported for each chain and overall job

## Future Improvements

1. **RPC health monitoring**: Track RPC endpoint availability over time
2. **Automatic fallback**: Switch to backup RPC endpoints when primary fails
3. **Circuit breaker**: Temporarily disable problematic RPC endpoints
4. **Metrics**: Add metrics for RPC success/failure rates 