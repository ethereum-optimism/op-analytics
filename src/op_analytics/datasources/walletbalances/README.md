# Wallet ERC20 Token Balance Puller

This module provides functionality to query ERC20 token balances for multiple wallet addresses using JSON-RPC calls.

## Features

- Read wallet addresses from CSV files
- Read ERC20 token addresses from CSV files  
- Query current balances for all wallet-token combinations
- Automatically fetch token decimals if not provided
- Rate limiting to avoid RPC endpoint limits
- Retry logic with exponential backoff
- Export results to CSV with both raw and formatted balances

## Usage

### Command Line Interface

The wallet balance puller is available as a CLI command through the `opdata` tool:

```bash
uv run opdata walletbalances pull-balances \
    --wallets-csv wallets.csv \
    --tokens-csv tokens.csv \
    --output-csv balances.csv \
    --rpc-endpoint https://mainnet.optimism.io/ \
    --rate-limit-delay 0.1
```

### Input CSV Formats

#### Wallet Addresses CSV
The wallet CSV file must contain an `address` column. Optional `label` column for wallet names.

```csv
address,label
0x1234567890123456789012345678901234567890,Example Wallet 1
0xabcdefabcdefabcdefabcdefabcdefabcdefabcd,Example Wallet 2
```

#### Token Addresses CSV  
The token CSV file must contain an `address` column. Optional `symbol` and `decimals` columns.

```csv
address,symbol,decimals
0x4200000000000000000000000000000000000042,OP,18
0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85,USDC,6
```

If `decimals` is not provided, the puller will attempt to fetch it from the token contract.

### Output Format

The output CSV contains the following columns:

- `wallet`: The wallet address
- `wallet_label`: Optional label for the wallet
- `token`: The token contract address  
- `token_symbol`: Optional symbol for the token
- `balance`: Raw balance as string (preserves precision)
- `decimals`: Number of decimal places for the token
- `balance_formatted`: Human-readable balance (balance / 10^decimals)

Example output:
```csv
wallet,wallet_label,token,token_symbol,balance,decimals,balance_formatted
0x1234567890123456789012345678901234567890,Example Wallet 1,0x4200000000000000000000000000000000000042,OP,1000000000000000000,18,1.0
0x1234567890123456789012345678901234567890,Example Wallet 1,0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85,USDC,1000000,6,1.0
```

### Parameters

- `--wallets-csv`: Path to CSV file containing wallet addresses
- `--tokens-csv`: Path to CSV file containing token addresses  
- `--output-csv`: Path for output CSV file with results
- `--rpc-endpoint`: RPC endpoint URL for blockchain queries
- `--rate-limit-delay`: Delay in seconds between RPC calls (default: 0.1)

### Programmatic Usage

You can also use the module programmatically:

```python
from op_analytics.datasources.walletbalances.main import execute_wallet_balance_pull

results = execute_wallet_balance_pull(
    wallets_csv="wallets.csv",
    tokens_csv="tokens.csv", 
    output_csv="balances.csv",
    rpc_endpoint="https://mainnet.optimism.io/",
    rate_limit_delay=0.1
)
```

## RPC Endpoints

Common RPC endpoints for different networks:

- **Optimism Mainnet**: `https://mainnet.optimism.io/`
- **Base Mainnet**: `https://mainnet.base.org/`
- **Ethereum Mainnet**: `https://ethereum-rpc.publicnode.com/`
- **Arbitrum One**: `https://arb1.arbitrum.io/rpc`

## Rate Limiting

The puller includes rate limiting to prevent overwhelming RPC endpoints:

- Configurable delay between requests (default: 0.1 seconds)
- Automatic retry with exponential backoff on failures
- Error handling for rate limit responses

For public RPC endpoints, consider using a delay of 0.1-0.5 seconds. For private/paid endpoints, you may be able to use shorter delays.

## Error Handling

The puller handles various error conditions:

- Invalid wallet or token addresses
- RPC endpoint failures  
- Rate limiting responses
- Invalid token contracts (non-ERC20)
- Network connectivity issues

Failed queries will log errors but continue processing other combinations. Failed balances will appear as empty values in the output.

## Performance Considerations

- Processing time scales with the number of wallet-token combinations
- For large datasets, consider:
  - Using dedicated RPC endpoints
  - Adjusting rate limiting delays
  - Running queries in batches
  - Using multiple RPC endpoints in parallel (future enhancement)

## Examples

See the `examples/` directory for sample CSV files that demonstrate the expected input formats.