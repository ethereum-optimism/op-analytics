# Wallet Balance Puller - Usage Example

This document shows the actual command used to pull wallet balances with Safe API integration.

## Command Used

```bash
uv run opdata walletbalances pull_balances \
    --wallets-csv src/op_analytics/datasources/walletbalances/inputs/wallets.csv \
    --tokens-csv src/op_analytics/datasources/walletbalances/inputs/tokens.csv \
    --output-csv src/op_analytics/datasources/walletbalances/outputs/wallet_token_balances.csv \
    --rpc-endpoint https://mainnet.optimism.io/ \
    --rate-limit-delay 0.2 \
    --safe-api-url https://safe-transaction-optimism.safe.global/api/v1/owners/0xE4553b743E74dA3424Ac51f8C1E586fd43aE226F/safes/
```

## What This Command Does

1. **Reads wallet addresses** from `inputs/wallets.csv` (30 addresses with labels like "OP", "OPx")
2. **Fetches additional addresses** from the Safe API endpoint (436+ locked multisig addresses)
3. **Combines and deduplicates** all addresses (total: 466 unique wallets)
4. **Reads token addresses** from `inputs/tokens.csv` (OP and OPx tokens)
5. **Queries balances** for all wallet-token combinations (932 total queries: 466 wallets × 2 tokens)
6. **Applies rate limiting** (0.2 seconds between queries to respect RPC limits)
7. **Saves results** to `outputs/wallet_token_balances.csv`

## Results Summary

- **Input Sources**: CSV file + Safe API
- **Total Unique Wallets**: 466
- **Tokens Queried**: 2 (OP, OPx)
- **Total Combinations**: 932
- **Labels Applied**: 
  - CSV addresses keep their original labels ("OP", "OPx", etc.)
  - Safe API addresses get "locked multisig" label
- **Deduplication**: Automatic (CSV labels take priority over API labels)

## Output Fields

The resulting CSV contains:
- `wallet`: Wallet address
- `wallet_label`: Label ("OP", "OPx", "locked multisig", etc.)
- `token`: Token contract address  
- `token_symbol`: Token symbol ("OP", "OPx")
- `balance`: Raw balance (preserves full precision)
- `decimals`: Token decimal places
- `balance_formatted`: Human-readable balance

## Safe API Integration

The Safe API URL fetches all Safe multisig wallets owned by the specified address:
`https://safe-transaction-optimism.safe.global/api/v1/owners/0xE4553b743E74dA3424Ac51f8C1E586fd43aE226F/safes/`

This automatically includes hundreds of governance-related multisig wallets without manual entry.

## Performance Notes

- **Processing Time**: ~30 minutes for 932 combinations with 0.2s rate limiting
- **RPC Calls**: 932 balance queries + token metadata calls
- **Error Handling**: Enhanced retry logic with timeouts and rate limiting
  - **5 retry attempts** for balance queries with 2s initial wait + jitter
  - **60-second timeouts** (increased from 30s)
  - **Automatic retries** on timeouts, rate limits, and connection errors
  - **Exponential backoff** with jitter to avoid thundering herd
- **Memory Efficient**: Processes combinations iteratively

## Alternative Usage (CSV Only)

To run without Safe API integration:

```bash
uv run opdata walletbalances pull_balances \
    --wallets-csv src/op_analytics/datasources/walletbalances/inputs/wallets.csv \
    --tokens-csv src/op_analytics/datasources/walletbalances/inputs/tokens.csv \
    --output-csv src/op_analytics/datasources/walletbalances/outputs/wallet_token_balances.csv \
    --rpc-endpoint https://mainnet.optimism.io/ \
    --rate-limit-delay 0.2
```

(Simply omit the `--safe-api-url` parameter)