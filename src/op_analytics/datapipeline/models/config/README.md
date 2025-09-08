# Revshare Configuration

This directory contains configuration files for identifying and tracking revenue share transfers across different chains and tokens.

## Configuration Files

### revshare/revshare_to_addresses.yaml

This file defines the destination addresses that receive revenue share transfers. Each entry contains:

```yaml
- to_address: <ethereum_address>  # The destination address
  description: <string>           # Human-readable description of the address
  end_date: <date>               # Optional: Date after which transfers to this address should be excluded
```

Example:
```yaml
- to_address: 0x9c3631dDE5c8316bE5B7554B0CcD2631C15a9A05
  description: OP Receiver
  end_date: null  # No end date, will receive transfers indefinitely

- to_address: 0x391716d440C151C42cdf1C95C1d83A5427Bca52C
  description: OP Receiver
  end_date: '2024-07-04'  # Will only receive transfers until this date
```

### revshare/revshare_from_addresses.yaml

This file defines the source addresses and chains that generate revenue share transfers. Each entry contains:

```yaml
- revshare_from_chain: <string>           # The chain where the fees are generated
  revshare_from_addresses: [<addresses>]  # List of addresses that send revshare
  token_addresses: [<addresses>]          # List of token addresses to track (including 'native' for native token)
  expected_chains: [<chains>]             # List of chains where transfers should be found
```

Example:
```yaml
- revshare_from_chain: optimism
  revshare_from_addresses: 
    - 0x123...
    - 0x456...
  token_addresses:
    - native
    - 0x789...
  expected_chains:
    - base
    - ethereum
```

## Transfer Selection Strategy

The revshare transfer selection process follows these rules:

1. **Source Filtering**:
   - Transfers must come from addresses listed in `revshare_from_addresses`
   - The transfer chain must be in the `expected_chains` list
   - For native transfers: the token must be listed as 'native' in `token_addresses`
   - For token transfers: the token address must be in `token_addresses`

2. **Destination Filtering**:
   - Transfers must go to addresses listed in `revshare_to_addresses`
   - If an address has an `end_date`, only transfers before or on that date are included
   - Addresses without an `end_date` will receive transfers indefinitely

3. **Transfer Types**:
   - Native transfers are sourced from the `native_transfers` table
   - Token transfers are sourced from the `all_transfers` table

4. **Output Fields**:
   - All standard transfer fields (dt, chain, addresses, amounts, etc.)
   - The source chain (`revshare_from_chain`) where the revenue was generated
   - The list of expected chains (`expected_chains`) where transfers should be found
   - The description of the destination address (`to_address_description`) 