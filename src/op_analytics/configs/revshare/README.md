# Configuration Files

This directory contains YAML configuration files used by the op-analytics data pipeline.

## Revshare Transfers Configuration

Note: Revshare YAMLs are now organized under `src/op_analytics/configs/revshare/`.

### revshare/revshare_from_addresses.yaml

This file defines the "from" addresses for revshare transfers - addresses that send funds to OP receivers.

#### Field Descriptions

- **`addresses`** (array of strings): List of addresses that send revshare
  - Example: `["0x1234...", "0x5678..."]`
  - Addresses are normalized to lowercase in the database

- **`tokens`** (array of strings): Token types this address can send
  - `"native"` for ETH transfers
  - Contract addresses for ERC20 token transfers
  - Example: `["native"]` or `["0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85"]`

- **`expected_chains`** (array of strings): Chain(s) where this address sends revshare
  - Example: `["ethereum"]`, `["base"]`, `["ethereum", "base"]`
  - Used for filtering transfers by chain

- **`end_date`** (string or null): Optional cutoff date for this address
  - Format: `"YYYY-MM-DD"` (e.g., `"2024-12-31"`)
  - `null` means no date filtering (include all transfers)
  - Transfers after this date will be excluded

- **`chain_id`** (integer): The chain ID for the chain sending revshare
  - Example:  `130` for Unichain, `8453` for Base
  - Used for joining back with other datasets

#### Example Configuration
```yaml
base:
  addresses:
    - "0x09C7bAD99688a55a2e83644BFAed09e62bDcCcBA"
    - "0x45969D00739d518f0Dde41920B67cE30395135A0"
  tokens:
    - "native"
  expected_chains:
    - "base"
  end_date: null
  chain_id: 8453
```

### revshare/revshare_to_addresses.yaml

This file defines the "to" addresses for revshare transfers - OP receiver addresses that receive funds.

#### Field Descriptions

- **`description`** (string): Human-readable description of this receiver
  - Example: `"OP Receiver - Base"`, `"OP Receiver"`
  - Used for analytics and reporting

- **`end_date`** (string or null): Optional cutoff date for this address
  - Format: `"YYYY-MM-DD"` (e.g., `"2024-12-31"`)
  - `null` means no date filtering (include all transfers)
  - Transfers after this date will be excluded

- **`expected_chains`** (array of strings): Chains where we look for this address to receive revshare
  - Example: `["ethereum"]`, `["base", "ethereum"]`
  - Special value `"all"` can be used to match transfers from any chain
  - Example: `["all"]` matches all chains
  - Used for filtering transfers to the chains where they are expected.

#### Example Configuration
```yaml
"0x9c3631dDE5c8316bE5B7554B0CcD2631C15a9A05":
  description: "OP Receiver - Base"
  end_date: null
  expected_chains:
    - "base"
    - "ethereum"

"0x16A27462B4D61BDD72CbBabd3E43e11791F7A28c":
  description: "OP Receiver - All Chains"
  expected_chains:
    - "all"
```

## Usage

### Adding New Addresses

1. **Edit the appropriate YAML file** in this directory
2. **Add the new address** with required fields
3. **Run the Dagster job** - the address will be automatically loaded into ClickHouse

### Modifying Existing Addresses

1. **Edit the YAML file** to update fields
2. **Run the Dagster job** - changes will be reflected in the database

### Date Filtering

- Set `end_date` to a specific date to stop including transfers after that date
- Set `end_date: null` to include all transfers (no filtering)
- Useful for deprecating old addresses or changing receiver addresses

## Validation

The configuration files are validated by tests that check for:
- Duplicate addresses (case-insensitive)
- Duplicate tokens within each address configuration (case-insensitive)
- Valid YAML syntax
- Required fields present

Run tests with:
```bash
python -m pytest tests/op_analytics/datapipeline/etl/models/test_revshare_transfers_config.py
```

## Notes

- All addresses are normalized to lowercase in the database
- Changes to these files are version controlled in git
- The system automatically loads these configs into ClickHouse tables
- No SQL modifications are needed when updating addresses
