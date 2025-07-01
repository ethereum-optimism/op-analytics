# Revshare Transfers Configuration

This directory contains the ClickHouse DDL files for the `revshare_transfers_v1` table.

## Overview

The revshare transfers system processes both native transfers (ETH) and ERC20 token transfers that match specific from/to address patterns. The system uses YAML configuration files for address management, which are loaded into ClickHouse tables before the main SQL execution.

## Architecture

### 1. Configuration Files
- **From addresses**: `src/op_analytics/configs/revshare_from_addresses.yaml`
- **To addresses**: `src/op_analytics/configs/revshare_to_addresses.yaml`

### 2. Dagster Assets
The system uses three Dagster assets that run in sequence:

1. **`revshare_from_addresses`** - Loads from_addresses YAML into ClickHouse table
2. **`revshare_to_addresses`** - Loads to_addresses YAML into ClickHouse table  
3. **`revshare_transfers`** - Main ETL job (depends on both address tables)

### 3. ClickHouse Tables
- `revshare_from_addresses` - Contains from address configurations
- `revshare_to_addresses` - Contains to address configurations
- `revshare_transfers_v1` - Main transfers table

## How It Works

### Step 1: Configuration Management
Edit the YAML files in `src/op_analytics/configs/` to add/remove addresses or modify configurations.

### Step 2: Dagster Execution
When you run the Dagster job:
1. **Address loaders run first** - YAML configs are loaded into ClickHouse tables
2. **Main SQL executes** - References the loaded tables instead of hardcoded CTEs

### Step 3: SQL Processing
The SQL joins against the address tables to:
- Filter transfers by from/to addresses
- Apply date filtering (end_date logic)
- Handle both native and ERC20 transfers
- Combine results with UNION ALL

## File Structure

- `revshare_transfers_v1__CREATE.sql` - Table creation DDL
- `revshare_transfers_v1__INSERT.sql` - Main ETL SQL (references ClickHouse tables)
- `scripts/generate_revshare_sql.py` - Legacy script (no longer used)

## Benefits of This Architecture

✅ **Clean separation**: YAML configs separate from SQL logic  
✅ **Easy maintenance**: Add/remove addresses by editing YAML files  
✅ **Proper dependencies**: Dagster ensures correct execution order  
✅ **No embedded data**: SQL file is clean and readable  
✅ **Version control**: Address changes are tracked in YAML files  

## Usage

1. **Edit YAML files** to modify address configurations
2. **Run Dagster job** - dependencies ensure proper ordering
3. **No SQL regeneration needed** - config tables are updated automatically

## Configuration Format

### From Addresses (revshare_from_addresses.yaml)
```yaml
chain_name:
  addresses:
    - "0x1234..."
    - "0x5678..."
  tokens:
    - "native"  # or contract addresses for ERC20
  expected_chains:
    - "ethereum"
  end_date: "2024-12-31"  # or null
  chain_id: 1 # ID of the chain contributing RevShare
```

### To Addresses (revshare_to_addresses.yaml)
```yaml
"0x1234...":
  description: "Description"
  end_date: "2024-12-31"  # or null
  expected_chains:
    - "ethereum"
    - "base"
```

## Testing

Run the duplicate address test:
```bash
python -m pytest tests/op_analytics/datapipeline/etl/models/test_revshare_transfers_config.py::TestRevshareTransfersConfig::test_no_duplicate_addresses
```

## Notes

- Address changes require no SQL modifications - just update YAMLs
- The system processes both native transfers (ETH) and ERC20 token transfers
- All addresses are normalized to lowercase for consistent matching
- Date filtering uses `end_date` fields (null = no filtering) 