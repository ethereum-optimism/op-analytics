# Revshare Transfers Configuration

This directory contains the ClickHouse DDL files for the `revshare_transfers_v1` table.

## Configuration Management

The address lists are managed through YAML configuration files for easy editing:

- **From addresses**: `src/op_analytics/datapipeline/models/config/revshare_from_addresses.yaml`
- **To addresses**: `src/op_analytics/datapipeline/models/config/revshare_to_addresses.yaml`

## How to Modify Addresses

1. **Edit the YAML files** in `src/op_analytics/datapipeline/models/config/`
2. **Run the generation script** to update the SQL:
   ```bash
   python scripts/generate_revshare_sql.py
   ```

This approach allows you to:
- Keep address lists in a readable YAML format
- Version control the configuration separately from the SQL
- Generate the SQL file automatically to avoid manual errors

## File Structure

- `revshare_transfers_v1__CREATE.sql` - Table creation DDL
- `revshare_transfers_v1__INSERT.sql` - Generated INSERT SQL (do not edit manually)
- `scripts/generate_revshare_sql.py` - Script to generate SQL from YAML configs
- `README.md` - This documentation

## Configuration Format

### From Addresses (revshare_from_addresses.yaml)
```yaml
chain_name:
  addresses:
    - "0x1234..."
    - "0x5678..."
  tokens:
    - "native"
  expected_chains:
    - "ethereum"
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

## Notes

- The SQL file is generated automatically - **do not edit it manually**
- Changes to addresses require running the generation script
- The model processes both native transfers (ETH) and ERC20 token transfers
- All addresses in the `revshare_from_addresses` arrays are checked using `hasAny()` 