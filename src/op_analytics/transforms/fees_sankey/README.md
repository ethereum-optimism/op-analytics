# Fees Sankey Transform

## Overview

This transform generates datasets for Sankey diagram visualization of Superchain fee flow breakdowns.

## Purpose

The Sankey diagram shows how fees flow through the Superchain ecosystem with hierarchical breakdowns:

**Level 1 (Primary Categories - sum to 100%):**
- Chain fees (L2 execution fees)
- MEV operator fees 
- Stablecoin issuer revenue
- App-specific fees

**Level 2 (Sub-component breakdowns):**
- Chain fees → Revenue share to Optimism, Gas costs, Remaining
- MEV fees → Revenue share to Optimism, Remaining  
- Stablecoin fees → Revenue share to Optimism, Remaining
- App fees → Revenue to App, Revenue share to Optimism, Remaining

**Revenue Field Definitions:**
- `revshare_*` fields = Revenue to Optimism (the Collective)
- `*_revenue_*` fields = Revenue to the App/Protocol

## Source Data

- **Input**: `oplabs-tools-data.materialized_tables.daily_superchain_health_mv`
- **Output**: BigQuery test table and ClickHouse analytics table

## Schema

| Column | Type | Description |
|--------|------|-------------|
| chain_set | String | Chain display name |
| source | String | Source node in flow |
| destination | String | Destination node in flow |
| value | Float | Fee amount in USD |
| pct_of_total_fees_usd | Float | Percentage of total fees (Level 1 only) |

## Usage

### Command Line

```bash
# Run with latest data (90 days lookback by default)
uv run opdata transforms fees-sankey

# Dry run to validate without writing
uv run opdata transforms fees-sankey --dry-run

# Custom lookback period
uv run opdata transforms fees-sankey --days 30
```

### Programmatic Usage

```python
from op_analytics.transforms.fees_sankey.generate_sankey_fees_dataset import execute_pull

# Execute with options
result = execute_pull(days=90, dry_run=False)
print(f"Processed {result['chains_processed']} chains, generated {result['edges_generated']} edges")
```

## Implementation Notes

- Uses existing op-analytics utilities for BigQuery and ClickHouse writes
- Manual execution script (not integrated with Dagster)
- Writes to test datasets for safety
- Comprehensive validation ensures Level 1 percentages sum to 100%
- Returns execution summary following op-analytics patterns

## Prototyping

Use the notebook at `notebooks/adhoc/clickhouse_transforms/fees_sankey_prototype.ipynb` for:
- Testing changes to fee flow logic
- Validating with different date ranges
- Prototyping new fee categories
- Exporting data for Sankey visualization testing 