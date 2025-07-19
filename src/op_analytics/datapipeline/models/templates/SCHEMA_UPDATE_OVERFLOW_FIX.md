# ClickHouse Schema Updates for INT64 Overflow Fix

## Overview

This document contains the SQL commands needed to update existing ClickHouse tables to support the new decimal data types that prevent INT64 overflow when processing high gas price transactions (e.g., Ethereum L1 data during congestion).

## What Changed

The overflow fix involved changing several columns from `BIGINT` and `DOUBLE` to `DECIMAL` types to safely handle large arithmetic operations:

- **Gas fee calculations**: `BIGINT` → `DECIMAL(38,0)` 
- **Complex multiplications**: `DOUBLE` → `DECIMAL(38,20)`
- **Size calculations**: `DECIMAL(38,12)` → `DECIMAL(38,6)` (improved precision)
- **Unified gas calculations**: `BIGINT` → `DECIMAL(38,0)`

## Required Schema Updates

### 1. Update `refined_transactions_fees_v2` Table

```sql
-- Update fee calculation columns to DECIMAL(38,0)
ALTER TABLE refined_transactions_fees_v2 
  MODIFY COLUMN legacy_extra_fee_per_gas DECIMAL(38,0),
  MODIFY COLUMN l2_fee DECIMAL(38,0),
  MODIFY COLUMN l2_priority_fee DECIMAL(38,0),
  MODIFY COLUMN l2_base_fee DECIMAL(38,0),
  MODIFY COLUMN tx_fee DECIMAL(38,0),
  MODIFY COLUMN l2_legacy_extra_fee DECIMAL(38,0),
  MODIFY COLUMN l1_gas_used_unified DECIMAL(38,0);

-- Update L1 fee calculation columns to DECIMAL(38,6) for better precision
ALTER TABLE refined_transactions_fees_v2
  MODIFY COLUMN l1_base_fee DECIMAL(38,6),
  MODIFY COLUMN l1_base_scaled_size DECIMAL(38,6),
  MODIFY COLUMN l1_blob_fee DECIMAL(38,6),
  MODIFY COLUMN l1_blob_scaled_size DECIMAL(38,6);
```

### 2. Update `refined_traces_fees_v2` Table

```sql
-- Update trace fee calculation columns to DECIMAL(38,20) for high precision
ALTER TABLE refined_traces_fees_v2
  MODIFY COLUMN tx_l2_fee_native_minus_subtraces DECIMAL(38,20),
  MODIFY COLUMN tx_l2_base_fee_native_minus_subtraces DECIMAL(38,20),
  MODIFY COLUMN tx_l2_priority_fee_native_minus_subtraces DECIMAL(38,20),
  MODIFY COLUMN tx_l2_legacy_base_fee_native_minus_subtraces DECIMAL(38,20);
```

## Execution Instructions

### Option 1: Execute All at Once
```sql
-- Execute both ALTER statements in sequence
ALTER TABLE refined_transactions_fees_v2 
  MODIFY COLUMN legacy_extra_fee_per_gas DECIMAL(38,0),
  MODIFY COLUMN l2_fee DECIMAL(38,0),
  MODIFY COLUMN l2_priority_fee DECIMAL(38,0),
  MODIFY COLUMN l2_base_fee DECIMAL(38,0),
  MODIFY COLUMN tx_fee DECIMAL(38,0),
  MODIFY COLUMN l2_legacy_extra_fee DECIMAL(38,0),
  MODIFY COLUMN l1_gas_used_unified DECIMAL(38,0),
  MODIFY COLUMN l1_base_fee DECIMAL(38,6),
  MODIFY COLUMN l1_base_scaled_size DECIMAL(38,6),
  MODIFY COLUMN l1_blob_fee DECIMAL(38,6),
  MODIFY COLUMN l1_blob_scaled_size DECIMAL(38,6);

ALTER TABLE refined_traces_fees_v2
  MODIFY COLUMN tx_l2_fee_native_minus_subtraces DECIMAL(38,20),
  MODIFY COLUMN tx_l2_base_fee_native_minus_subtraces DECIMAL(38,20),
  MODIFY COLUMN tx_l2_priority_fee_native_minus_subtraces DECIMAL(38,20),
  MODIFY COLUMN tx_l2_legacy_base_fee_native_minus_subtraces DECIMAL(38,20);
```

### Option 2: Execute Incrementally (Safer for Large Tables)
```sql
-- Execute one column at a time if tables are very large
ALTER TABLE refined_transactions_fees_v2 MODIFY COLUMN legacy_extra_fee_per_gas DECIMAL(38,0);
ALTER TABLE refined_transactions_fees_v2 MODIFY COLUMN l2_fee DECIMAL(38,0);
ALTER TABLE refined_transactions_fees_v2 MODIFY COLUMN l2_priority_fee DECIMAL(38,0);
-- ... continue for each column
```

## Verification Queries

After running the schema updates, verify the changes were applied correctly:

```sql
-- Check refined_transactions_fees_v2 schema
DESCRIBE refined_transactions_fees_v2;

-- Check refined_traces_fees_v2 schema  
DESCRIBE refined_traces_fees_v2;

-- Verify specific columns
SELECT 
  name,
  type
FROM system.columns 
WHERE table = 'refined_transactions_fees_v2' 
  AND name IN (
    'legacy_extra_fee_per_gas',
    'l2_fee', 
    'l2_priority_fee',
    'l2_base_fee',
    'tx_fee',
    'l2_legacy_extra_fee',
    'l1_gas_used_unified',
    'l1_base_fee',
    'l1_base_scaled_size',
    'l1_blob_fee',
    'l1_blob_scaled_size'
  );
```

## Impact and Considerations

### Benefits
- **Prevents INT64 Overflow**: Can now handle Ethereum L1 gas prices during high congestion
- **Future-Proof**: Supports even higher values as networks evolve
- **Maintains Precision**: DECIMAL types preserve exact values for financial calculations

### Potential Impacts
- **Storage**: DECIMAL columns may use slightly more storage than BIGINT
- **Performance**: Minimal impact on query performance 
- **Compatibility**: Existing queries should work without modification
- **Memory**: DECIMAL operations may use slightly more memory

### Testing Recommendations
1. Test the schema changes on a staging environment first
2. Verify that existing queries still return expected results
3. Run performance tests on critical queries
4. Check that data pipeline ingestion continues to work correctly

## Rollback Plan

If you need to rollback the changes (not recommended after new data is ingested):

```sql
-- WARNING: This may cause data loss if values exceed BIGINT limits
ALTER TABLE refined_transactions_fees_v2 
  MODIFY COLUMN legacy_extra_fee_per_gas BIGINT,
  MODIFY COLUMN l2_fee BIGINT,
  MODIFY COLUMN l2_priority_fee BIGINT,
  MODIFY COLUMN l2_base_fee BIGINT,
  MODIFY COLUMN tx_fee BIGINT,
  MODIFY COLUMN l2_legacy_extra_fee BIGINT,
  MODIFY COLUMN l1_gas_used_unified BIGINT;

-- Note: L1 fee columns were already DECIMAL, changing precision back
ALTER TABLE refined_transactions_fees_v2
  MODIFY COLUMN l1_base_fee DECIMAL(38,12),
  MODIFY COLUMN l1_base_scaled_size DECIMAL(38,12),
  MODIFY COLUMN l1_blob_fee DECIMAL(38,12),
  MODIFY COLUMN l1_blob_scaled_size DECIMAL(38,12);

ALTER TABLE refined_traces_fees_v2
  MODIFY COLUMN tx_l2_fee_native_minus_subtraces DOUBLE,
  MODIFY COLUMN tx_l2_base_fee_native_minus_subtraces DOUBLE,
  MODIFY COLUMN tx_l2_priority_fee_native_minus_subtraces DOUBLE,
  MODIFY COLUMN tx_l2_legacy_base_fee_native_minus_subtraces DOUBLE;
```

## Related Files

The following template files were updated with the overflow prevention logic:
- `refined_transactions_fees.sql.j2` - Core transaction fee calculations
- `refined_traces/traces_txs_join.sql.j2` - Trace-transaction fee joins
- `compute/udfs.py` - Added helper functions: `dec38()`, `dec38_6()`, `safe_mul()`, `safe_mul_scalar()`

## Contact

If you encounter any issues with these schema updates, check:
1. ClickHouse logs for any error messages
2. Data pipeline monitoring for ingestion issues  
3. Test queries to verify data integrity

The changes are designed to be backward compatible while preventing the INT64 overflow that was occurring with high Ethereum L1 gas prices. 