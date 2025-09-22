# Jovian Calldata Analysis

Comprehensive analysis of Superchain blocks to determine optimal Jovian calldata footprint gas scalar configuration by analyzing the fullest blocks (top 1%), random sampling, and testing multiple footprint costs.

## Overview

This project analyzes blockchain blocks to determine optimal Jovian calldata footprint gas scalar configuration by:
- Fetching the top 1% fullest blocks (by calldata size) from ClickHouse or random sampling (1%) from the block history
- Using dynamic gas limits from historical time series data
- Testing multiple calldata footprint gas scalars [160, 400, 600, 800]
- Generating comprehensive visualizations and recommendations
- Supporting multiple chains: Base, OP Mainnet, Mode, Ink, Soneium, Unichain, Worldchain, etc.

## Key Features

- **Per-Block Gas Limits**: Uses actual gas limits from `blocks_v1` data for each block
- **FastLZ Compression**: Realistic DA usage estimates using Jovian's compression algorithm
- **Multi-Chain Support**: Analyze Base, OP Mainnet, Mode, Ink, Soneium, Unichain, Worldchain, etc.
- **Dual Sampling Methods**: Top percentile analysis and random sampling
- **Time-Series Analysis**: Analyze DA throttling events with moving averages and throttling detection
- **Deposit Transaction Handling**: Excludes DA usage for deposits in block totals

## Project Structure

```
jovian_analysis/
├── README.md                       # This file
├── jovian_src/                     # All Python source code
│   ├── core.py                     # Core Jovian analysis engine
│   ├── config.py                   # Configuration parameters
│   ├── analysis_functions.py       # Jovian analysis functions
│   ├── visualization_jovian.py     # Visualization generation
│   ├── clickhouse_fetcher.py       # Data fetching with gas limits
│   ├── chain_config.py             # Chain-specific configurations
│   └── constants.py                # Constants and hardcoded values
├── notebooks/                       # Jupyter notebooks
│   ├── jovian_analysis.ipynb       # 🆕 Consolidated analysis notebook
│   ├── da_throttling_analysis.ipynb # 🆕 DA throttling analysis notebook
│   ├── saved_output_html/           # 📁 Saved HTML outputs from analysis
│   └── archive/                     # 📁 Archived old notebooks
└── results/                         # Analysis outputs (created on run)

```

## Installation

Ensure you have the required dependencies by following the instructions in the [OP Labs Data Platform](https://static.optimism.io/op-analytics/sphinx/html/index.html).

## Usage

### Quick Start

Run the consolidated analysis notebook:
```bash
cd notebooks
jupyter notebook jovian_analysis.ipynb
```

### Configuration

Edit the configuration section in the notebook:

```python
# Chain selection
CHAIN = "base"  # Options: base, op, mode, ink, soneium, unichain, worldchain

# Sampling method
SAMPLING_METHOD = "top_percentile"  # "top_percentile" or "random"
PERCENTILE = 99.0    # For top percentile (99 = top 1%)
SAMPLE_FRACTION = 0.01  # For random sampling (1% = 0.01)

# Date range
START_DATE = "2025-03-01"
END_DATE = "2025-06-30"

# Analysis parameters
FORCE_REFRESH = False  # Set True to ignore cache and re-download
SAVE_RESULTS = True    # Save results to files
SHOW_PLOTS = True      # Display plots inline
```

### Programmatic Usage

```python
from jovian_src.clickhouse_fetcher import fetch_top_percentile_blocks
from jovian_src.analysis_functions import perform_jovian_analysis
from jovian_src.visualization_jovian import generate_all_visualizations

# Fetch data with per-block gas limits
df = fetch_top_percentile_blocks(
    chain="base",
    date="2025-03-01",
    percentile=99.0
)

# Analyze with multiple calldata footprint gas scalars
# Gas limits are handled per-block automatically
analysis_results = perform_jovian_analysis(
    df=df,
    da_footprint_gas_scalars=[160, 400, 600, 800],
    chain="base",
    sampling_method="top_percentile",
    start_date="2025-03-01",
    end_date="2025-03-01"
)

# Generate visualizations
figures = generate_all_visualizations(
    results_by_scalar=analysis_results,
    output_dir="results/histograms"
)
```

## Analysis Goals

1. **Histogram of total DA usage estimates** - Distribution of block sizes
2. **Blocks exceeding limits** - Analysis of when/how blocks exceed
3. **Excess distribution** - Distribution for blocks over limit
4. **Over-utilization quantification** - Percentage metrics (e.g., 50/10000 blocks)
5. **Over-limit distribution** - How much blocks exceed by

## Key Calculations

### Jovian Footprint
- **DA Usage Estimate**: `max(100, -42585600 + 836500 * fastlz_size / 1e6)`
- **Footprint**: `da_usage_estimate * calldata_footprint_gas_scalar`
- **Block Footprint**: `sum(tx_footprints)` (excluding deposit transactions)
- **Utilization**: `block_footprint / gas_limit` (per-block gas limit)

### Deposit Transaction Handling
- **Deposit Detection**: Transactions with `transaction_type == 126` are identified as deposits
- **DA Usage Calculation**: Deposit transactions still have their DA usage calculated
- **Block Totals**: Deposit transactions are excluded from block-level totals (footprint, DA usage, etc.)
- **Individual Analysis**: Deposit transactions are included in individual transaction analysis

### Calldata Footprint Gas Scalars Tested
- **160**: 4x multiplier (aggressive)
- **400**: 10x multiplier (balanced)
- **600**: 15x multiplier (conservative)
- **800**: 20x multiplier (very conservative)

## Gas Limits

Gas limits are now fetched directly from `blocks_v1` data in ClickHouse:
- **Per-Block Gas Limits**: Each block uses its actual gas limit from the `blocks_v1` table
- **Automatic Handling**: No manual gas limit configuration required
- **Fallback**: If gas limit data is not available, uses default from config (30,000,000)
- **Multi-Chain Support**: Works with all supported chains automatically

### Gas Limit Data Flow

1. **Data Fetching**: `clickhouse_fetcher.py` joins with `blocks_v1` table to get per-block gas limits
2. **Analysis**: `core.py` uses each block's actual gas limit for utilization calculations
3. **Aggregation**: `analysis_functions.py` calculates most common gas limit for summary statistics
4. **Results**: Both per-block and aggregate gas limit data are preserved in results

## Output

Results are saved to: `results/{chain}/jovian_analysis_{method}_{start_date}_{end_date}/`

Generated files:
- DA usage estimates histogram
- Blocks exceeding analysis
- Compression analysis
- Trend analysis
- Scalar comparison

HTML outputs are also saved to `notebooks/saved_output_html/` for easy sharing and viewing.

## Results

The analysis provides:
- Percentage of blocks exceeding limits for each calldata footprint gas scalar
- Average and maximum utilization metrics
- Distribution of excess amounts
- Recommendation for optimal configuration (constant vs configurable)

## Data Sources

- **Transaction Data**: ClickHouse via GCS parquet files
- **Gas Limits**: Per-block gas limits from `blocks_v1` table in ClickHouse
- **Chains**: Base, OP Mainnet, Mode, Ink, Soneium, Unichain, Worldchain


## Testing

Test individual modules:
```bash
# Test data fetching
python jovian_src/clickhouse_fetcher.py

# Test analysis functions
python jovian_src/analysis_functions.py

# Test visualization
python jovian_src/visualization_jovian.py

# Test core functionality
python jovian_src/core.py
```

## DA Throttling Analysis

The `da_throttling_analysis.ipynb` notebook provides:
- Time-series analysis of DA throttling events
- Moving averages and trend detection
- Throttling period identification
- Performance metrics over time

### Configuration

Edit the `THROTTLING_PERIODS` and `THROTTLING_THRESHOLDS` sections to:
- Add new analysis periods
- Adjust sampling strategies
- Modify throttling detection thresholds
- Change moving average window sizes

## Contributing

This project is part of the op-analytics repository. Please follow the project's contribution guidelines.

## License

Part of the op-analytics project.
