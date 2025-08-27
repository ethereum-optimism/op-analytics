# Jovian Calldata Analysis

Comprehensive analysis of Superchain blocks to determine optimal Jovian calldata footprint gas scalar configuration by analyzing the fullest blocks (top 1%), random sampling, and testing multiple footprint costs.

## Overview

This project analyzes blockchain blocks to determine optimal Jovian calldata footprint gas scalar configuration by:
- Fetching the top 1% fullest blocks (by calldata size) from ClickHouse or random sampling (1%) from the block history
- Using dynamic gas limits from historical time series data
- Testing multiple calldata footprint gas scalars [160, 400, 600, 800]
- Generating comprehensive visualizations and recommendations
- Supporting multiple chains: Base, OP Mainnet etc.

## Key Features

- **Dynamic Gas Limits**: Uses actual historical gas limits (30M to 240M+)
- **FastLZ Compression**: Realistic size estimates using Jovian's compression algorithm
- **Comprehensive Analysis**: Tests multiple calldata footprint gas scalars to find optimal configuration
- **Required Visualizations**: All 5 required histograms and distributions
- **Multi-Chain Support**: Analyze Base, OP Mainnet etc.
- **Dual Sampling Methods**: Top percentile analysis and random sampling

## Project Structure

```
jovian_analysis/
├── README.md                       # This file
├── jovian_src/                     # All Python source code
│   ├── core.py                     # Core Jovian analysis engine
│   ├── config.py                   # Configuration parameters
│   ├── analysis_functions.py       # Jovian analysis functions
│   ├── visualization_jovian.py     # Histogram generation
│   ├── clickhouse_fetcher.py       # Data fetching with gas limits
│   └── chain_config.py             # Chain-specific configurations
├── notebooks/                       # Jupyter notebooks
│   ├── op_jovian_analysis_final_random.ipynb
│   ├── op_jovian_analysis_final_top_percentile.ipynb
│   ├── base_jovian_analysis_final_random.ipynb
│   ├── base_jovian_analysis_final_top_percentile.ipynb
│   ├── quick_tests.ipynb
│   └── .cache/                      # Cached block data
│       ├── op/
│       │   ├── top_percentile/
│       │   └── random_sample/
│       └── base/
│           ├── top_percentile/
│           └── random_sample/
├── gas_limits/                      # Historical gas limits data
│   ├── op_gas_limits.csv
│   └── base_gas_limits.csv
└── results/                         # Analysis outputs (created on run)
    ├── op/
    └── base/
```

## Installation

Ensure you have the required dependencies by following the instructions in the [OP Labs Data Platform](https://static.optimism.io/op-analytics/sphinx/html/index.html).


## Usage

### Quick Start

Run the main analysis notebook:
```bash
cd notebooks
jupyter notebook op_jovian_analysis_final_top_percentile.ipynb
```

### Configuration

Edit the configuration section in the notebook:

```python
# Chain selection
CHAIN = "base"  # Options: base, optimism, mode, zora, world, ink, soneium

# Sampling method
SAMPLING_METHOD = "top_percentile"  # or "random"
PERCENTILE = 99.0    # For top percentile (99 = top 1%)
NUM_BLOCKS = 100     # For random sampling

# Date range
START_DATE = "2024-08-01"
END_DATE = "2024-08-07"
```

### Programmatic Usage

```python
from jovian_src.clickhouse_fetcher import fetch_top_percentile_blocks, load_gas_limits
from jovian_src.analysis_functions import perform_jovian_analysis
from jovian_src.visualization_jovian import generate_all_visualizations

# Load gas limits
gas_limits = load_gas_limits()

# Fetch data
df, gas_limit = fetch_top_percentile_blocks(
    chain="base",
    date="2024-08-01",
    percentile=99.0,
    gas_limit=120_000_000
)

# Analyze with multiple calldata footprint gas scalars
analysis_results = perform_jovian_analysis(
    df=df,
    gas_limit=gas_limit,
    calldata_footprint_gas_scalars=[160, 400, 600, 800]
)

# Generate visualizations
figures = generate_all_visualizations(
    results_by_scalar=analysis_results,
    output_dir="results/histograms"
)
```

## Analysis Goals

1. **Histogram of total size estimates** - Distribution of block sizes
2. **Blocks exceeding limits** - Analysis of when/how blocks exceed
3. **Excess distribution** - Distribution for blocks over limit
4. **Over-utilization quantification** - Percentage metrics (e.g., 50/10000 blocks)
5. **Over-limit distribution** - How much blocks exceed by

## Key Calculations

### Jovian Footprint
- **Size Estimate**: `max(100, -42585600 + 836500 * fastlz_size / 1e6)`
- **Footprint**: `size_estimate * calldata_footprint_gas_scalar`
- **Block Footprint**: `sum(tx_footprints)`
- **Utilization**: `block_footprint / gas_limit`

### Calldata Footprint Gas Scalars Tested
- **160**: 4x multiplier (aggressive)
- **400**: 10x multiplier (balanced)
- **600**: 15x multiplier (conservative)
- **800**: 20x multiplier (very conservative)

## Gas Limits

Gas limits are loaded from CSV files in `gas_limits/`:
- `base_gas_limits.csv`
- `op_gas_limits.csv`
- etc.

If no gas limit file exists for a chain, defaults to 240,000,000.

## Output

Results are saved to: `results/{chain}/{start_date}_to_{end_date}/`

Generated files:
- Size estimates histogram
- Blocks exceeding analysis
- Compression analysis
- Trend analysis
- Scalar comparison

## Results

The analysis provides:
- Percentage of blocks exceeding limits for each calldata footprint gas scalar
- Average and maximum utilization metrics
- Distribution of excess amounts
- Recommendation for optimal configuration (constant vs configurable)

## Data Sources

- **Transaction Data**: ClickHouse via GCS parquet files
- **Gas Limits**: Historical CSV with daily gas limits
- **Chains**: Base, OP Mainnet etc.

## Performance

- **Caching**: Local parquet files for repeated analysis
- **Parallel Processing**: Multi-threaded data fetching and analysis

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

## Contributing

This project is part of the op-analytics repository. Please follow the project's contribution guidelines.

## License

Part of the op-analytics project.
