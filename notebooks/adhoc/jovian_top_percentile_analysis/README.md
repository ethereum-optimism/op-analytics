# Fjord Top Percentile Analysis

Comprehensive analysis of the top 1% fullest blocks by calldata size for understanding Fjord footprint limits.

## Overview

This project analyzes blockchain blocks to determine optimal Fjord footprint cost configuration by:
- Fetching only the top 1% fullest blocks (by calldata size) from ClickHouse
- Using dynamic gas limits from historical time series data
- Testing multiple footprint costs [160, 400, 600, 800]
- Generating comprehensive visualizations and recommendations

## Key Features

- **Efficient Data Fetching**: SQL-based percentile filtering reduces data by ~99%
- **Dynamic Gas Limits**: Uses actual historical gas limits (30M to 240M+)
- **FastLZ Compression**: Realistic size estimates using Fjord's compression algorithm
- **Comprehensive Analysis**: Tests multiple footprint costs to find optimal configuration
- **Required Visualizations**: All 5 required histograms and distributions

## Project Structure

```
fjord_top_percentile_analysis/
├── CLAUDE.md                       # Project context and documentation
├── README.md                       # This file
├── config.py                        # Configuration parameters
├── core.py                          # Core Fjord analysis engine
├── gas_limits/
│   └── base_average_gas_limit.csv  # Historical gas limits
├── fjord_src/
│   ├── clickhouse_fetcher.py       # Data fetching with gas limits
│   ├── analysis_functions.py       # Fjord analysis functions
│   └── visualization.py            # Histogram generation
├── notebooks/
│   └── top_1_percent_analysis.ipynb # Main analysis notebook
├── data/                            # Cached data (created on run)
└── results/                         # Analysis outputs (created on run)
```

## Installation

1. Ensure you have the required dependencies:
```bash
pip install polars pandas numpy matplotlib seaborn clickhouse-driver
```

2. Set up environment variables for ClickHouse access:
```bash
export GCS_HMAC_ACCESS_KEY="your_key"
export GCS_HMAC_SECRET="your_secret"
```

## Usage

### Quick Start

Run the main analysis notebook:
```bash
cd notebooks
jupyter notebook top_1_percent_analysis.ipynb
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

## Results

The analysis provides:
- Percentage of blocks exceeding limits for each calldata footprint gas scalar
- Average and maximum utilization metrics
- Distribution of excess amounts
- Recommendation for optimal configuration (constant vs configurable)

## Data Sources

- **Transaction Data**: ClickHouse via GCS parquet files
- **Gas Limits**: Historical CSV with daily gas limits
- **Chains**: Base (primary), extensible to other chains

## Performance

- **Data Reduction**: ~99% by fetching only top 1% blocks
- **Caching**: Local parquet files for repeated analysis
- **Parallel Processing**: Multi-threaded data fetching and analysis

## Testing

Test individual modules:
```bash
# Test data fetching
python fjord_src/clickhouse_fetcher.py

# Test analysis functions
python jovian_src/analysis_functions.py

# Test visualization
python jovian_src/visualization_jovian.py
```

## Contributing

See CLAUDE.md for detailed project context and development guidelines.

## License

Part of the op-analytics project.
