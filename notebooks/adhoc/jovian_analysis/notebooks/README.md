# Jovian Analysis Notebook

## `jovian_analysis_lean.ipynb`

Simplified notebook for Jovian calldata footprint gas scalar analysis.

### Features
- **Date Range Analysis**: Analyzes multiple dates in a single run
- **Dual Sampling Methods**:
  - Top percentile: Analyze the fullest blocks (e.g., top 1%)
  - Random sampling: Analyze a random sample of N blocks per day
- **Multi-Chain Support**: Base, Optimism, Mode, Zora, World, Ink, Soneium
- **Compression Analysis**: FastLZ compression ratios and DA efficiency metrics
- **Trend Analysis**: Visualize trends across dates
- **Recommendations**: Optimal scalar recommendations based on target excess rate

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

### Gas Limits

Gas limits are loaded from CSV files in `../gas_limits/`:
- `base_gas_limits.csv`
- `optimism_gas_limits.csv`
- etc.

If no gas limit file exists for a chain, defaults to 240,000,000.

### Output

Results are saved to: `../results/{chain}/{start_date}_to_{end_date}/`

Generated files:
- Size estimates histogram
- Blocks exceeding analysis
- Compression analysis
- Trend analysis
- Scalar comparison

### Running the Analysis

1. Open the notebook
2. Configure chain, sampling method, and dates
3. Run all cells
4. Review results and visualizations

### Key Differences from Fjord

| Feature | Fjord | Jovian |
|---------|-------|--------|
| Terminology | footprint_cost | calldata_footprint_gas_scalar |
| Sampling | Top percentile only | Top percentile + Random |
| Compression | Basic | Full analysis with metrics |
| Chains | Base only | 7 chains supported |
