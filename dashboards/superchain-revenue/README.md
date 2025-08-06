# Superchain Revenue Dashboard

A static dashboard displaying revenue sharing data across the Optimism Collective and Superchain.

## Features

### High-Level Metrics
- **Monthly Optimism Earnings**: Total revshare earned by Optimism from all chains
- **Share of L2 Transaction Fees**: Optimism's market share in the L2 ecosystem

### Visualizations
1. **Stacked Column Chart**: Monthly earnings breakdown by chain
2. **Financial Statement Table**: Comprehensive revenue metrics across time periods
3. **Chain Logo Bubbles**: Interactive chain performance overview
4. **Transactions Log**: Recent revenue transfer transactions with block explorer links

### Interactive Features
- Toggle between monthly and quarterly views
- Hover tooltips with detailed chain information
- Responsive design for mobile and desktop
- Real-time data loading from CSV files

## Data Sources

The dashboard reads from three CSV files:

### 1. `revenue_data.csv`
- `dt_month`: Date in YYYY-MM-DD format (monthly)
- `chain_name`: Unique chain identifier
- `display_name`: Human-readable chain name
- `l2_fees_as_superchain`: Total L2 fees collected
- `l1_costs_as_superchain`: Total L1 costs incurred
- `net_onchain_profit_as_superchain`: L2 fees minus L1 costs
- `revshare_estimated`: Revenue shared with Optimism
- `chain_governor_profit_estimated`: Profit retained by chain governor

### 2. `op_collective_revenue_transactions.csv`
- `dt_month`: Date in YYYY-MM-DD format
- `chain_name`: Chain name (should match display_name from revenue_data)
- `is_op_transfer`: Boolean filter (only "true" values are displayed)
- `tx_block_time`: Transaction timestamp
- `tx_hash`: Transaction hash for block explorer linking
- `value_decimal`: ETH value transferred

### 3. `Superchain_market_share.csv`
- `dt_month`: Date in YYYY-MM-DD format
- `Metric`: Metric type (use "l2_chain_fees_share" for market share)
- `value`: Numeric value (percentage for market share)

## Setup and Usage

### Local Development
1. Place your CSV files in the same directory as `index.html`
2. Start a local web server (required for CSV loading):
   ```bash
   # Python 3
   python -m http.server 8000
   
   # Python 2
   python -m SimpleHTTPServer 8000
   
   # Node.js (if you have http-server installed)
   npx http-server
   
   # PHP
   php -S localhost:8000
   ```
3. Open http://localhost:8000 in your browser

### Production Deployment
1. Upload all files (`index.html`, `styles.css`, `script.js`, and CSV files) to your web server
2. Ensure CSV files are accessible via HTTP requests
3. Update CSV file paths in `script.js` if needed

## Customization

### Adding New Chains
1. Add chain data to `revenue_data.csv`
2. Update `CHAIN_COLORS` and `CHAIN_ICONS` objects in `script.js`
3. Add block explorer URL to `BLOCK_EXPLORERS` object

### Styling
- Colors and themes can be modified in `styles.css`
- CSS custom properties (variables) are defined in `:root` selector
- Chart colors are controlled by `CHAIN_COLORS` in `script.js`

### Data Processing
- Time range calculations can be modified in `script.js`
- Chart filtering logic (top 10 chains, minimum thresholds) can be adjusted
- Table metrics can be added or modified in the `metrics` array

## Dependencies

- **Chart.js**: For the stacked column chart visualization
- **PapaParse**: For CSV file parsing
- **Modern Browser**: Supports ES6+ features (async/await, arrow functions, etc.)

## Browser Support

- Chrome 60+
- Firefox 60+
- Safari 12+
- Edge 79+

## Performance Notes

- CSV files are loaded once on page load and cached in memory
- Chart.js provides hardware acceleration for smooth animations
- Responsive design ensures good performance on mobile devices
- Large datasets (>1000 rows) may require pagination for transaction table

## Troubleshooting

### Dashboard Won't Load
- Check browser console for errors
- Ensure CSV files are in the correct location
- Verify local server is running (CSV files require HTTP, not file:// protocol)

### Missing Data
- Check CSV file formatting (headers must match exactly)
- Ensure date formats are YYYY-MM-DD
- Verify boolean values in `is_op_transfer` column are "true" (string)

### Styling Issues
- Check for CSS conflicts with existing page styles
- Verify CSS custom properties are supported in your browser
- Test responsive design at different screen sizes

## License

This dashboard is part of the op-analytics project and follows the same licensing terms.