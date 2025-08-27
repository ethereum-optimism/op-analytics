#!/usr/bin/env python3
"""
Script to fix the gas limit access issue in the consolidated notebook.
"""

import json

# Read the notebook
with open('notebooks/jovian_analysis.ipynb', 'r') as f:
    notebook = json.load(f)

# Fix the gas limit access issue
for cell in notebook['cells']:
    if cell['cell_type'] == 'code':
        source = cell['source']
        if isinstance(source, list):
            # Fix the problematic line
            for i, line in enumerate(source):
                if 'gas_limit = date_gas_limits[date]' in line:
                    # Replace with proper Polars DataFrame access
                    source[i] = '    # Get gas limit for this date from the DataFrame\n'
                    source.insert(i+1, '    gas_limit_row = date_gas_limits.filter(pl.col("date") == date)\n')
                    source.insert(i+2, '    if not gas_limit_row.is_empty():\n')
                    source.insert(i+3, '        gas_limit = gas_limit_row["gas_limit"][0]\n')
                    source.insert(i+4, '    else:\n')
                    source.insert(i+5, '        gas_limit = 240_000_000  # Default fallback\n')
                    break

# Write the fixed notebook
with open('notebooks/jovian_analysis.ipynb', 'w') as f:
    json.dump(notebook, f, indent=2)

print("✅ Fixed gas limit access issue in jovian_analysis.ipynb")
