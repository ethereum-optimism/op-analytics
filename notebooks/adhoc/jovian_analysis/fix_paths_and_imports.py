#!/usr/bin/env python3
"""
Script to fix all path and import issues after folder reorganization.
"""

import json

# Read the notebook
with open('notebooks/jovian_analysis.ipynb', 'r') as f:
    notebook = json.load(f)

# Fix the issues
for cell in notebook['cells']:
    if cell['cell_type'] == 'code':
        source = cell['source']
        if isinstance(source, list):
            # Fix the gas limits loading section
            for i, line in enumerate(source):
                if 'date_gas_limits = load_gas_limits(csv_path=' in line:
                    # Fix the path and add proper gas limits handling
                    source[i] = 'date_gas_limits = load_gas_limits(csv_path=f"{DATA_PATH}/{FILE_PATH}")\n'
                    source.insert(i+1, 'print(f"✅ Loaded gas limits for {len(date_gas_limits)} dates")\n')
                    source.insert(i+2, '\n')
                    source.insert(i+3, '# Convert to dictionary for easier access\n')
                    source.insert(i+4, 'gas_limits_dict = {}\n')
                    source.insert(i+5, 'for row in date_gas_limits.iter_rows(named=True):\n')
                    source.insert(i+6, '    gas_limits_dict[row[\'date\']] = row[\'gas_limit\']\n')
                    source.insert(i+7, '\n')
                    source.insert(i+8, '# Cache configuration\n')
                    break
                elif 'gas_limit = date_gas_limits[date]' in line:
                    # Fix the gas limit access in the loop
                    source[i] = '    # Get gas limit for this date from the dictionary\n'
                    source.insert(i+1, '    gas_limit = gas_limits_dict.get(date, 240_000_000)\n')
                    break
                elif 'unique_limits = set(date_gas_limits.values())' in line:
                    # Fix reference to use gas_limits_dict
                    source[i] = 'unique_limits = set(gas_limits_dict.values())\n'
                    break
                elif 'limit_counts = Counter(date_gas_limits.values())' in line:
                    # Fix reference to use gas_limits_dict
                    source[i] = '    limit_counts = Counter(gas_limits_dict.values())\n'
                    break

# Write the fixed notebook
with open('notebooks/jovian_analysis.ipynb', 'w') as f:
    json.dump(notebook, f, indent=2)

print("✅ Fixed all path and import issues in jovian_analysis.ipynb")
