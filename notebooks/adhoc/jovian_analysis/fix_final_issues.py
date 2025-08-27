#!/usr/bin/env python3
"""
Script to fix the final issues in the consolidated notebook.
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
            # Fix duplicate comment and missing default value
            for i, line in enumerate(source):
                if '# Get gas limit for this date from the DataFrame' in line:
                    # Remove duplicate comment
                    source[i] = '    # Get gas limit for this date from the dictionary\n'
                    break
                elif 'gas_limit = gas_limits_dict.get(date)' in line:
                    # Add default value
                    source[i] = '    gas_limit = gas_limits_dict.get(date, 240_000_000)\n'
                    break
                elif 'unique_limits = set(date_gas_limits.values())' in line:
                    # Fix reference to use gas_limits_dict
                    source[i] = 'unique_limits = set(gas_limits_dict.values())\n'
                    break
                elif 'limit_counts = Counter(date_gas_limits.values())' in line:
                    # Fix reference to use gas_limits_dict
                    source[i] = '    limit_counts = Counter(gas_limits_dict.values())\n'
                    break

# Add missing Counter import
for cell in notebook['cells']:
    if cell['cell_type'] == 'code':
        source = cell['source']
        if isinstance(source, list):
            # Look for the imports section
            for i, line in enumerate(source):
                if 'from collections import Counter' in line:
                    # Counter is already imported, no need to add
                    break
                elif 'from collections import' in line and 'Counter' not in line:
                    # Add Counter to existing collections import
                    source[i] = source[i].replace('from collections import', 'from collections import Counter,')
                    break

# Write the fixed notebook
with open('notebooks/jovian_analysis.ipynb', 'w') as f:
    json.dump(notebook, f, indent=2)

print("✅ Fixed final issues in jovian_analysis.ipynb")
