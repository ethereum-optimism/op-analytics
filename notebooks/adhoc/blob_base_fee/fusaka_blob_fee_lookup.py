"""
Fusaka Blob Base Fee Lookup Generator

This script computes exact blob base fees for Fusaka using Python's arbitrary
precision integers, matching go-ethereum's fake_exponential implementation exactly.

Usage:
1. Export unique excess_blob_gas values from Fusaka blocks on Dune
2. Run this script to compute blob_base_fee for each
3. Upload result to Dune as a lookup table

The SQL query to get Fusaka excess_blob_gas values:
    SELECT DISTINCT excess_blob_gas
    FROM ethereum.blocks
    WHERE number >= 23935694  -- Fusaka activation
"""

import csv
from pathlib import Path


# Constants from EIP-7840 blobSchedule
MIN_BASE_FEE_PER_BLOB_GAS_WEI = 1
BLOB_BASE_FEE_UPDATE_FRACTION_PECTRA = 5007716  # Same for Fusaka

# EIP-7918 reserve price constants
BLOB_BASE_COST = 8192  # 2^13
GAS_PER_BLOB = 131072  # 2^17


def fake_exponential(factor: int, numerator: int, denominator: int) -> int:
    """
    Exact translation of go-ethereum's fakeExponential function.
    
    Approximates factor * e^(numerator / denominator) using Taylor expansion.
    Uses Python's arbitrary precision integers for exact integer math.
    """
    output = 0
    accum = factor * denominator
    
    i = 1
    while accum > 0:
        output += accum
        accum = (accum * numerator) // denominator
        accum = accum // i
        i += 1
    
    return output // denominator


def calc_blob_base_fee_exp(excess_blob_gas: int) -> int:
    """
    Calculate the exponential part of blob base fee for Fusaka.
    
    Formula: fake_exponential(1, excess_blob_gas, 5007716)
    """
    return fake_exponential(
        MIN_BASE_FEE_PER_BLOB_GAS_WEI,
        excess_blob_gas,
        BLOB_BASE_FEE_UPDATE_FRACTION_PECTRA
    )


def calc_blob_base_fee_with_reserve(excess_blob_gas: int, base_fee_per_gas: int) -> int:
    """
    Calculate full Fusaka blob base fee including EIP-7918 reserve price.
    
    Returns: max(exp_part, base_fee_per_gas * BLOB_BASE_COST / GAS_PER_BLOB)
    
    Note: For the lookup table, we only store the exp_part since the reserve price
    depends on base_fee_per_gas which varies per block.
    """
    exp_part = calc_blob_base_fee_exp(excess_blob_gas)
    reserve_price = (base_fee_per_gas * BLOB_BASE_COST) // GAS_PER_BLOB
    return max(exp_part, reserve_price)


def generate_lookup_from_csv(input_csv: str, output_csv: str):
    """
    Read excess_blob_gas values from input CSV and generate lookup table.
    
    Input CSV should have a column named 'excess_blob_gas'.
    Output CSV will have columns: excess_blob_gas, blob_base_fee_exp
    """
    input_path = Path(input_csv)
    output_path = Path(output_csv)
    
    # Read unique excess_blob_gas values
    excess_values = set()
    with open(input_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            excess_values.add(int(row['excess_blob_gas']))
    
    print(f"Loaded {len(excess_values)} unique excess_blob_gas values")
    
    # Compute blob base fee for each
    results = []
    for i, excess in enumerate(sorted(excess_values)):
        blob_fee = calc_blob_base_fee_exp(excess)
        results.append({
            'excess_blob_gas': excess,
            'blob_base_fee_exp': blob_fee
        })
        if (i + 1) % 1000 == 0:
            print(f"  Processed {i + 1}/{len(excess_values)}...")
    
    # Write output CSV
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['excess_blob_gas', 'blob_base_fee_exp'])
        writer.writeheader()
        writer.writerows(results)
    
    print(f"Wrote {len(results)} rows to {output_path}")
    return results


def generate_lookup_from_list(excess_values: list[int], output_csv: str):
    """
    Generate lookup table from a list of excess_blob_gas values.
    """
    output_path = Path(output_csv)
    
    # Dedupe and sort
    excess_values = sorted(set(excess_values))
    print(f"Computing blob fees for {len(excess_values)} unique excess_blob_gas values...")
    
    # Compute blob base fee for each
    results = []
    for excess in excess_values:
        blob_fee = calc_blob_base_fee_exp(excess)
        results.append({
            'excess_blob_gas': excess,
            'blob_base_fee_exp': blob_fee
        })
    
    # Write output CSV
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['excess_blob_gas', 'blob_base_fee_exp'])
        writer.writeheader()
        writer.writerows(results)
    
    print(f"Wrote {len(results)} rows to {output_path}")
    return results


# Example usage / quick test
if __name__ == "__main__":
    # Test with the values the user mentioned
    test_values = [
        82572471,
        70734440,
        71345848,
        82486623,
        73312761,
        71783474,
        83885604,
        84932517,
        84058562,
        83927439,
        83534073,
        91007622,
        75672130,
        136399112,
        82528795,
        73837200,
        73181772,
        71128108,
        74797983,
        70822023,
        83622563,
        83185178,
        83096914,
        85238154,
        84713820,
    ]
    
    print("Testing fake_exponential with user's excess_blob_gas values:\n")
    print(f"{'excess_blob_gas':>15} | {'blob_base_fee_exp (wei)':>25} | {'(gwei)':>15}")
    print("-" * 60)
    
    for excess in sorted(test_values)[:10]:  # Show first 10
        fee = calc_blob_base_fee_exp(excess)
        fee_gwei = fee / 1e9
        print(f"{excess:>15,} | {fee:>25,} | {fee_gwei:>15.6f}")
    
    print("\n... and more values")
    
    # Generate a test lookup file
    output_file = Path(__file__).parent / "fusaka_blob_fees_lookup.csv"
    generate_lookup_from_list(test_values, str(output_file))
    
    print(f"\nTo use with the full Fusaka dataset:")
    print("1. Run this Dune query to export excess_blob_gas values:")
    print("   SELECT DISTINCT excess_blob_gas FROM ethereum.blocks WHERE number >= 23935694")
    print("2. Save as CSV, then run:")
    print("   generate_lookup_from_csv('fusaka_excess_values.csv', 'fusaka_blob_fees_lookup.csv')")
    print("3. Upload fusaka_blob_fees_lookup.csv to Dune")

