#!/usr/bin/env python3
"""
Script to generate revshare_transfers_v1__INSERT.sql from YAML config files.

This allows easy editing of the address lists in YAML format while maintaining
the SQL file for ClickHouse batch processing.

Usage:
    python scripts/generate_revshare_sql.py
"""

import yaml
from pathlib import Path


def load_config(config_name: str) -> dict:
    """Load configuration from YAML file."""
    config_path = (
        Path(__file__).parent.parent.parent.parent.parent.parent.parent.parent.parent
        / "src/op_analytics/configs"
        / f"{config_name}.yaml"
    )
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def generate_from_addresses_sql(from_addresses: dict) -> str:
    """Generate SQL for from_addresses CTE."""
    lines = ["WITH from_addresses AS ("]

    for i, (chain, config) in enumerate(from_addresses.items()):
        addresses_str = "[" + ", ".join(f"'{addr}'" for addr in config["addresses"]) + "]"
        tokens_str = "[" + ", ".join(f"'{token}'" for token in config["tokens"]) + "]"
        chains_str = (
            "[" + ", ".join(f"'{chain_name}'" for chain_name in config["expected_chains"]) + "]"
        )

        lines.append("  SELECT")
        lines.append(f"    '{chain}' AS revshare_from_chain,")
        lines.append(f"    {addresses_str} AS revshare_from_addresses,")
        lines.append(f"    {tokens_str} AS token_addresses,")
        lines.append(f"    {chains_str} AS expected_chains")

        if i < len(from_addresses) - 1:
            lines.append("  UNION ALL")

    lines.append("),")
    return "\n".join(lines)


def generate_to_addresses_sql(to_addresses: dict) -> str:
    """Generate SQL for to_addresses CTE."""
    lines = ["to_addresses AS ("]

    for i, (address, config) in enumerate(to_addresses.items()):
        end_date = f"'{config['end_date']}'" if config.get("end_date") else "NULL"
        chains_str = "[" + ", ".join(f"'{chain}'" for chain in config["expected_chains"]) + "]"

        lines.append("  SELECT")
        lines.append(f"    '{address}' AS to_address,")
        lines.append(f"    '{config['description']}' AS description,")
        lines.append(f"    {end_date} AS end_date,")
        lines.append(f"    {chains_str} AS expected_chains")

        if i < len(to_addresses) - 1:
            lines.append("  UNION ALL")

    lines.append("),")
    return "\n".join(lines)


def generate_sql():
    """Generate the complete SQL file."""
    from_addresses = load_config("revshare_from_addresses")
    to_addresses = load_config("revshare_to_addresses")

    # Read the template SQL (everything after the CTEs)
    template_path = Path(__file__).parent.parent / "revshare_transfers_v1__INSERT.sql"

    with open(template_path, "r") as f:
        content = f.read()

    # Find where the CTEs end and the main query begins
    lines = content.split("\n")
    main_query_start = None

    for i, line in enumerate(lines):
        if "-- Native transfers" in line:
            main_query_start = i
            break

    if main_query_start is None:
        raise ValueError("Could not find '-- Native transfers' marker in SQL file")

    # Generate new SQL
    new_sql = []
    new_sql.append(generate_from_addresses_sql(from_addresses))
    new_sql.append("")
    new_sql.append(generate_to_addresses_sql(to_addresses))
    new_sql.append("")
    new_sql.append("\n".join(lines[main_query_start:]))

    # Write the new SQL file
    output_path = Path(__file__).parent.parent / "revshare_transfers_v1__INSERT.sql"

    with open(output_path, "w") as f:
        f.write("\n".join(new_sql))

    print(f"Generated SQL file: {output_path}")
    print("To modify addresses, edit the YAML files and run this script again.")


if __name__ == "__main__":
    generate_sql()
