from op_coreutils.path import repo_path
from op_analytics.cli.subcommands.chains.chain_metadata import goldsky_chains

from op_analytics.cli.subcommands.chains.dbtgen.yamlwriter import write_sources_yaml


def generate():
    """Generate dbt source YAML files for goldsky ingested tables."""

    chains = goldsky_chains()
    datasets = ["blocks", "transactions", "traces", "logs"]
    tables = [dict(name=f"{chain}_{dataset}") for chain in chains for dataset in datasets]

    source = "superchain_goldsky"
    dbt_sources = {
        "version": "2.0",
        "sources": [
            {
                "name": source,
                "schema": "default",
                "description": "Tables for data ingested by Goldsky.",
                "loader": "Goldsky",
                "tables": tables,
            }
        ],
    }

    path = repo_path(f"dbt/sources/{source}.yml")
    write_sources_yaml(path, dbt_sources)
