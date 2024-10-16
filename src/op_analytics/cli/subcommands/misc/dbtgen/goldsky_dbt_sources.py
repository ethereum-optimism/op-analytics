from op_coreutils.path import repo_path

from op_analytics.cli.subcommands.misc.dbtgen.yamlwriter import write_sources_yaml


def generate():
    """Generate dbt source YAML files for goldsky ingested tables."""

    datasets = ["blocks", "transactions", "traces", "logs"]
    tables = [
        dict(name=f"{dataset}", identifier=f"merge('default', '\\w+_{dataset}$')")
        for dataset in datasets
    ]

    source = "goldsky_pipelines"
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
