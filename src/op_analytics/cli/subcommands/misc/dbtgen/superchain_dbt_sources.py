from op_coreutils.logger import structlog
from op_coreutils.path import repo_path
from op_datasets.schemas.blocks.v1 import BLOCKS_V1_SCHEMA
from op_datasets.schemas.core import Column, CoreDataset

from op_analytics.cli.subcommands.misc.dbtgen.yamlwriter import write_sources_yaml

log = structlog.get_logger()


def to_dbt_column(column: Column):
    """Convert a Table to a dbt column."""
    meta = {
        "iceberg_type": column.field_type.__class__.__name__,
        "is_required": column.required,
    }
    if column.json_rpc_method is not None:
        meta["json_rpc_method"] = column.json_rpc_method.name
        meta["json_rpc_field_name"] = column.json_rpc_field_name
    if column.op_analytics_enrichment_function is not None:
        meta["enrichment_function"] = column.op_analytics_enrichment_function

    result: dict[str, str | dict] = {
        "name": column.name,
    }

    if column.doc:
        result["description"] = column.doc

    result["meta"] = meta
    return result


def to_dbt_table(table: CoreDataset):
    """Convert a Table to a dbt source."""

    return {
        "name": table.name,
        "description": table.doc,
        "columns": [to_dbt_column(_) for _ in table.columns],
    }


def generate():
    """Generate dbt source YAML files for our core tables."""
    source = "superchain_oplabs"

    dbt_sources = {
        "version": "2.0",
        "sources": [
            {
                "name": source,
                "description": "Tables for superchain data curated by OP Labs.",
                "loader": "OP Labs",
                "tables": [to_dbt_table(_) for _ in [BLOCKS_V1_SCHEMA]],
            }
        ],
    }

    path = repo_path(f"dbt/sources/{source}.yml")
    write_sources_yaml(path, dbt_sources)
