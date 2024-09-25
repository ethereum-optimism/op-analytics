from op_indexer.core import Column, Table


def to_dbt_column(column: Column):
    """Convert a Table to a dbt column."""
    meta = {
        "iceberg_type": column.field_type.__class__.__name__,
        "is_required": column.required,
    }
    if column.json_rpc_method is not None:
        meta["json_rpc_method"] = column.json_rpc_method.name
        meta["json_rpc_field_name"] = column.json_rpc_field_name
    if column.enrichment_function is not None:
        meta["enrichment_function"] = column.enrichment_function

    result = {
        "name": column.name,
    }

    if column.doc:
        result["description"] = column.doc

    result["meta"] = meta
    return result


def to_dbt_table(table: Table):
    """Convert a Table to a dbt source."""

    return {
        "name": table.name,
        "description": table.doc,
        "columns": [to_dbt_column(_) for _ in table.columns],
    }
