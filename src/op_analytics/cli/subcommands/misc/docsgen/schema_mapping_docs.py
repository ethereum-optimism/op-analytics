from collections import defaultdict

from op_datasets.schemas.core import Table
from op_datasets.schemas.blocks import BLOCKS_SCHEMA
from op_datasets.schemas.transactions import TRANSACTIONS_SCHEMA
from op_coreutils.path import repo_path
from op_coreutils.gsheets import update_gsheet

import pandas as pd

from py_markdown_table.markdown_table import markdown_table


EXCLUDED_COLS = {"ingestion_metadata"}


def column_details_df(schema: Table) -> list[dict]:
    """Produces a list with display details for all of the columns in the schema."""
    return pd.DataFrame(
        [col.display_dict() for col in schema.columns if col.name not in EXCLUDED_COLS]
    ).fillna("--")


def generate():
    template_path = repo_path("src/op_analytics/cli/subcommands/misc/docsgen/coreschemas.md")
    output_path = repo_path("sphinx/sections/onchain/coreschemas.md")

    with open(template_path) as fobj:
        template = fobj.read()

    with open(output_path, "w") as fobj:
        fobj.write(template)

        multiline = defaultdict(lambda: 30)

        fobj.write("\n\n## Blocks\n")
        blocks_df = column_details_df(BLOCKS_SCHEMA)
        update_gsheet("core_schemas", "Blocks", blocks_df)
        fobj.write(
            markdown_table(blocks_df.to_dict(orient="records"))
            .set_params(
                row_sep="markdown",
                multiline=multiline,
                quote=False,
            )
            .get_markdown()
        )

        fobj.write("\n\n## Transactions\n")
        transactions_df = column_details_df(TRANSACTIONS_SCHEMA)
        update_gsheet("core_schemas", "Transactions", transactions_df)
        fobj.write(
            markdown_table(transactions_df.to_dict(orient="records"))
            .set_params(
                row_sep="markdown",
                multiline=multiline,
                quote=False,
            )
            .get_markdown()
        )
