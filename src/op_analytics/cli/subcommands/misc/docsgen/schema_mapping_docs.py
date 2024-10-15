from collections import defaultdict

from op_datasets.schemas.core import CoreDataset
from op_datasets.schemas import resolve_core_dataset
from op_coreutils.path import repo_path
from op_coreutils.gsheets import update_gsheet

import pandas as pd

from py_markdown_table.markdown_table import markdown_table


EXCLUDED_COLS = {"ingestion_metadata"}


def column_details_df(schema: CoreDataset) -> list[dict]:
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

        for name in ["Blocks", "Transactions", "Logs", "Traces"]:
            dataset = resolve_core_dataset(name.lower())
            fobj.write(f"\n\n## {name}\n")
            df = column_details_df(dataset)
            update_gsheet("core_schemas", name, df)
            fobj.write(
                markdown_table(df.to_dict(orient="records"))
                .set_params(
                    row_sep="markdown",
                    multiline=multiline,
                    quote=False,
                )
                .get_markdown()
            )
