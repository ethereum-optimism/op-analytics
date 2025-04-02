from collections import defaultdict

import pandas as pd
from py_markdown_table.markdown_table import markdown_table

from op_analytics.coreutils.gsheets import update_gsheet
from op_analytics.coreutils.path import repo_path
from op_analytics.datapipeline.schemas import ONCHAIN_CURRENT_VERSION
from op_analytics.datapipeline.schemas.core import CoreDataset

EXCLUDED_COLS = {"ingestion_metadata"}


def column_details_df(schema: CoreDataset) -> pd.DataFrame:
    """Produces a list with display details for all of the columns in the schema."""
    return pd.DataFrame(
        [col.display_dict() for col in schema.columns if col.name not in EXCLUDED_COLS]
    ).fillna("--")


def generate():
    template_path = repo_path("src/op_analytics/cli/subcommands/misc/docsgen/coreschemas.md")
    output_path = repo_path("sphinx/dataplatform/blockbatch/coreschemas.md")

    assert template_path is not None
    assert output_path is not None

    with open(template_path) as fobj:
        template = fobj.read()

    with open(output_path, "w") as fobj:
        fobj.write(template)

        # Avoid having to codify the data columns in the multiline config dictionary
        # by using a defaultdict.
        multiline: dict[str, int] = defaultdict(lambda: 30)

        for name, dataset in ONCHAIN_CURRENT_VERSION.items():
            capitalized = name.capitalize()
            fobj.write(f"\n\n## {capitalized}\n")
            df = column_details_df(dataset)
            update_gsheet("core_schemas", capitalized, df)
            fobj.write(
                markdown_table(df.to_dict(orient="records"))
                .set_params(
                    row_sep="markdown",
                    multiline=multiline,
                    quote=False,
                )
                .get_markdown()
            )
