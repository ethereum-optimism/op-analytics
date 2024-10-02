from collections import defaultdict

from op_datasets.schemas.blocks import BLOCKS_SCHEMA
from op_datasets.schemas.transactions import TRANSACTIONS_SCHEMA
from op_coreutils.path import repo_path

import pandas as pd

from py_markdown_table.markdown_table import markdown_table


def generate():
    template_path = repo_path("src/op_analytics/cli/subcommands/chains/docsgen/schemas.md")
    output_path = repo_path("sphinx/sections/onchain/schemas.md")

    with open(template_path) as fobj:
        template = fobj.read()

    with open(output_path, "w") as fobj:
        fobj.write(template)

        multiline = defaultdict(lambda: 30)

        fobj.write("\n\n## Blocks\n")
        fobj.write(
            markdown_table(
                pd.DataFrame([col.display_dict() for col in BLOCKS_SCHEMA.columns])
                .fillna("--")
                .to_dict(orient="records")
            )
            .set_params(
                row_sep="markdown",
                multiline=multiline,
                quote=False,
            )
            .get_markdown()
        )

        fobj.write("\n\n## Transactions\n")
        fobj.write(
            markdown_table(
                pd.DataFrame([col.display_dict() for col in TRANSACTIONS_SCHEMA.columns])
                .fillna("--")
                .to_dict(orient="records")
            )
            .set_params(
                row_sep="markdown",
                multiline=multiline,
                quote=False,
            )
            .get_markdown()
        )
