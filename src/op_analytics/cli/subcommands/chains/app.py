import typer
from typing_extensions import Annotated

from op_analytics.cli.subcommands.chains import chain_metadata
from op_coreutils.storage.gcs import gcs_upload_csv

app = typer.Typer(help="Chain related utilities.")


@app.command()
def clean_and_upload_chain_metadata(
    path: Annotated[
        str, typer.Argument(help="Path to local file that has the raw chain metadata.")
    ],
):
    """Upload raw chain metadata csv file.


    The chain_metadata_raw.csv file is maintained manually by the OP Labs team. This function
    accepts a local CSV file with raw chain metadata. It loads the data, cleans it up and uploads
    it to GCS, Dune, Clickhouse and BigQuery.
    """
    import polars as pl

    with open(path, "r") as fcsv:
        raw_df = pl.scan_csv(fcsv)
        clean_df = chain_metadata.clean(raw_df).collect()
        gcs_upload_csv("op_chains_tracking/chain_metadata.csv", clean_df)
