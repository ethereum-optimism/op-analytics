import polars as pl
import typer
from op_coreutils.logger import LOGGER
from op_datasets.coretables.fromgoldsky import get_core_tables

from op_analytics.cli.subcommands.chains import chain_metadata
from op_analytics.cli.subcommands.chains.dbtgen import (
    dbt_docs,
    goldsky_dbt_sources,
    goldsky_dbt_views,
    superchain_dbt_sources,
)

log = LOGGER.get_logger()

app = typer.Typer(help="Chain related utilities.")


app.command(name="upload_metadata")(chain_metadata.upload_metadata)
app.command(name="customize_dbt_docs")(dbt_docs.customize)


@app.command()
def generate_dbt():
    goldsky_dbt_sources.generate()
    superchain_dbt_sources.generate()
    goldsky_dbt_views.generate()


@app.command()
def process_blocks(blocks: str):
    """[WIP]. Run our transformation process on a range of blocks."""
    dataframes = get_core_tables(blocks)

    ctx = pl.SQLContext()
    for name, df in dataframes.items():
        ctx.register(name=name, frame=df)

    extractions = []

    results = []
    for extraction in extractions:
        results.append(extraction(ctx, dataframes))
