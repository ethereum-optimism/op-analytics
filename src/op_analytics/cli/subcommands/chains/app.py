import typer
from op_coreutils.logger import LOGGER


from op_analytics.cli.subcommands.chains import chain_metadata
from op_analytics.cli.subcommands.chains import goldsky_dbt_sources
from op_analytics.cli.subcommands.chains import superchain_dbt_sources
from op_analytics.cli.subcommands.chains import dbt_docs

log = LOGGER.get_logger()

app = typer.Typer(help="Chain related utilities.")


app.command(name="upload_metadata")(chain_metadata.upload_metadata)
app.command(name="customize_dbt_docs")(dbt_docs.customize)


@app.command()
def generate_dbt_sources():
    goldsky_dbt_sources.generate()
    superchain_dbt_sources.generate()
