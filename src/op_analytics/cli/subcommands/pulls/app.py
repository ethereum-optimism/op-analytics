from typing import Annotated

import typer
from op_coreutils.logger import structlog

from .agora import agora_pull
from .defillama import pull_stablecoins as dfl_pull_stablecoins
from .github_analytics import pull as github_analytics_pull
from .l2beat import pull as l2beat_pull

log = structlog.get_logger()


app = typer.Typer(help="Pull data from 3rd party sources.", add_completion=False)


@app.command()
def l2beat():
    """Pull data from L2 beat."""
    l2beat_pull()


@app.command()
def defillama_stablecoins(
    symbols: Annotated[
        str | None, typer.Option(help="Comma-separated list of symbols to be processed.")
    ] = None,
):
    """Pull stablecoin data from Defillama."""
    if symbols is not None:
        symbols_list = [_.strip() for _ in symbols.split(",")]
    else:
        symbols_list = None

    dfl_pull_stablecoins(symbols=symbols_list)


@app.command()
def agora():
    """Pull data from Agora."""
    agora_pull()


@app.command()
def github_analytics():
    """Pull repo analytics data from GitHub."""
    github_analytics_pull()
