import typer

from op_analytics.coreutils.logger import structlog

from .github_analytics import pull_github_analytics
from .l2beat import pull_l2beat
from .defillama.stablecoins import pull_stablecoins
from .defillama.historical_chain_tvl import pull_historical_chain_tvl
from .defillama.protocols import pull_protocol_tvl
from .agora.delegates import pull_delegates
from .agora.delegate_events import (
    fetch_delegate_votes,
    fetch_proposals,
    fetch_delegate_delegatees,
    fetch_delegate_delegators,
)

log = structlog.get_logger()


app = typer.Typer(help="Pull data from 3rd party sources.", add_completion=False)


@app.command()
def l2beat():
    """Pull data from L2 beat."""
    pull_l2beat()


@app.command()
def defillama_stablecoins_bq():
    """Pull stablecoin data from Defillama.

    NOTE: This is deprecated. Readers should use the GCS dataset.
    """
    # Always pull all symbols from the CLI.
    # You can temporarily modify the code if a partial pull is needed.
    pull_stablecoins(symbols=None)


@app.command()
def defillama_stablecoins():
    """Pull stablecoin data from Defillama."""
    # Always pull all symbols from the CLI.
    # You can temporarily modify the code if a partial pull is needed.
    pull_stablecoins(symbols=None)


@app.command()
def defillama_historical_chain_tvl():
    """Pull historical chain tvl data from Defillama."""
    # Always pull all chains from the CLI.
    # You can temporarily modify the code if a partial pull is needed.
    pull_historical_chain_tvl(pull_chains=None)


@app.command()
def defillama_protocol_tvl():
    """Pull historical chain tvl data from Defillama."""
    # Always pull all protocols from the CLI.
    # You can temporarily modify the code if a partial pull is needed.
    pull_protocol_tvl(pull_protocols=None)


@app.command()
def github_analytics():
    """Pull repo analytics data from GitHub."""
    pull_github_analytics()


@app.command()
def pull_agora_delegate_data():
    """Pull and write agora data."""
    delegates = pull_delegates()

    fetch_delegate_votes(delegates)

    fetch_delegate_delegatees(delegates)

    fetch_delegate_delegators(delegates)

    fetch_proposals()
