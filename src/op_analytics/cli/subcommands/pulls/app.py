import typer

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import insert_daily_data_to_clickhouse

from .agora.delegate_events import (
    fetch_delegate_delegatees,
    fetch_delegate_delegators,
    fetch_delegate_votes,
    fetch_proposals,
)
from .agora.delegates import pull_delegates
from .defillama.historical_chain_tvl import pull_historical_chain_tvl
from .defillama.protocols import pull_protocol_tvl
from .defillama.stablecoins import pull_stablecoins
from .defillama.dex_volume_fees_revenue import pull_dex_volume, pull_fees, pull_revenue
from .github_analytics import pull_github_analytics
from .growthepie.chains_daily_fundamentals import pull_growthepie_summary
from .l2beat import pull_l2beat

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

@app.command()
def defillama_dexs_fees_revenue():
    """Pull DEX Volumes, Fees, and Revenue from Defillama."""
    pull_dex_volume()
    pull_fees()
    pull_revenue()

@app.command()
def defillama_dexs():
    """Pull DEX Volumes from Defillama."""
    pull_dex_volume()

@app.command()
def defillama_fees():
    """Pull Fees from Defillama."""
    pull_fees()

@app.command()
def defillama_revenue():
    """Pull Revenue from Defillama."""
    pull_revenue()

@app.command()
def growthepie_chain_summary():
    """Pull daily chain summary fundamentals from GrowThePie."""
    pull_growthepie_summary()

    from .growthepie.dataaccess import GrowThePie

    insert_daily_data_to_clickhouse(GrowThePie.FUNDAMENTALS_SUMMARY)
