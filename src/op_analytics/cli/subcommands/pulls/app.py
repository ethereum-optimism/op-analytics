import typer

from op_analytics.coreutils.logger import structlog
from op_analytics.datasources.agora.delegate_events import (
    fetch_delegate_delegatees,
    fetch_delegate_delegators,
    fetch_delegate_votes,
    fetch_proposals,
)
from op_analytics.datasources.agora.delegates import pull_delegates
from op_analytics.datasources.defillama.historical_chain_tvl import pull_historical_chain_tvl
from op_analytics.datasources.defillama.protocols import pull_protocol_tvl
from op_analytics.datasources.defillama.stablecoins import pull_stablecoins
from op_analytics.datasources.github import execute as github_execute
from op_analytics.datasources.growthepie.chains_daily_fundamentals import pull_growthepie_summary
from op_analytics.datasources.l2beat import pull_l2beat

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
def github():
    """Pull repo analytics data from GitHub."""
    github_execute.execute_pull_traffic()
    github_execute.execute_pull_activity()


@app.command()
def pull_agora_delegate_data():
    """Pull and write agora data."""
    delegates = pull_delegates()

    fetch_delegate_votes(delegates)

    fetch_delegate_delegatees(delegates)

    fetch_delegate_delegators(delegates)

    fetch_proposals()


@app.command()
def growthepie_chain_summary():
    """Pull daily chain summary fundamentals from GrowThePie."""
    pull_growthepie_summary()

    from op_analytics.datasources.growthepie.dataaccess import GrowThePie

    GrowThePie.FUNDAMENTALS_SUMMARY.insert_to_clickhouse(incremental_overlap=1)
    GrowThePie.CHAIN_METADATA.insert_to_clickhouse(incremental_overlap=1)
