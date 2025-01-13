import typer

from op_analytics.coreutils.logger import structlog

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
from .defillama.volume_fees_revenue import execute_pull, write_to_clickhouse
from .github_analytics import pull_github_analytics
from .growthepie.chains_daily_fundamentals import pull_growthepie_summary
from .l2beat import pull_l2beat
from .manual_mappings.gsheets_to_bigquery import (
    upload_token_categories,
    upload_token_source_protocols,
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


@app.command()
def defillama_volume_fees_revenue():
    """Pull DEX Volumes, Fees, and Revenue from Defillama."""
    execute_pull()
    write_to_clickhouse()


@app.command()
def growthepie_chain_summary():
    """Pull daily chain summary fundamentals from GrowThePie."""
    pull_growthepie_summary()

    from .growthepie.dataaccess import GrowThePie

    GrowThePie.FUNDAMENTALS_SUMMARY.insert_to_clickhouse(incremental_overlap=1)
    GrowThePie.CHAIN_METADATA.insert_to_clickhouse(incremental_overlap=1)


@app.command()
def token_categories():
    """Upload token categories from Google Sheets to BigQuery."""
    upload_token_categories()


@app.command()
def token_source_protocols():
    """Upload token source protocols from Google Sheets to BigQuery."""
    upload_token_source_protocols()
