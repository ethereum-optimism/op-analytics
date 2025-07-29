import typer
from rich import print
from typing_extensions import Annotated

from op_analytics.coreutils.logger import structlog
from op_analytics.datasources.dune.top_contracts import execute_pull as top_contracts_execute_pull

log = structlog.get_logger()

app = typer.Typer(
    help="Dune Analytics data utilities.",
    add_completion=False,
    pretty_exceptions_show_locals=False,
)

# Common option types
FORCE_COMPLETE_OPTION = Annotated[
    bool, typer.Option(help="Run even if completion markers already exist.")
]


@app.command()
def top_contracts(
    min_dt: Annotated[
        str,
        typer.Option(
            "--min-dt",
            help="Start date in YYYY-MM-DD format. If not provided, defaults to 7 days ago."
        ),
    ] = None,
    max_dt: Annotated[
        str,
        typer.Option(
            "--max-dt", 
            help="End date in YYYY-MM-DD format. If not provided, defaults to today."
        ),
    ] = None,
    top_n_contracts_per_chain: Annotated[
        int,
        typer.Option(
            "--top-n-contracts",
            help="Number of top contracts to fetch per chain."
        ),
    ] = 5000,
    min_usd_per_day_threshold: Annotated[
        int,
        typer.Option(
            "--min-usd-threshold",
            help="Minimum USD threshold per day for contracts to be included."
        ),
    ] = 100,
    chunk_days: Annotated[
        int,
        typer.Option(
            "--chunk-days",
            help="Number of days per chunk to avoid query timeouts."
        ),
    ] = 7,
    force_complete: FORCE_COMPLETE_OPTION = False,
):
    """Fetch top contracts data from Dune Analytics.
    
    This command fetches data about the highest gas-consuming contracts across
    different chains using Dune Analytics. Data is processed in chunks to handle
    large date ranges efficiently.
    
    Examples:
        # Fetch last 7 days (default)
        uv run opdata dune top-contracts
        
        # Fetch specific date range
        uv run opdata dune top-contracts --min-dt 2025-01-01 --max-dt 2025-01-31
        
        # Force re-process existing data
        uv run opdata dune top-contracts --force-complete
    """
    print(f"Fetching Dune top contracts data...")
    print(f"  Date range: {min_dt or 'auto'} to {max_dt or 'auto'}")
    print(f"  Top contracts per chain: {top_n_contracts_per_chain}")
    print(f"  Min USD threshold: ${min_usd_per_day_threshold}")
    print(f"  Chunk size: {chunk_days} days")
    print(f"  Force complete: {force_complete}")
    print()
    
    try:
        result = top_contracts_execute_pull(
            min_dt=min_dt,
            max_dt=max_dt,
            top_n_contracts_per_chain=top_n_contracts_per_chain,
            min_usd_per_day_threshold=min_usd_per_day_threshold,
            chunk_days=chunk_days,
            force_complete=force_complete,
        )
        
        print("✅ Success!")
        print(f"  Chunks processed: {result['chunks_processed']}")
        print(f"  Chunks skipped: {result['chunks_skipped']}")
        print(f"  Total rows written: {result['total_rows_written']:,}")
        
        if "data_summary" in result:
            print(f"  Date range: {result['date_range']}")
            print(f"  Unique chains: {result['unique_chains']}")
            print(f"  Unique contracts: {result['unique_contracts']}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        log.error(f"Failed to execute top_contracts pull: {e}")
        raise typer.Exit(1) 
