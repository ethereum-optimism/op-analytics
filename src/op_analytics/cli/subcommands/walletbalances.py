import typer
from pathlib import Path
from typing_extensions import Annotated

from op_analytics.coreutils.logger import structlog
from op_analytics.datasources.walletbalances.main import execute_wallet_balance_pull

log = structlog.get_logger()

app = typer.Typer(
    help="Wallet ERC20 token balance utilities.",
    add_completion=False,
    pretty_exceptions_show_locals=False,
)


@app.command()
def pull_balances(
    wallets_csv: Annotated[
        str,
        typer.Option(
            "--wallets-csv",
            help="Path to CSV file containing wallet addresses. Must have 'address' column, optional 'label' column."
        ),
    ],
    tokens_csv: Annotated[
        str,
        typer.Option(
            "--tokens-csv",
            help="Path to CSV file containing token addresses. Must have 'address' column, optional 'symbol' and 'decimals' columns."
        ),
    ],
    output_csv: Annotated[
        str,
        typer.Option(
            "--output-csv",
            help="Path for output CSV file with wallet-token balance results."
        ),
    ],
    rpc_endpoint: Annotated[
        str,
        typer.Option(
            "--rpc-endpoint",
            help="RPC endpoint URL for blockchain queries (e.g., https://mainnet.optimism.io/)"
        ),
    ],
    rate_limit_delay: Annotated[
        float,
        typer.Option(
            "--rate-limit-delay",
            help="Delay in seconds between RPC calls to avoid rate limiting."
        ),
    ] = 0.1,
    safe_api_url: Annotated[
        str | None,
        typer.Option(
            "--safe-api-url",
            help="Optional Safe API URL to fetch additional multisig addresses (labeled as 'locked multisig')"
        ),
    ] = None,
):
    """Pull ERC20 token balances for wallet-token combinations.
    
    This command reads wallet addresses and ERC20 token addresses from CSV files,
    then queries the current balance for each wallet-token combination using RPC calls.
    
    The output CSV will contain columns:
    - wallet: The wallet address
    - wallet_label: Optional label for the wallet
    - token: The token contract address  
    - token_symbol: Optional symbol for the token
    - balance: Raw balance as string (preserves precision)
    - decimals: Number of decimal places for the token
    - balance_formatted: Human-readable balance (balance / 10^decimals)
    
    Example usage:
    
        uv run opdata walletbalances pull-balances \\
            --wallets-csv wallets.csv \\
            --tokens-csv tokens.csv \\
            --output-csv balances.csv \\
            --rpc-endpoint https://mainnet.optimism.io/ \\
            --rate-limit-delay 0.1
    """
    
    # Validate input files exist
    wallets_path = Path(wallets_csv)
    tokens_path = Path(tokens_csv)
    
    if not wallets_path.exists():
        log.error(f"Wallets CSV file not found: {wallets_csv}")
        raise typer.Exit(code=1)
    
    if not tokens_path.exists():
        log.error(f"Tokens CSV file not found: {tokens_csv}")
        raise typer.Exit(code=1)
    
    # Ensure output directory exists
    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        log.info("Starting wallet balance pull operation")
        
        results = execute_wallet_balance_pull(
            wallets_csv=wallets_csv,
            tokens_csv=tokens_csv,
            output_csv=output_csv,
            rpc_endpoint=rpc_endpoint,
            rate_limit_delay=rate_limit_delay,
            safe_api_url=safe_api_url
        )
        
        log.info(f"Successfully completed wallet balance pull. Results saved to {output_csv}")
        print(f"✅ Pulled balances for {len(results)} wallet-token combinations")
        print(f"📁 Results saved to: {output_csv}")
        
    except Exception as e:
        log.error(f"Wallet balance pull failed: {str(e)}")
        raise typer.Exit(code=1)
