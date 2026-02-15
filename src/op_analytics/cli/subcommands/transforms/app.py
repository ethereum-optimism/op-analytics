import typer
from typing_extensions import Annotated

from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()

app = typer.Typer(
    help="Transform utilities for data processing.",
    add_completion=False,
    pretty_exceptions_show_locals=False,
)


@app.command(name="fees-sankey")
def fees_sankey_command(
    days: Annotated[int, typer.Option("--days", help="Number of days to look back")] = 90,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Don't write to databases, just validate")
    ] = False,
):
    """Generate Sankey diagram dataset for Superchain fee flows."""
    from op_analytics.transforms.fees_sankey.generate_sankey_fees_dataset import execute_pull

    log.info("Starting fees Sankey transform", days=days, dry_run=dry_run)

    try:
        result = execute_pull(days=days, dry_run=dry_run)
        log.info("Transform completed successfully", **result)
    except Exception as e:
        log.error("Transform failed", error=str(e))
        raise typer.Exit(1)
