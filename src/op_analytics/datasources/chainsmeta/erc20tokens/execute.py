from datetime import date


from op_analytics.coreutils.threads import run_concurrently_store_failures
from op_analytics.coreutils.time import now_date
from op_analytics.coreutils.logger import structlog


from .database import find_tokens
from .chaintokens import ChainTokens


log = structlog.get_logger()


def execute_pull(process_dt: date | None = None):
    process_dt = process_dt or now_date()

    # Find the list of ERC20 tokens to update. These are grouped by chain.
    chains: list[ChainTokens] = find_tokens(process_dt=process_dt)
    targets: dict[str, ChainTokens] = {_.chain: _ for _ in chains}

    def _fetch(x: ChainTokens):
        return x.fetch(process_dt=process_dt)

    # Run each chain on a separate thread, but continue even if some chains fail.
    summary = run_concurrently_store_failures(
        function=_fetch,
        targets=targets,
        max_workers=16,
    )

    # Log any chain failures but don't fail the entire job
    if summary.failures:
        for chain, error_msg in summary.failures.items():
            detail = f"failed chain={chain}: error={error_msg}"
            log.error(detail)

        log.warning(
            f"{len(summary.failures)} chains failed to execute, but continuing with successful chains"
        )

    # Log success summary
    successful_chains = len(summary.results)
    total_chains = len(targets)
    log.info(
        f"ERC20 tokens processing completed: {successful_chains}/{total_chains} chains successful"
    )

    # TODO:
    # Here we can copy over from the clickhouse dimension table to GCS.
    # As we migrate out of BigQuery it maybe useful to have a copy of the token
    # metdata that we can read from BigQuery.
    # ChainsMeta.ERC20_TOKEN_METADATA.write(dataframe=df.with_columns(dt=pl.lit(DEFAULT_DT)))

    # This will be a dictionary from chain to total rows inserted.
    return summary.results
