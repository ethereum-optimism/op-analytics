from dataclasses import dataclass
from datetime import timedelta

import polars as pl
import spice

from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.coreutils.time import date_fromstr, now_date

from .dataaccess import Dune
from .utils import determine_lookback

log = structlog.get_logger()

TOP_CONTRACTS_QUERY_ID = 5536798
N_DAYS = 7


@dataclass
class DuneTopContractsSummary:
    """Summary of top contracts from Dune."""

    df: pl.DataFrame

    @classmethod
    def fetch(
        cls,
        min_dt: str | None = None,
        max_dt: str | None = None,
        top_n_contracts_per_chain: int = 5000,
        min_usd_per_day_threshold: int = 100,
    ) -> "DuneTopContractsSummary":
        """Fetch Dune top contracts summary."""

        lookback_start, lookback_end = determine_lookback(
            min_dt=min_dt,
            max_dt=max_dt,
            default_n_days=N_DAYS,
        )
        print(f"lookback_start: {lookback_start}, lookback_end: {lookback_end}")

        df = (
            spice.query(
                TOP_CONTRACTS_QUERY_ID,
                # Runs every time so we are guaranteed to get fresh results.
                refresh=True,
                parameters={
                    "lookback_start_days": lookback_start,
                    "lookback_end_days": lookback_end,
                    "top_n_contracts_per_chain": top_n_contracts_per_chain,
                    "min_usd_per_day_threshold": min_usd_per_day_threshold,
                },
                api_key=env_get("DUNE_API_KEY"),
                cache=False,
                performance="large",
            )
            .with_columns(
                pl.col("block_date")
                .str.to_date(format="%Y-%m-%d", strict=False)
                .cast(pl.Utf8)
                .alias("dt")
            )
            .drop("block_date")
        )

        return DuneTopContractsSummary(df=df)


def _create_date_chunks(min_dt: str | None, max_dt: str | None, chunk_days: int = 30):
    """
    Break a date range into smaller chunks to avoid query failures.
    Processes from most recent to earliest date.
    
    Returns:
        List of (min_dt, max_dt) tuples for each chunk, ordered from most recent to earliest
    """
    now_dateval = now_date()
    
    if min_dt is None:
        start_date = now_dateval - timedelta(days=N_DAYS)
    else:
        start_date = date_fromstr(min_dt)
        
    if max_dt is None:
        end_date = now_dateval
    else:
        end_date = date_fromstr(max_dt)
    
    # If the range is small, return as single chunk
    if (end_date - start_date).days <= chunk_days:
        return [(min_dt, max_dt)]
    
    # Break into chunks, working backwards from most recent
    chunks = []
    current_end_date = end_date
    
    while current_end_date > start_date:
        chunk_start_date = max(current_end_date - timedelta(days=chunk_days), start_date)
        
        chunk_start_str = chunk_start_date.strftime("%Y-%m-%d")
        chunk_end_str = current_end_date.strftime("%Y-%m-%d")
        
        chunks.append((chunk_start_str, chunk_end_str))
        current_end_date = chunk_start_date
        
    return chunks


def execute_pull(
    min_dt: str | None = None,
    max_dt: str | None = None,
    top_n_contracts_per_chain: int = 5000,
    min_usd_per_day_threshold: int = 100,
    chunk_days: int = 30,
):
    """
    Fetch and write to GCS with automatic chunking for large date ranges.
    
    Args:
        min_dt: Start date (YYYY-MM-DD format)
        max_dt: End date (YYYY-MM-DD format)  
        top_n_contracts_per_chain: Number of top contracts per chain
        min_usd_per_day_threshold: Minimum USD threshold per day
        chunk_days: Number of days per chunk to avoid query timeouts (default: 30)
    """
    chunks = _create_date_chunks(min_dt, max_dt, chunk_days)
    
    log.info(f"Processing {len(chunks)} chunks of ~{chunk_days} days each")
    
    all_dfs = []
    
    for i, (chunk_start, chunk_end) in enumerate(chunks):
        # Calculate lookback days for this chunk to show in logs
        chunk_lookback_start, chunk_lookback_end = determine_lookback(
            min_dt=chunk_start,
            max_dt=chunk_end,
            default_n_days=N_DAYS,
        )
        
        log.info(f"Processing chunk {i+1}/{len(chunks)}: {chunk_start} ({chunk_lookback_start} days) to {chunk_end} ({chunk_lookback_end} days)")
        
        try:
            result = DuneTopContractsSummary.fetch(
                min_dt=chunk_start,
                max_dt=chunk_end,
                top_n_contracts_per_chain=top_n_contracts_per_chain,
                min_usd_per_day_threshold=min_usd_per_day_threshold,
            )
            
            if result.df.shape[0] > 0:
                all_dfs.append(result.df)
                log.info(f"Chunk {i+1} completed: {result.df.shape[0]} rows")
            else:
                log.info(f"Chunk {i+1} completed: No data returned")
                
        except Exception as e:
            log.error(f"Failed to process chunk {i+1} ({chunk_start} to {chunk_end}): {e}")
            raise
    
    # Combine all dataframes
    if all_dfs:
        combined_df = pl.concat(all_dfs, how="vertical")
        log.info(f"Combined {len(all_dfs)} chunks into {combined_df.shape[0]} total rows")
    else:
        # Create empty dataframe with expected structure
        combined_df = pl.DataFrame()
        log.info("No data found across all chunks")
    
    # Write combined result
    Dune.TOP_CONTRACTS.write(
        dataframe=combined_df,
        sort_by=["dt"],
    )

    return {
        "df": dt_summary(combined_df),
        "chunks_processed": len(chunks),
    } 
