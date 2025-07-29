from dataclasses import dataclass
from datetime import timedelta

import polars as pl
import spice

from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.misc import raise_for_schema_mismatch
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.coreutils.time import date_fromstr, now_date

from .dataaccess import Dune
from .utils import determine_lookback

log = structlog.get_logger()

TOP_CONTRACTS_QUERY_ID = 5536798
N_DAYS = 7
CHUNK_SIZE = 7

# Expected schema for validation
TOP_CONTRACTS_SCHEMA = pl.Schema({
    "dt": pl.Utf8,
    "chain": pl.Utf8,
    "chain_id": pl.Int64,
    "contract_address": pl.Utf8,
    "gas_fees_sum": pl.Float64,
    "gas_fees_sum_usd": pl.Float64,
    "tx_count": pl.Int64,
    "name": pl.Utf8,
    "gas_fees_pct_of_chain": pl.Float64,
    "rank_within_chain": pl.Int64,
})


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
        # print(f"lookback_start: {lookback_start}, lookback_end: {lookback_end}")

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


def _create_date_chunks(min_dt: str | None, max_dt: str | None, chunk_days: int = CHUNK_SIZE):
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
        # Convert to proper date strings
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        return [(start_str, end_str)]
    
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


def _chunk_has_data(chunk_start: str, chunk_end: str) -> bool:
    """Check if data already exists for ALL dates in the given chunk range using markers."""
    try:
        # Generate all expected dates in the range
        start_date = date_fromstr(chunk_start)
        end_date = date_fromstr(chunk_end)
        
        expected_dates = []
        current_date = start_date
        while current_date <= end_date:
            expected_dates.append(current_date)
            current_date += timedelta(days=1)
        
        # Query markers for these dates - this is much more efficient than reading data files
        markers_df = Dune.TOP_CONTRACTS.written_markers_datevals(expected_dates)
        
        if len(markers_df) == 0:
            log.debug(f"No markers found for chunk {chunk_start} to {chunk_end}")
            return False
        
        # Get the dates that have markers (i.e., data was successfully written)
        existing_dates = set(markers_df["dt"].to_list())
        expected_dates_set = set(expected_dates)
        
        missing_dates = expected_dates_set - existing_dates
        
        if missing_dates:
            missing_date_strs = sorted([d.strftime("%Y-%m-%d") for d in missing_dates])
            log.info(f"Chunk {chunk_start} to {chunk_end} missing markers for {len(missing_dates)} dates: {missing_date_strs}")
            return False
        
        log.debug(f"Chunk {chunk_start} to {chunk_end} has complete markers for all {len(expected_dates)} dates")
        return True
        
    except Exception as e:
        # If there's any error reading markers, assume no data
        log.debug(f"Error checking markers for chunk {chunk_start} to {chunk_end}: {e}")
        return False


def execute_pull(
    min_dt: str | None = None,
    max_dt: str | None = None,
    top_n_contracts_per_chain: int = 5000,
    min_usd_per_day_threshold: int = 100,
    chunk_days: int = CHUNK_SIZE,
    force_complete: bool = False,
):
    """
    Fetch and write to GCS with automatic chunking for large date ranges.
    
    Args:
        min_dt: Start date (YYYY-MM-DD format)
        max_dt: End date (YYYY-MM-DD format)  
        top_n_contracts_per_chain: Number of top contracts per chain
        min_usd_per_day_threshold: Minimum USD threshold per day
        chunk_days: Number of days per chunk to avoid query timeouts
        force_complete: If True, re-process all chunks even if data exists
    """
    chunks = _create_date_chunks(min_dt, max_dt, chunk_days)
    
    log.info(f"Processing {len(chunks)} chunks of ~{chunk_days} days each")
    
    chunks_processed = 0
    chunks_skipped = 0
    total_rows_written = 0
    all_written_dfs = []  # Keep track for summary
    
    for i, (chunk_start, chunk_end) in enumerate(chunks):
        # Check if chunk already has data (unless force_complete is True)
        if not force_complete and _chunk_has_data(chunk_start, chunk_end):
            log.info(f"Chunk {i+1}/{len(chunks)}: {chunk_start} to {chunk_end} - SKIPPED (data already exists)")
            chunks_skipped += 1
            continue
            
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
                # Validate schema before writing
                raise_for_schema_mismatch(
                    actual_schema=result.df.schema,
                    expected_schema=TOP_CONTRACTS_SCHEMA,
                )
                
                # Write this chunk immediately to GCS
                Dune.TOP_CONTRACTS.write(
                    dataframe=result.df,
                    sort_by=["dt", "chain_id", "rank_within_chain"],
                )
                total_rows_written += result.df.shape[0]
                all_written_dfs.append(result.df)
                log.info(f"Chunk {i+1} completed and written: {result.df.shape[0]} rows")
            else:
                log.info(f"Chunk {i+1} completed: No data returned")
            
            chunks_processed += 1
                
        except Exception as e:
            log.error(f"Failed to process chunk {i+1} ({chunk_start} to {chunk_end}): {e}")
            raise
    
    # Create BigQuery external table if any data was written
    if total_rows_written > 0:
        try:
            Dune.TOP_CONTRACTS.create_bigquery_external_table()
            log.info("Created BigQuery external table")
        except Exception as e:
            log.warning(f"Failed to create BigQuery external table: {e}")
    
    log.info(f"Processing complete: {chunks_processed} chunks processed, {chunks_skipped} chunks skipped, {total_rows_written:,} total rows written")

    # Generate summary following the pattern from other datasources
    summary = {
        "chunks_processed": chunks_processed,
        "chunks_skipped": chunks_skipped,
        "total_chunks": len(chunks),
        "total_rows_written": total_rows_written,
    }
    
    # Add data summary if we have written data
    if all_written_dfs:
        combined_df = pl.concat(all_written_dfs, how="vertical")
        summary["data_summary"] = dt_summary(combined_df)
        summary["date_range"] = f"{combined_df['dt'].min()} to {combined_df['dt'].max()}"
        summary["unique_chains"] = len(combined_df["chain"].unique())
        summary["unique_contracts"] = len(combined_df["contract_address"].unique())

    return summary 
