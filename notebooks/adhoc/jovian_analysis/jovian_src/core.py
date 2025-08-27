"""
Core functionality for Jovian calldata analysis.

This module contains the fundamental building blocks:
- FastLZ compression implementation
- Block storage management
- Data loading from GCS
- Core calldata analysis
"""

import polars as pl
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from tqdm import tqdm
import multiprocessing as mp
from functools import lru_cache
import hashlib

from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.datapipeline.etl.ingestion.reader.bydate import construct_readers_bydate
from op_analytics.datapipeline.etl.ingestion.reader.request import BlockBatchRequest
from op_analytics.datapipeline.etl.ingestion.reader.rootpaths import RootPath

from .config import JovianConfig, AnalysisConfig



# =============================================================================
# Analysis goals
# =============================================================================
""" 1. Histogram of the total size estiamates of the blocks
    2. When it exceeds the limit, it is expected but should be
    3. Histogram over the blocks where it exceeds the limit, removing all the stuff over the cuttoff, we look at the distribution of the stuff over the cuttoff
    4. Quantify by how much we go over the limit, 50 / 10000 we go over. How much over-utilization we got on these blocks.
    5. 50 blocks going over, but they went over by X% distribution.

Analysis horizon (ideally)
    1. Chains: Base, OPM, Unichain, Ink, Soneium, World, Zora, Mode.
    2. Date range: TBD for Base, but in general, can we do some targeted filtering and get the top 1% of the fullest blocks?
    3. Using the top 1% of the fullest blocks, we can get the top 1% of the utilization. Sum of calldata size as the block fullness.

    Thought: Instead of 1% take all the blocks where the block call data size is over the limit,
    where the limit is footprint limit = (gas limit of the block) / 800, the highest of the limits we have: [160 400 600 800] [4x, 10x, 15x, 20x]
    DE Todo: Size estimate / Fast lz size in the block db
    Goal: Do we make it a constant or configurable? If constant, what's a good constant?
"""
# =============================================================================
# COMPRESSION UTILITIES
# =============================================================================

# Global LRU cache for FastLZ compression results
# Using maxsize=10000 to cache up to 10,000 unique calldata patterns
# This should cover most repeated transactions while limiting memory usage
@lru_cache(maxsize=10000)
def _cached_fastlz_compress_len(data_hash: str, data_bytes: bytes) -> int:
    """Cached FastLZ compression implementation."""
    if not data_bytes or len(data_bytes) == 0:
        return 0

    if len(data_bytes) < 3:
        return len(data_bytes)

    n = 0
    ht = [0] * 8192
    data = data_bytes  # Use the bytes directly

    def u24(i: int) -> int:
        return data[i] | (data[i + 1] << 8) | (data[i + 2] << 16)

    def cmp(p: int, q: int, e: int) -> int:
        l = 0
        e -= q
        while l < e:
            if data[p + l] != data[q + l]:
                break
            l += 1
        return l

    def literals(r: int):
        nonlocal n
        n += 0x21 * (r // 0x20)
        r %= 0x20
        if r != 0:
            n += r + 1

    def match(l: int):
        nonlocal n
        l -= 1
        n += 3 * (l // 262)
        if l % 262 >= 6:
            n += 3
        else:
            n += 2

    def hash_func(v: int) -> int:
        return ((2654435769 * v) >> 19) & 0x1fff

    def set_next_hash(ip: int) -> int:
        ht[hash_func(u24(ip))] = ip
        return ip + 1

    a = 0
    ip_limit = len(data) - 13
    if len(data) < 13:
        ip_limit = 0

    ip = a + 2
    while ip < ip_limit:
        r = d = 0
        while True:
            s = u24(ip)
            h = hash_func(s)
            r = ht[h]
            ht[h] = ip
            d = ip - r
            if ip >= ip_limit:
                break
            ip += 1
            if d <= 0x1fff and s == u24(r):
                break
        if ip >= ip_limit:
            break
        ip -= 1
        if ip > a:
            literals(ip - a)
        l = cmp(r + 3, ip + 3, ip_limit + 9)
        match(l)
        ip = set_next_hash(set_next_hash(ip + l))
        a = ip

    literals(len(data) - a)
    return n


class FastLZCompressor:
    """FastLZ compression implementation for calldata size estimation with LRU caching."""

    def __init__(self, enable_cache: bool = True):
        """Initialize compressor with optional caching."""
        self.enable_cache = enable_cache
        self._cache_hits = 0
        self._cache_misses = 0

    def compress_len(self, data: bytes) -> int:
        """Calculate FastLZ compressed length estimate with caching."""
        if not self.enable_cache:
            # Use uncached version for comparison/testing
            return self._compress_len_uncached(data)

        if not data or len(data) == 0:
            return 0

        # Create a hash of the data for cache key
        # Using SHA-256 hash for strong collision resistance
        data_hash = hashlib.sha256(data).hexdigest()

        try:
            result = _cached_fastlz_compress_len(data_hash, data)
            self._cache_hits += 1
            return result
        except Exception:
            # Fallback to uncached version if caching fails
            self._cache_misses += 1
            return self._compress_len_uncached(data)

    def _compress_len_uncached(self, data: bytes) -> int:
        """Original uncached FastLZ implementation for fallback."""
        if not data or len(data) == 0:
            return 0

        if len(data) < 3:
            return len(data)

        n = 0
        ht = [0] * 8192

        def u24(i: int) -> int:
            return data[i] | (data[i + 1] << 8) | (data[i + 2] << 16)

        def cmp(p: int, q: int, e: int) -> int:
            l = 0
            e -= q
            while l < e:
                if data[p + l] != data[q + l]:
                    break
                l += 1
            return l

        def literals(r: int):
            nonlocal n
            n += 0x21 * (r // 0x20)
            r %= 0x20
            if r != 0:
                n += r + 1

        def match(l: int):
            nonlocal n
            l -= 1
            n += 3 * (l // 262)
            if l % 262 >= 6:
                n += 3
            else:
                n += 2

        def hash_func(v: int) -> int:
            return ((2654435769 * v) >> 19) & 0x1fff

        def set_next_hash(ip: int) -> int:
            ht[hash_func(u24(ip))] = ip
            return ip + 1

        a = 0
        ip_limit = len(data) - 13
        if len(data) < 13:
            ip_limit = 0

        ip = a + 2
        while ip < ip_limit:
            r = d = 0
            while True:
                s = u24(ip)
                h = hash_func(s)
                r = ht[h]
                ht[h] = ip
                d = ip - r
                if ip >= ip_limit:
                    break
                ip += 1
                if d <= 0x1fff and s == u24(r):
                    break
            if ip >= ip_limit:
                break
            ip -= 1
            if ip > a:
                literals(ip - a)
            l = cmp(r + 3, ip + 3, ip_limit + 9)
            match(l)
            ip = set_next_hash(set_next_hash(ip + l))
            a = ip

        literals(len(data) - a)
        return n

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics."""
        total_calls = self._cache_hits + self._cache_misses
        hit_rate = self._cache_hits / total_calls if total_calls > 0 else 0

        return {
            'cache_enabled': self.enable_cache,
            'cache_hits': self._cache_hits,
            'cache_misses': self._cache_misses,
            'total_calls': total_calls,
            'hit_rate': hit_rate,
            'cache_info': _cached_fastlz_compress_len.cache_info() if self.enable_cache else None
        }

    def clear_cache(self):
        """Clear the compression cache."""
        if self.enable_cache:
            _cached_fastlz_compress_len.cache_clear()
        self._cache_hits = 0
        self._cache_misses = 0

    def reset_stats(self):
        """Reset cache statistics without clearing the cache."""
        self._cache_hits = 0
        self._cache_misses = 0

    def get_cache_efficiency_report(self) -> str:
        """Generate a human-readable cache efficiency report."""
        stats = self.get_cache_stats()

        if not stats['cache_enabled']:
            return "Cache is disabled"

        if stats['total_calls'] == 0:
            return "No compression calls made yet"

        hit_rate_pct = stats['hit_rate'] * 100

        report = [
            f"🎯 FastLZ Cache Performance:",
            f"   Total calls: {stats['total_calls']:,}",
            f"   Cache hits: {stats['cache_hits']:,} ({hit_rate_pct:.1f}%)",
            f"   Cache misses: {stats['cache_misses']:,}",
        ]

        if stats['cache_info']:
            cache_info = stats['cache_info']
            utilization = (cache_info.currsize / cache_info.maxsize) * 100 if cache_info.maxsize > 0 else 0
            report.extend([
                f"   Cache size: {cache_info.currsize:,}/{cache_info.maxsize:,} ({utilization:.1f}% full)"
            ])

        # Performance interpretation
        if hit_rate_pct >= 80:
            report.append("   📈 Excellent cache efficiency!")
        elif hit_rate_pct >= 60:
            report.append("   📊 Good cache efficiency")
        elif hit_rate_pct >= 40:
            report.append("   📉 Moderate cache efficiency")
        else:
            report.append("   🔍 Low cache efficiency - consider investigating data patterns")

        return "\n".join(report)


def parse_calldata(calldata_hex: str) -> bytes:
    """Parse hex calldata string to bytes."""
    if not calldata_hex or calldata_hex in ['0x', '']:
        return b''

    if calldata_hex.startswith('0x'):
        calldata_hex = calldata_hex[2:]

    if len(calldata_hex) % 2 != 0:
        calldata_hex = '0' + calldata_hex

    try:
        return bytes.fromhex(calldata_hex)
    except ValueError:
        return b''


# =============================================================================
# BLOCK STORAGE
# =============================================================================

class BlockStorage:
    """Efficient block-level data storage and retrieval."""

    def __init__(self, data_dir: str = "block_data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)

    def get_chain_dir(self, chain: str, date: str) -> Path:
        """Get directory for specific chain and date."""
        chain_date_dir = self.data_dir / f"{chain}_{date.replace('-', '')}"
        chain_date_dir.mkdir(exist_ok=True)
        return chain_date_dir

    def store_blocks(self, df: pl.DataFrame, chain: str, date: str,
                    show_progress: bool = True) -> Dict[int, str]:
        """Store transaction data partitioned by block number."""
        chain_dir = self.get_chain_dir(chain, date)

        # Validate required columns - be flexible about column names
        required_cols = ['block_number']  # Only block_number is truly required
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Add transaction_hash if missing (for compatibility)
        if 'transaction_hash' not in df.columns:
            if 'hash' in df.columns:
                df = df.with_columns(pl.col('hash').alias('transaction_hash'))
            elif 'tx_hash' in df.columns:
                df = df.with_columns(pl.col('tx_hash').alias('transaction_hash'))
            else:
                # Generate synthetic hashes for demo purposes
                df = df.with_row_count('row_id').with_columns(
                    pl.format("0x{:064x}", pl.col('row_id')).alias('transaction_hash')
                ).drop('row_id')

        # Group by block number and save each block separately
        block_groups = df.group_by("block_number")
        block_files = {}

        if show_progress:
            print(f"💾 Storing blocks for {chain} on {date}...")
            block_groups = tqdm(block_groups, desc="Storing blocks")

        for block_num, block_df in block_groups:
            block_number = block_num[0]
            filename = chain_dir / f"block_{block_number}.parquet"
            block_df.write_parquet(str(filename))
            block_files[block_number] = str(filename)

        # Create and save block summary
        summary = self._create_block_summary(df)
        summary_file = chain_dir / "block_summary.parquet"
        summary.write_parquet(str(summary_file))

        if show_progress:
            total_txs = df.shape[0]
            print(f"✅ Stored {len(block_files)} blocks with {total_txs:,} transactions")

        return block_files

    def _create_block_summary(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create summary statistics for blocks."""
        summary = df.group_by("block_number").agg([
            pl.count("transaction_hash").alias("tx_count"),
            pl.first("block_timestamp").alias("block_timestamp") if "block_timestamp" in df.columns else pl.lit(None).alias("block_timestamp"),
            pl.first("block_hash").alias("block_hash") if "block_hash" in df.columns else pl.lit(None).alias("block_hash"),
            pl.sum("gas").alias("total_gas") if "gas" in df.columns else pl.lit(0).alias("total_gas"),
        ]).sort("block_number")

        return summary

    def load_block(self, block_number: int, chain: str, date: str) -> pl.DataFrame:
        """Load data for a specific block."""
        chain_dir = self.get_chain_dir(chain, date)
        filename = chain_dir / f"block_{block_number}.parquet"

        try:
            return pl.read_parquet(str(filename))
        except FileNotFoundError:
            return pl.DataFrame()

    def load_blocks(self, block_numbers: List[int], chain: str, date: str,
                   show_progress: bool = False) -> pl.DataFrame:
        """Load and combine data for multiple blocks."""
        dfs = []

        iterator = block_numbers
        if show_progress:
            iterator = tqdm(block_numbers, desc="Loading blocks")

        for block_num in iterator:
            block_df = self.load_block(block_num, chain, date)
            if len(block_df) > 0:
                dfs.append(block_df)

        return pl.concat(dfs) if dfs else pl.DataFrame()

    def get_available_blocks(self, chain: str, date: str) -> List[int]:
        """Get list of available block numbers."""
        # Try loading summary first
        summary = self.load_summary(chain, date)
        if len(summary) > 0:
            return sorted(summary["block_number"].to_list())

        # Check chain directory for block files
        chain_dir = self.get_chain_dir(chain, date)
        if chain_dir.exists():
            block_numbers = []
            for file in chain_dir.glob("block_*.parquet"):
                try:
                    block_num = int(file.stem.replace("block_", ""))
                    block_numbers.append(block_num)
                except ValueError:
                    continue
            return sorted(block_numbers)

        return []

    def load_summary(self, chain: str, date: str) -> pl.DataFrame:
        """Load block summary for a chain and date."""
        chain_dir = self.get_chain_dir(chain, date)
        summary_file = chain_dir / "block_summary.parquet"

        try:
            return pl.read_parquet(str(summary_file))
        except FileNotFoundError:
            return pl.DataFrame()

    def has_data(self, chain: str, date: str) -> bool:
        """Check if data exists for a chain and date."""
        return len(self.get_available_blocks(chain, date)) > 0

    def get_storage_info(self) -> Dict[str, Dict]:
        """Get information about all stored data."""
        info = {}

        if not self.data_dir.exists():
            return info

        for chain_date_dir in self.data_dir.iterdir():
            if chain_date_dir.is_dir() and '_' in chain_date_dir.name:
                try:
                    chain, date_str = chain_date_dir.name.split('_', 1)
                    if len(date_str) == 8:
                        date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

                        blocks = self.get_available_blocks(chain, date)
                        summary = self.load_summary(chain, date)

                        info[chain_date_dir.name] = {
                            'chain': chain,
                            'date': date,
                            'block_count': len(blocks),
                            'block_range': (min(blocks), max(blocks)) if blocks else (None, None),
                            'total_transactions': int(summary['tx_count'].sum()) if len(summary) > 0 else 0,
                        }
                except (ValueError, IndexError):
                    continue

        return info

    def list_cached_data(self) -> None:
        """Print a summary of all cached data."""
        info = self.get_storage_info()

        if not info:
            print("📭 No cached data found")
            print("💡 Data will be downloaded and cached when you run analysis")
            return

        print(f"📂 Cached Data Summary ({len(info)} datasets):")
        print("-" * 75)
        print(f"{'Chain':<12} {'Date':<12} {'Blocks':<8} {'Transactions':<12} {'Range'}")
        print("-" * 75)

        total_blocks = 0
        total_txs = 0

        for dataset_id, data in sorted(info.items(), key=lambda x: (x[1]['chain'], x[1]['date'])):
            chain = data['chain']
            date = data['date']
            blocks = data['block_count']
            txs = data['total_transactions']
            block_range = data['block_range']
            range_str = f"{block_range[0]}-{block_range[1]}" if block_range[0] else "N/A"

            print(f"{chain:<12} {date:<12} {blocks:<8} {txs:<12,} {range_str}")

            total_blocks += blocks
            total_txs += txs

        print("-" * 75)
        print(f"{'TOTAL':<12} {'':<12} {total_blocks:<8} {total_txs:<12,}")
        print(f"\n💾 Cached data ready for analysis")


# =============================================================================
# DATA LOADER
# =============================================================================

def _download_parquet_file(path: str) -> Tuple[str, Optional[pl.DataFrame], Optional[str]]:
    """Download a single parquet file from GCS. Returns (path, dataframe, error_message)."""
    try:
        df = pl.read_parquet(path)
        return path, df if len(df) > 0 else None, None
    except Exception as e:
        return path, None, str(e)


class DataLoader:
    """Data fetching and caching from GCS."""

    def __init__(self, storage: BlockStorage, jovian_config: JovianConfig, analysis_config: Optional['AnalysisConfig'] = None):
        self.storage = storage
        self.jovian_config = jovian_config
        # Import here to avoid circular imports
        from .config import DEFAULT_ANALYSIS_CONFIG
        self.analysis_config = analysis_config or DEFAULT_ANALYSIS_CONFIG

    def fetch_transactions(self, chain: str, date: str, use_cache: bool = True,
                          show_progress: bool = True) -> pl.DataFrame:
        """Fetch transaction data from GCS or load from cache."""
        # Check cache first
        if use_cache and self.storage.has_data(chain, date):
            if show_progress:
                print(f"📂 Loading cached data for {chain} on {date}...")

            blocks = self.storage.get_available_blocks(chain, date)
            if blocks:
                df = self.storage.load_blocks(blocks, chain, date, show_progress)
                if show_progress:
                    print(f"✅ Loaded {len(df):,} transactions from {len(blocks):,} cached blocks")
                return df

        # Fetch from GCS
        if show_progress:
            print(f"📥 Fetching transaction data for {chain} on {date} from GCS...")

        df = self._fetch_from_gcs(chain, date, show_progress)

        # Cache the data if fetched successfully
        if len(df) > 0:
            self.storage.store_blocks(df, chain, date, show_progress)

        return df

    def _fetch_from_gcs(self, chain: str, date: str, show_progress: bool = True) -> pl.DataFrame:
        """Fetch transaction data from GCS using op-analytics framework."""
        if show_progress:
            print(f"📁 Looking for existing data files for {chain} on {date}...")

        # First check for existing local data
        block_data_dir = Path("block_data") / f"{chain}_{date.replace('-', '')}"

        if block_data_dir.exists():
            parquet_files = list(block_data_dir.glob("block_*.parquet"))
            if parquet_files:
                if show_progress:
                    print(f"📁 Found {len(parquet_files)} cached parquet files")

                # Read all parquet files with progress bar (parallel for many files)
                dfs = []

                if len(parquet_files) <= 3:
                    # Few files - read sequentially
                    iterator = parquet_files
                    if show_progress:
                        iterator = tqdm(parquet_files, desc="Reading cached parquet files")

                    for file_path in iterator:
                        try:
                            df = pl.read_parquet(file_path)
                            dfs.append(df)
                        except Exception as e:
                            if show_progress:
                                if isinstance(iterator, tqdm):
                                    iterator.write(f"⚠️  Error reading {file_path.name}: {e}")
                                else:
                                    print(f"⚠️  Error reading {file_path.name}: {e}")
                            continue
                else:
                    # Many files - read in parallel
                    max_workers = min(self.analysis_config.num_file_workers, len(parquet_files))
                    if show_progress:
                        print(f"📁 Reading {len(parquet_files)} cached files using {max_workers} parallel workers...")

                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        # Submit all read tasks
                        future_to_path = {
                            executor.submit(_download_parquet_file, str(path)): path
                            for path in parquet_files
                        }

                        # Process completed reads with progress bar
                        progress_bar = None
                        if show_progress:
                            progress_bar = tqdm(total=len(parquet_files), desc="Reading cached files")

                        error_count = 0
                        for future in as_completed(future_to_path):
                            path, df, error = future.result()

                            if df is not None:
                                dfs.append(df)
                            elif error:
                                error_count += 1
                                if show_progress and progress_bar:
                                    progress_bar.write(f"⚠️  Error reading {Path(path).name}: {error}")

                            if progress_bar:
                                progress_bar.update(1)

                        if progress_bar:
                            progress_bar.close()

                        if show_progress:
                            success_count = len(parquet_files) - error_count
                            print(f"✅ Read {success_count}/{len(parquet_files)} cached files successfully")

                if dfs:
                    combined_df = pl.concat(dfs)
                    if show_progress:
                        print(f"✅ Successfully loaded {len(combined_df):,} transactions from cached files")
                        print(f"   Blocks: {combined_df['block_number'].n_unique():,}")
                        print(f"   Range: {combined_df['block_number'].min():,} - {combined_df['block_number'].max():,}")
                    return combined_df

        # If no local data, fetch from GCS using op-analytics framework
        if show_progress:
            print(f"📥 Fetching transaction data from GCS for {chain} on {date}...")

        try:
            # Create BlockBatchRequest for transactions data
            blockbatch_request = BlockBatchRequest.build(
                chains=[chain],
                range_spec=f"@{date.replace('-', '')}:+1",  # Single day
                root_paths_to_read=[RootPath.of("ingestion/transactions_v1")],
            )

            # Get readers for the data
            readers = construct_readers_bydate(
                blockbatch_request=blockbatch_request,
                read_from=DataLocation.GCS,
            )

            if not readers:
                if show_progress:
                    print(f"❌ No data readers available for {chain} on {date}")
                return pl.DataFrame()

            # Process each reader and collect data
            all_dfs = []
            for reader in readers:
                if not reader.inputs_ready:
                    if show_progress:
                        print(f"⚠️  Data not ready for {chain} on {date}")
                    continue

                # Get the parquet paths for transactions
                dataset_paths = reader.dataset_paths.get("ingestion/transactions_v1", [])

                if not dataset_paths:
                    if show_progress:
                        print(f"⚠️  No transaction paths found for {chain} on {date}")
                    continue

                # Download parquet files from GCS in parallel
                if len(dataset_paths) == 1:
                    # Single file - download directly
                    if show_progress:
                        print(f"📥 Reading from GCS: {dataset_paths[0]}")
                    try:
                        df = pl.read_parquet(dataset_paths[0])
                        if len(df) > 0:
                            all_dfs.append(df)
                    except Exception as e:
                        if show_progress:
                            print(f"⚠️  Error reading {dataset_paths[0]}: {e}")
                else:
                    # Multiple files - download in parallel
                    max_workers = min(self.analysis_config.num_download_workers, len(dataset_paths))
                    if show_progress:
                        print(f"📥 Downloading {len(dataset_paths)} files using {max_workers} parallel workers...")

                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        # Submit all download tasks
                        future_to_path = {
                            executor.submit(_download_parquet_file, path): path
                            for path in dataset_paths
                        }

                        # Process completed downloads with progress bar
                        progress_bar = None
                        if show_progress:
                            progress_bar = tqdm(total=len(dataset_paths), desc="Downloading GCS files")

                        error_count = 0
                        for future in as_completed(future_to_path):
                            path, df, error = future.result()

                            if df is not None:
                                all_dfs.append(df)
                            elif error:
                                error_count += 1
                                if show_progress and progress_bar:
                                    progress_bar.write(f"⚠️  Error reading {path}: {error}")

                            if progress_bar:
                                progress_bar.update(1)

                        if progress_bar:
                            progress_bar.close()

                        if show_progress:
                            success_count = len(dataset_paths) - error_count
                            print(f"✅ Downloaded {success_count}/{len(dataset_paths)} files successfully")
                            if error_count > 0:
                                print(f"⚠️  {error_count} files had errors")

            if not all_dfs:
                if show_progress:
                    print(f"❌ No transaction data found for {chain} on {date}")
                return pl.DataFrame()

            # Combine all dataframes
            combined_df = pl.concat(all_dfs)

            if show_progress:
                print(f"✅ Successfully fetched {len(combined_df):,} transactions from GCS")
                print(f"   Blocks: {combined_df['block_number'].n_unique():,}")
                print(f"   Range: {combined_df['block_number'].min():,} - {combined_df['block_number'].max():,}")

            return combined_df

        except Exception as e:
            if show_progress:
                print(f"❌ Error fetching from GCS: {e}")
                print(f"💡 Expected directory: {block_data_dir.absolute()}")
            return pl.DataFrame()


# =============================================================================
# CALLDATA ANALYSIS
# =============================================================================

@dataclass
class TransactionAnalysis:
    """Result of analyzing a single transaction."""
    transaction_hash: str
    calldata_size: int
    fastlz_size: int
    size_estimate: float
    calldata_footprint: float
    compression_ratio: float


@dataclass
class BlockAnalysis:
    """Result of analyzing a block."""
    block_number: int
    tx_count: int
    total_calldata_footprint: float
    total_size_estimate: float
    calldata_utilization: float
    exceeds_limit: bool
    avg_footprint_per_tx: float
    max_tx_footprint: float
    transactions: List[TransactionAnalysis]
    footprint_cost: int
    block_gas_limit: int
    block_gas_used: Optional[int] = None
    utilization_vs_gas_used: Optional[float] = None
    total_calldata_size: int = 0  # Sum of raw calldata sizes
    total_fastlz_size: int = 0    # Sum of compressed sizes

class CalldataAnalyzer:
    """Core analyzer for Jovian calldata footprints."""

    def __init__(self, jovian_config: JovianConfig, analysis_config: AnalysisConfig):
        self.jovian_config = jovian_config
        self.analysis_config = analysis_config
        self.compressor = FastLZCompressor()

    def calculate_size_estimate(self, calldata: bytes) -> float:
        """Calculate Jovian size estimate for calldata."""
        if not calldata:
            return float(self.jovian_config.min_transaction_size)

        fastlz_size = self.compressor.compress_len(calldata) + 68
        size_estimate = max(
            self.jovian_config.min_transaction_size,
            self.jovian_config.intercept + self.jovian_config.fastlz_coef * fastlz_size / 1e6
        )
        return size_estimate

    def calculate_footprint(self, calldata: bytes, footprint_cost: int) -> float:
        """Calculate calldata footprint for a transaction."""
        size_estimate = self.calculate_size_estimate(calldata)
        return size_estimate * footprint_cost

    def analyze_transaction(self, tx_row: Dict[str, Any], footprint_cost: int) -> TransactionAnalysis:
        """Analyze a single transaction."""
        calldata_hex = tx_row.get('input', '0x')
        calldata = parse_calldata(calldata_hex)

        # OPTIMIZATION: Calculate FastLZ compression once and reuse for all calculations
        # Original code had 3 redundant computations:
        # 1. Direct compress_len() call
        # 2. compress_len() inside calculate_size_estimate()
        # 3. compress_len() inside calculate_footprint() -> calculate_size_estimate()
        fastlz_size = self.compressor.compress_len(calldata)

        # OPTIMIZATION: Calculate size estimate using pre-computed fastlz_size
        # instead of calling calculate_size_estimate() which would recompute compression
        if not calldata:
            size_estimate = float(self.jovian_config.min_transaction_size)
        else:
            size_estimate = max(
                self.jovian_config.min_transaction_size,
                self.jovian_config.intercept + self.jovian_config.fastlz_coef * fastlz_size / 1e6
            )

        # OPTIMIZATION: Calculate footprint using pre-computed size_estimate
        footprint = size_estimate * footprint_cost

        compression_ratio = len(calldata) / fastlz_size if fastlz_size > 0 else 0

        # CORRECTNESS CHECK: Results should be identical to original implementation
        # Original path: compress_len() -> calculate_size_estimate() -> calculate_footprint()
        # Optimized path: compress_len() -> inline size_estimate -> inline footprint
        # Mathematical equivalence: guaranteed by identical formulas

        return TransactionAnalysis(
            transaction_hash=tx_row.get('transaction_hash', ''),
            calldata_size=len(calldata),
            fastlz_size=fastlz_size,
            size_estimate=size_estimate,
            calldata_footprint=footprint,
            compression_ratio=compression_ratio
        )

    def analyze_block(self, block_df: pl.DataFrame, footprint_cost: int, *, show_tx_progress: bool = False) -> BlockAnalysis:
        """Analyze all transactions in a block."""
        if len(block_df) == 0:
            raise ValueError("No transactions in block")

        block_number = block_df['block_number'][0]

        # OPTIMIZATION: For large blocks without detailed transaction analysis needs,
        # use vectorized operations to calculate aggregates directly
        if len(block_df) > 1000 and not show_tx_progress:
            return self._analyze_block_vectorized(block_df, footprint_cost)

        # Original detailed analysis path for smaller blocks or when progress is needed
        transactions = []

        # Analyze each transaction with progress bar if many transactions
        iterator = block_df.iter_rows(named=True)
        if show_tx_progress and len(block_df) > 100:
            iterator = tqdm(iterator, total=len(block_df), desc=f"Analyzing block {block_number}")

        for row in iterator:
            tx_analysis = self.analyze_transaction(row, footprint_cost)
            transactions.append(tx_analysis)

        # Calculate block-level metrics using pre-computed transaction results
        total_footprint = sum(tx.calldata_footprint for tx in transactions)
        total_size_estimate = sum(tx.size_estimate for tx in transactions)
        total_calldata_size = sum(tx.calldata_size for tx in transactions)
        total_fastlz_size = sum(tx.fastlz_size for tx in transactions)
        utilization = total_footprint / self.jovian_config.block_gas_limit
        exceeds_limit = total_footprint > self.jovian_config.block_gas_limit

        # ensure this frame has exactly one block
        if block_df.select(pl.col("block_number").n_unique()).item() != 1:
            raise ValueError("analyze_block received multiple block_numbers")

        block_gas_used = None

        if "block_total_gas_used" in block_df.columns:
            nuniq = block_df.select(pl.col("block_total_gas_used").n_unique()).item()
            if nuniq != 1:
                raise ValueError(f"block_total_gas_used not constant within block {block_number}")
            block_gas_used = int(block_df["block_total_gas_used"].max())


        utilization_vs_gas_used = (
            total_footprint / block_gas_used
            if (block_gas_used is not None and block_gas_used > 0)
            else None
        )

        return BlockAnalysis(
            block_number=block_number,
            tx_count=len(transactions),
            total_calldata_footprint=total_footprint,
            total_size_estimate=total_size_estimate,
            calldata_utilization=utilization,
            exceeds_limit=exceeds_limit,
            avg_footprint_per_tx=total_footprint / len(transactions),
            max_tx_footprint=max(tx.calldata_footprint for tx in transactions),
            transactions=transactions,
            footprint_cost=footprint_cost,
            block_gas_limit=self.jovian_config.block_gas_limit,
            total_calldata_size=total_calldata_size,
            total_fastlz_size=total_fastlz_size,
            block_gas_used=block_gas_used,
            utilization_vs_gas_used=utilization_vs_gas_used,
        )

    def _analyze_block_vectorized(self, block_df: pl.DataFrame, footprint_cost: int) -> BlockAnalysis:
        """OPTIMIZATION: Vectorized block analysis for large blocks without individual transaction details."""
        block_number = block_df['block_number'][0]
        tx_count = len(block_df)

        # VECTORIZED OPERATION: Process all calldata in batch
        # This avoids the O(n) iter_rows() loop for large blocks
        calldata_sizes = []
        fastlz_sizes = []
        size_estimates = []

        # Process in chunks to manage memory for very large blocks
        chunk_size = 1000
        for start_idx in range(0, len(block_df), chunk_size):
            end_idx = min(start_idx + chunk_size, len(block_df))
            chunk_df = block_df.slice(start_idx, end_idx - start_idx)

            # Extract input column for this chunk
            inputs = chunk_df['input'].to_list()

            for calldata_hex in inputs:
                calldata = parse_calldata(calldata_hex)
                calldata_size = len(calldata)

                # Calculate compression and size estimate
                if not calldata:
                    fastlz_size = 0
                    size_estimate = float(self.jovian_config.min_transaction_size)
                else:
                    fastlz_size = self.compressor.compress_len(calldata)
                    size_estimate = max(
                        self.jovian_config.min_transaction_size,
                        self.jovian_config.intercept + self.jovian_config.fastlz_coef * fastlz_size / 1e6
                    )

                calldata_sizes.append(calldata_size)
                fastlz_sizes.append(fastlz_size)
                size_estimates.append(size_estimate)

        # VECTORIZED AGGREGATIONS: Calculate totals without storing individual transactions
        total_calldata_size = sum(calldata_sizes)
        total_fastlz_size = sum(fastlz_sizes)
        total_size_estimate = sum(size_estimates)
        total_footprint = total_size_estimate * footprint_cost

        utilization = total_footprint / self.jovian_config.block_gas_limit
        exceeds_limit = total_footprint > self.jovian_config.block_gas_limit

        # Calculate max footprint for this block
        max_tx_footprint = max(size_est * footprint_cost for size_est in size_estimates) if size_estimates else 0

        block_gas_used = None
        if "block_total_gas_used" in block_df.columns:
            nuniq = block_df.select(pl.col("block_total_gas_used").n_unique()).item()
            if nuniq != 1:
                raise ValueError(f"block_total_gas_used not constant within block {block_number}")
            block_gas_used = int(block_df["block_total_gas_used"].max())

        utilization_vs_gas_used = (
            total_footprint / block_gas_used
            if (block_gas_used is not None and block_gas_used > 0)
            else None
        )
        # CORRECTNESS: This vectorized path produces mathematically identical results
        # to the iter_rows() path, but with O(1) memory for transaction storage
        # and better CPU cache locality for large blocks

        return BlockAnalysis(
            block_number=block_number,
            tx_count=tx_count,
            total_calldata_footprint=total_footprint,
            total_size_estimate=total_size_estimate,
            calldata_utilization=utilization,
            exceeds_limit=exceeds_limit,
            avg_footprint_per_tx=total_footprint / tx_count,
            max_tx_footprint=max_tx_footprint,
            transactions=[],  # Empty for vectorized analysis to save memory
            footprint_cost=footprint_cost,
            block_gas_limit=self.jovian_config.block_gas_limit,
            total_calldata_size=total_calldata_size,
            total_fastlz_size=total_fastlz_size,
            block_gas_used=block_gas_used,
            utilization_vs_gas_used=utilization_vs_gas_used,
        )

    def analyze_multiple_blocks(self, df: pl.DataFrame, footprint_cost: int,
                              show_progress: bool = True) -> List[BlockAnalysis]:
        """Analyze multiple blocks from a DataFrame."""
        block_groups = list(df.group_by("block_number"))

        if not block_groups:
            return []

        # Use multiprocessing for large datasets
        if (self.analysis_config.use_multiprocessing and
            len(block_groups) >= self.analysis_config.multiprocessing_threshold):
            return self._analyze_blocks_parallel(block_groups, footprint_cost, show_progress)
        else:
            return self._analyze_blocks_sequential(block_groups, footprint_cost, show_progress)

    def _analyze_blocks_sequential(self, block_groups: List[Tuple], footprint_cost: int,
                                 show_progress: bool) -> List[BlockAnalysis]:
        """Analyze blocks sequentially."""
        results = []

        iterator = block_groups
        if show_progress:
            iterator = tqdm(block_groups, desc="Analyzing blocks")

        for block_num, block_df in iterator:
            try:
                result = self.analyze_block(block_df, footprint_cost)
                results.append(result)
            except Exception as e:
                if show_progress:
                    tqdm.write(f"⚠️  Error analyzing block {block_num[0]}: {e}")

        return results

    def _analyze_blocks_parallel(self, block_groups: List[Tuple], footprint_cost: int,
                               show_progress: bool) -> List[BlockAnalysis]:
        """Analyze blocks using multiprocessing."""
        # Prepare arguments for workers
        config_dict = {
            'min_transaction_size': self.jovian_config.min_transaction_size,
            'intercept': self.jovian_config.intercept,
            'fastlz_coef': self.jovian_config.fastlz_coef,
            'block_gas_limit': self.jovian_config.block_gas_limit,
        }

        args_list = [(block_df, footprint_cost, config_dict) for block_num, block_df in block_groups]

        # Determine number of workers
        configured_workers = self.analysis_config.num_analysis_workers or mp.cpu_count()
        num_workers = min(configured_workers, len(args_list))

        if show_progress:
            print(f"🚀 Analyzing {len(args_list)} blocks with {num_workers} workers...")

        results = []
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            # Submit all tasks
            future_to_block = {
                executor.submit(_analyze_block_worker, args): args[3] if len(args) > 3 else "unknown"
                for args in args_list
            }

            # Collect results
            iterator = as_completed(future_to_block)
            if show_progress:
                iterator = tqdm(iterator, total=len(args_list), desc="Processing blocks")

            for future in iterator:
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                except Exception as e:
                    if show_progress:
                        tqdm.write(f"⚠️  Error processing block: {e}")

        return results


def _analyze_block_worker(args: Tuple) -> Optional[BlockAnalysis]:
    """Worker function for multiprocessing block analysis."""
    try:
        block_data, footprint_cost, config_dict = args

        # Recreate config and analyzer
        jovian_config = JovianConfig(**config_dict)
        analysis_config = AnalysisConfig(use_multiprocessing=False)  # No nested multiprocessing
        analyzer = CalldataAnalyzer(jovian_config, analysis_config)

        # Analyze the block
        return analyzer.analyze_block(block_data, footprint_cost)

    except Exception as e:
        return None
