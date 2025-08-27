"""
Configuration module for Jovian calldata analysis.

This module contains all configuration parameters and constants used throughout
the Jovian analysis framework.
"""

from dataclasses import dataclass
from typing import List, Optional
from pathlib import Path
from .constants import (
    DEFAULT_CALLDATA_FOOTPRINT_GAS_SCALARS,
    DEFAULT_BLOCK_GAS_LIMIT,
    FASTLZ_INTERCEPT,
    FASTLZ_COEFFICIENT,
    DYNAMIC_LIMIT_DIVISOR,
    FULLNESS_PERCENTILE_THRESHOLD,
    MULTIPROCESSING_THRESHOLD,
    NUM_DOWNLOAD_WORKERS,
    NUM_FILE_WORKERS
)


@dataclass
class JovianConfig:
    """Configuration parameters for Jovian calldata analysis.

    Attributes:
        min_transaction_size: Minimum transaction size in gas units
        intercept: Jovian intercept parameter for size calculation
        fastlz_coef: FastLZ compression coefficient
        block_gas_limit: Maximum gas limit per block
        default_calldata_footprint_gas_scalars: Default scalars to test for calldata footprint
        data_dir: Directory for storing block data
    """
    min_transaction_size: int = 100
    intercept: int = FASTLZ_INTERCEPT
    fastlz_coef: int = FASTLZ_COEFFICIENT
    block_gas_limit: int = DEFAULT_BLOCK_GAS_LIMIT
    default_calldata_footprint_gas_scalars: Optional[List[int]] = None
    data_dir: Optional[Path] = None

    def __post_init__(self):
        """Initialize default values after dataclass initialization."""
        if self.default_calldata_footprint_gas_scalars is None:
            self.default_calldata_footprint_gas_scalars = DEFAULT_CALLDATA_FOOTPRINT_GAS_SCALARS

        if self.data_dir is None:
            self.data_dir = Path("block_data")
        elif isinstance(self.data_dir, str):
            self.data_dir = Path(self.data_dir)


@dataclass
class AnalysisConfig:
    """Configuration for analysis operations.

    Attributes:
        use_multiprocessing: Enable multiprocessing for large datasets
        multiprocessing_threshold: Minimum blocks to trigger multiprocessing
        use_cache: Use cached data when available
        progress_bars: Show progress bars during processing
        max_workers: Maximum number of parallel workers (None for auto)
        num_download_workers: Number of workers for downloading files
        num_file_workers: Number of workers for reading local files
        num_analysis_workers: Number of workers for block analysis (None for cpu_count)
        default_sample_blocks: Default number of blocks to sample (None for all)
        fullness_percentile_threshold: Percentile for identifying fullest blocks
        dynamic_limit_divisor: Divisor for dynamic gas limit calculation
    """
    use_multiprocessing: bool = True
    multiprocessing_threshold: int = MULTIPROCESSING_THRESHOLD
    use_cache: bool = True
    progress_bars: bool = True
    max_workers: Optional[int] = None  # Deprecated, use specific worker configs
    num_download_workers: int = NUM_DOWNLOAD_WORKERS  # Workers for GCS downloads
    num_file_workers: int = NUM_FILE_WORKERS  # Workers for local file reads
    num_analysis_workers: Optional[int] = None  # Workers for analysis (None = cpu_count)
    default_sample_blocks: Optional[int] = None  # None means analyze all blocks
    fullness_percentile_threshold: int = FULLNESS_PERCENTILE_THRESHOLD  # Top 1% fullest blocks
    dynamic_limit_divisor: int = DYNAMIC_LIMIT_DIVISOR  # For gas_limit / divisor calculation


# Default configurations
DEFAULT_JOVIAN_CONFIG = JovianConfig()
DEFAULT_ANALYSIS_CONFIG = AnalysisConfig()
