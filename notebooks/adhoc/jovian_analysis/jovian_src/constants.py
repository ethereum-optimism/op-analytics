"""
Constants for Jovian calldata analysis.

This module centralizes all magic numbers and hardcoded values used throughout
the Jovian analysis framework.
"""

# =============================================================================
# GAS LIMITS
# =============================================================================

# Default gas limits
DEFAULT_GAS_LIMIT = 240_000_000
CHAIN_DEFAULT_GAS_LIMIT = 30_000_000
DEFAULT_BLOCK_GAS_LIMIT = 150_000_000

# =============================================================================
# CALLDATA FOOTPRINT GAS SCALARS
# =============================================================================

# Default DA footprint gas scalars to test
DEFAULT_DA_FOOTPRINT_GAS_SCALARS = [160, 400, 600, 800]

# Dynamic limit divisor for gas limit calculations
DYNAMIC_LIMIT_DIVISOR = 800

# =============================================================================
# FASTLZ COMPRESSION CONSTANTS
# =============================================================================

# FastLZ compression parameters
FASTLZ_INTERCEPT = -42_585_600
FASTLZ_COEFFICIENT = 836_500

# =============================================================================
# ANALYSIS THRESHOLDS
# =============================================================================

# Percentile threshold for identifying fullest blocks
FULLNESS_PERCENTILE_THRESHOLD = 99

# Multiprocessing thresholds
MULTIPROCESSING_THRESHOLD = 10

# =============================================================================
# WORKER CONFIGURATION
# =============================================================================

# Number of workers for different operations
NUM_DOWNLOAD_WORKERS = 8
NUM_FILE_WORKERS = 4

# =============================================================================
# VISUALIZATION CONSTANTS
# =============================================================================

# Histogram bins
HISTOGRAM_BINS = 50

# Plot figure sizes
FIGURE_SIZE_LARGE = (16, 7)
FIGURE_SIZE_MEDIUM = (12, 8)
FIGURE_SIZE_SMALL = (8, 6)

# Plot DPI
PLOT_DPI = 150
