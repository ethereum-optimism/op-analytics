"""
Jovian analysis functions leveraging the calculation engine from core.py.
Provides comprehensive analysis for multiple calldata footprint gas scalars and gas limits.
Includes compression ratio analysis and multi-chain support.
"""

import polars as pl
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path

from .core import (
    CalldataAnalyzer,
    BlockAnalysis,
    JovianConfig,
    AnalysisConfig,
    parse_calldata
)
from .clickhouse_fetcher import load_gas_limits, get_gas_limit_for_date
from .chain_config import get_chain_display_name
from .constants import DEFAULT_DA_FOOTPRINT_GAS_SCALARS


@dataclass
class JovianAnalysisResult:
    """Results from analyzing blocks with a specific DA footprint gas scalar."""
    da_footprint_gas_scalar: int
    gas_limit: int
    chain: str
    sampling_method: str  # "top_percentile" or "random"
    start_date: str  # Analysis start date
    end_date: str    # Analysis end date
    total_blocks: int
    blocks_exceeding: int
    percentage_exceeding: float
    avg_utilization: float
    max_utilization: float
    avg_excess_percentage: float
    max_excess_percentage: float
    block_analyses: List[BlockAnalysis]
    utilization_distribution: Dict[str, int]  # Bins for histogram
    excess_distribution: Optional[Dict[str, int]]  # For blocks that exceed
    compression_metrics: Dict[str, float]  # New: compression ratio metrics
    avg_util_vs_gas_used: float = 0.0
    median_util_vs_gas_used: float = 0.0
    p95_util_vs_gas_used: float = 0.0
    share_over_1_vs_used: float = 0.0
    blocks_with_gas_used: int = 0


def perform_jovian_analysis(
    df: pl.DataFrame,
    gas_limit: int,
    da_footprint_gas_scalars: List[int] = None,
    chain: str = "base",
    sampling_method: str = "top_percentile",
    start_date: str = None,
    end_date: str = None,
    show_progress: bool = True
) -> Dict[int, JovianAnalysisResult]:
    """
    Perform comprehensive Jovian analysis for multiple calldata footprint gas scalars.

    Args:
        df: Transaction data from blocks
        gas_limit: Gas limit for the period
        da_footprint_gas_scalars: List of scalars to test (default: [160, 400, 600, 800])
        chain: Chain name for context
        sampling_method: Method used to sample blocks
        start_date: Analysis start date (YYYY-MM-DD format)
        end_date: Analysis end date (YYYY-MM-DD format)
        show_progress: Show progress bars

    Returns:
        Dictionary mapping DA footprint gas scalar to analysis results
    """
    if da_footprint_gas_scalars is None:
        da_footprint_gas_scalars = DEFAULT_DA_FOOTPRINT_GAS_SCALARS

    # Create analyzer with appropriate config
    jovian_config = JovianConfig(block_gas_limit=gas_limit)
    analysis_config = AnalysisConfig(progress_bars=show_progress)
    analyzer = CalldataAnalyzer(jovian_config, analysis_config)

    results = {}

    # Add analysis summary header
    if show_progress:
        chain_display = get_chain_display_name(chain)
        date_range = f"{start_date} → {end_date}" if start_date and end_date else "date range not specified"
        print(f"\n🚀 JOVIAN ANALYSIS SUMMARY")
        print("=" * 60)
        print(f"Chain: {chain_display}")
        print(f"Sampling Method: {sampling_method}")
        print(f"Date Range: {date_range}")
        print(f"Gas Limit: {gas_limit:,}")
        print(f"DA Footprint Gas Scalars: {da_footprint_gas_scalars}")
        print("=" * 60)

    for scalar in da_footprint_gas_scalars:
        if show_progress:
            print(f"\n📊 Analyzing {get_chain_display_name(chain)} with DA footprint gas scalar: {scalar}")
            print(f"   Effective limit: {gas_limit / scalar:,.0f} bytes")
            print(f"   Sampling method: {sampling_method}")

        # Analyze blocks with this scalar (using da_footprint_gas_scalar param for compatibility)
        block_analyses = analyzer.analyze_multiple_blocks(df, scalar, show_progress)

        # Calculate aggregate statistics
        result = calculate_aggregate_statistics(
            block_analyses,
            scalar,
            gas_limit,
            chain,
            sampling_method,
            start_date,
            end_date
        )

        results[scalar] = result

        if show_progress:
            print(f"   ✅ Blocks exceeding: {result.blocks_exceeding}/{result.total_blocks} "
                  f"({result.percentage_exceeding:.1f}%)")
            print(f"   📈 Avg utilization: {result.avg_utilization:.2%}")
            print(f"   🗜️ Avg compression ratio: {result.compression_metrics.get('avg_compression_ratio', 0):.2f}x")

    return results


def calculate_aggregate_statistics(
    block_analyses: List[BlockAnalysis],
    da_footprint_gas_scalar: int,
    gas_limit: int,
    chain: str = "base",
    sampling_method: str = "top_percentile",
    start_date: str = None,
    end_date: str = None
) -> JovianAnalysisResult:
    """
    Calculate aggregate statistics from block analyses.

    Args:
        block_analyses: List of BlockAnalysis objects
        da_footprint_gas_scalar: Scalar used
        gas_limit: Gas limit
        chain: Chain name
        sampling_method: Sampling method used
        start_date: Analysis start date
        end_date: Analysis end date

    Returns:
        JovianAnalysisResult with aggregate statistics
    """
    if not block_analyses:
        return JovianAnalysisResult(
            da_footprint_gas_scalar=da_footprint_gas_scalar,
            gas_limit=gas_limit,
            chain=chain,
            sampling_method=sampling_method,
            start_date=start_date,
            end_date=end_date,
            total_blocks=0,
            blocks_exceeding=0,
            percentage_exceeding=0,
            avg_utilization=0,
            max_utilization=0,
            avg_excess_percentage=0,
            max_excess_percentage=0,
            block_analyses=[],
            utilization_distribution={},
            excess_distribution=None,
            compression_metrics={}
        )

    # Extract key metrics
    utilizations = [block.calldata_utilization for block in block_analyses]

    util_vs_used_vals = [
        block.utilization_vs_gas_used
        for block in block_analyses
        if getattr(block, "utilization_vs_gas_used", None) is not None
    ]

    blocks_with_gas_used = len(util_vs_used_vals)

    if util_vs_used_vals:
        avg_util_vs_gas_used   = float(np.mean(util_vs_used_vals))
        median_util_vs_gas_used= float(np.median(util_vs_used_vals))
        p95_util_vs_gas_used   = float(np.percentile(util_vs_used_vals, 95))
        share_over_1_vs_used   = float(np.mean(np.array(util_vs_used_vals) > 1.0))
    else:
        avg_util_vs_gas_used   = 0.0
        median_util_vs_gas_used= 0.0
        p95_util_vs_gas_used   = 0.0
        share_over_1_vs_used   = 0.0

    exceeding = [block for block in block_analyses if block.exceeds_limit]

    # Calculate utilization distribution (for histogram)
    utilization_bins = create_utilization_bins(utilizations)

    # Calculate excess distribution for blocks that exceed
    excess_distribution = None
    avg_excess_pct = 0
    max_excess_pct = 0

    if exceeding:
        excess_percentages = [
            (block.total_calldata_footprint - gas_limit) / gas_limit * 100
            for block in exceeding
        ]
        avg_excess_pct = np.mean(excess_percentages)
        max_excess_pct = max(excess_percentages)
        excess_distribution = create_excess_bins(excess_percentages)

    # Calculate compression metrics
    compression_metrics = calculate_compression_metrics(block_analyses)

    return JovianAnalysisResult(
        calldata_footprint_gas_scalar=calldata_footprint_gas_scalar,
        gas_limit=gas_limit,
        chain=chain,
        sampling_method=sampling_method,
        start_date=start_date,
        end_date=end_date,
        total_blocks=len(block_analyses),
        blocks_exceeding=len(exceeding),
        percentage_exceeding=len(exceeding) / len(block_analyses) * 100,
        avg_utilization=np.mean(utilizations),
        max_utilization=max(utilizations),
        avg_excess_percentage=avg_excess_pct,
        max_excess_percentage=max_excess_pct,
        block_analyses=block_analyses,
        utilization_distribution=utilization_bins,
        excess_distribution=excess_distribution,
        compression_metrics=compression_metrics,
        avg_util_vs_gas_used=avg_util_vs_gas_used,
        median_util_vs_gas_used=median_util_vs_gas_used,
        p95_util_vs_gas_used=p95_util_vs_gas_used,
        share_over_1_vs_used=share_over_1_vs_used,
        blocks_with_gas_used=blocks_with_gas_used
    )


def calculate_compression_metrics(block_analyses: List[BlockAnalysis]) -> Dict[str, float]:
    """
    Calculate compression ratio metrics for blocks.

    Args:
        block_analyses: List of BlockAnalysis objects

    Returns:
        Dictionary with compression metrics
    """
    if not block_analyses:
        return {}

    compression_ratios = []
    da_efficiency_ratios = []

    for block in block_analyses:
        if block.total_fastlz_size > 0:
            # Compression ratio: original / compressed
            compression_ratio = block.total_calldata_size / block.total_fastlz_size
            compression_ratios.append(compression_ratio)

            # DA efficiency: da_usage_estimate / original_calldata
            da_efficiency = block.total_da_usage_estimate / block.total_calldata_size
            da_efficiency_ratios.append(da_efficiency)

    return {
        "avg_compression_ratio": np.mean(compression_ratios) if compression_ratios else 0,
        "median_compression_ratio": np.median(compression_ratios) if compression_ratios else 0,
        "min_compression_ratio": min(compression_ratios) if compression_ratios else 0,
        "max_compression_ratio": max(compression_ratios) if compression_ratios else 0,
        "std_compression_ratio": np.std(compression_ratios) if compression_ratios else 0,
        "avg_da_efficiency": np.mean(da_efficiency_ratios) if da_efficiency_ratios else 0,
        "median_da_efficiency": np.median(da_efficiency_ratios) if da_efficiency_ratios else 0,
        "blocks_analyzed": len(compression_ratios)
    }


def create_utilization_bins(utilizations: List[float]) -> Dict[str, int]:
    """
    Create histogram bins for utilization distribution.

    Args:
        utilizations: List of utilization values (0-1+)

    Returns:
        Dictionary with bin labels and counts
    """
    bins = {
        "0-10%": 0,
        "10-20%": 0,
        "20-30%": 0,
        "30-40%": 0,
        "40-50%": 0,
        "50-60%": 0,
        "60-70%": 0,
        "70-80%": 0,
        "80-90%": 0,
        "90-100%": 0,
        ">100%": 0
    }

    for util in utilizations:
        util_pct = util * 100
        if util_pct <= 10:
            bins["0-10%"] += 1
        elif util_pct <= 20:
            bins["10-20%"] += 1
        elif util_pct <= 30:
            bins["20-30%"] += 1
        elif util_pct <= 40:
            bins["30-40%"] += 1
        elif util_pct <= 50:
            bins["40-50%"] += 1
        elif util_pct <= 60:
            bins["50-60%"] += 1
        elif util_pct <= 70:
            bins["60-70%"] += 1
        elif util_pct <= 80:
            bins["70-80%"] += 1
        elif util_pct <= 90:
            bins["80-90%"] += 1
        elif util_pct <= 100:
            bins["90-100%"] += 1
        else:
            bins[">100%"] += 1

    return bins


def create_excess_bins(excess_percentages: List[float]) -> Dict[str, int]:
    """
    Create histogram bins for excess distribution (blocks over limit).

    Args:
        excess_percentages: List of excess percentages (how much over 100%)

    Returns:
        Dictionary with bin labels and counts
    """
    bins = {
        "0-5%": 0,
        "5-10%": 0,
        "10-20%": 0,
        "20-30%": 0,
        "30-50%": 0,
        "50-100%": 0,
        ">100%": 0
    }

    for excess in excess_percentages:
        if excess <= 5:
            bins["0-5%"] += 1
        elif excess <= 10:
            bins["5-10%"] += 1
        elif excess <= 20:
            bins["10-20%"] += 1
        elif excess <= 30:
            bins["20-30%"] += 1
        elif excess <= 50:
            bins["30-50%"] += 1
        elif excess <= 100:
            bins["50-100%"] += 1
        else:
            bins[">100%"] += 1

    return bins


def create_compression_ratio_bins(compression_ratios: List[float]) -> Dict[str, int]:
    """
    Create histogram bins for compression ratio distribution.

    Args:
        compression_ratios: List of compression ratios (original/compressed)

    Returns:
        Dictionary with bin labels and counts
    """
    bins = {
        "1.0-1.5x": 0,
        "1.5-2.0x": 0,
        "2.0-2.5x": 0,
        "2.5-3.0x": 0,
        "3.0-3.5x": 0,
        "3.5-4.0x": 0,
        "4.0-5.0x": 0,
        ">5.0x": 0
    }

    for ratio in compression_ratios:
        if ratio <= 1.5:
            bins["1.0-1.5x"] += 1
        elif ratio <= 2.0:
            bins["1.5-2.0x"] += 1
        elif ratio <= 2.5:
            bins["2.0-2.5x"] += 1
        elif ratio <= 3.0:
            bins["2.5-3.0x"] += 1
        elif ratio <= 3.5:
            bins["3.0-3.5x"] += 1
        elif ratio <= 4.0:
            bins["3.5-4.0x"] += 1
        elif ratio <= 5.0:
            bins["4.0-5.0x"] += 1
        else:
            bins[">5.0x"] += 1

    return bins


def calculate_scalar_limits(
    gas_limit: int,
    da_footprint_gas_scalars: List[int] = None
) -> Dict[int, int]:
    """
    Calculate effective DA limits for different scalars.

    Args:
        gas_limit: Current gas limit
        da_footprint_gas_scalars: List of scalars to calculate

    Returns:
        Dictionary mapping scalar to effective limit
    """
    if da_footprint_gas_scalars is None:
        da_footprint_gas_scalars = DEFAULT_DA_FOOTPRINT_GAS_SCALARS

    limits = {}
    for scalar in da_footprint_gas_scalars:
        limits[scalar] = gas_limit // scalar

    return limits


def generate_jovian_recommendation(
    results: Dict[int, JovianAnalysisResult],
    target_excess_rate: float = 0.01,  # 1% blocks can exceed
    chain: str = "base",
    start_date: str = None,
    end_date: str = None
) -> Dict[str, Any]:
    recommendations = []

    for scalar, result in results.items():
        excess_rate = result.percentage_exceeding / 100

        measured_blocks = getattr(result, "blocks_with_gas_used", 0) or 0
        share_over_1 = getattr(result, "share_over_1_vs_used", 0.0) or 0.0
        over_gas_used_pct = share_over_1 * 100.0
        over_gas_used_blocks = int(round(share_over_1 * measured_blocks))

        recommendation = {
            "da_footprint_gas_scalar": scalar,
            "blocks_exceeding_pct": result.percentage_exceeding,
            "avg_utilization": result.avg_utilization,
            "max_utilization": result.max_utilization,
            "meets_target": excess_rate <= target_excess_rate,
            "effective_limit": result.gas_limit // scalar,
            "avg_compression_ratio": result.compression_metrics.get("avg_compression_ratio", 0),
            "avg_da_efficiency": result.compression_metrics.get("avg_da_efficiency", 0),

            # NEW fields (reference only; doesn’t affect selection logic)
            "over_gas_used_pct": over_gas_used_pct,                # % among measured blocks
            "over_gas_used_blocks": over_gas_used_blocks,          # count among measured blocks
            "measured_blocks_gas_used": measured_blocks,           # denominator for the % above
        }

        # Severity assessment (unchanged)
        if excess_rate == 0:
            recommendation["assessment"] = "Very Conservative"
        elif excess_rate <= 0.001:
            recommendation["assessment"] = "Conservative"
        elif excess_rate <= 0.01:
            recommendation["assessment"] = "Balanced"
        elif excess_rate <= 0.05:
            recommendation["assessment"] = "Aggressive"
        else:
            recommendation["assessment"] = "Very Aggressive"

        recommendations.append(recommendation)

    # Optimal pick logic (unchanged)
    optimal = None
    for rec in recommendations:
        if rec["meets_target"]:
            if optimal is None or rec["da_footprint_gas_scalar"] > optimal["da_footprint_gas_scalar"]:
                optimal = rec
    if optimal is None:
        optimal = min(recommendations, key=lambda x: x["blocks_exceeding_pct"])

    chain_name = get_chain_display_name(chain)
    date_range = f"{start_date} → {end_date}" if start_date and end_date else "date range not specified"

    return {
        "chain": chain,
        "chain_display_name": chain_name,
        "start_date": start_date,
        "end_date": end_date,
        "date_range": date_range,
        "optimal_da_footprint_gas_scalar": optimal["da_footprint_gas_scalar"],
        "optimal_assessment": optimal["assessment"],
        "optimal_excess_rate": optimal["blocks_exceeding_pct"],
        "optimal_compression_ratio": optimal["avg_compression_ratio"],
        "recommendation_type": "constant",
        "rationale": (
            f"For {chain_name} ({date_range}): DA footprint gas scalar of "
            f"{optimal['da_footprint_gas_scalar']} provides {optimal['assessment'].lower()} "
            f"configuration with {optimal['blocks_exceeding_pct']:.2f}% blocks exceeding limit "
            f"(avg compression: {optimal['avg_compression_ratio']:.2f}x)"
        ),
        "all_options": recommendations
    }


def analyze_block_size_distribution(
    block_analyses: List[BlockAnalysis]
) -> Dict[str, Any]:
    """
    Analyze the distribution of block sizes for histogram generation.

    Args:
        block_analyses: List of BlockAnalysis objects

    Returns:
        Dictionary with distribution statistics
    """
    if not block_analyses:
        return {"error": "No blocks to analyze"}

    # Extract DA usage estimates
    total_da_usage_estimates = [block.total_da_usage_estimate for block in block_analyses]
    calldata_sizes = [block.total_calldata_size for block in block_analyses]
    compressed_sizes = [block.total_fastlz_size for block in block_analyses]

    # Calculate compression ratios
    compression_ratios = []
    for calldata, compressed in zip(calldata_sizes, compressed_sizes):
        if compressed > 0:
            compression_ratios.append(calldata / compressed)

    return {
        "da_usage_estimates": {
            "min": min(total_da_usage_estimates),
            "max": max(total_da_usage_estimates),
            "mean": np.mean(total_da_usage_estimates),
            "median": np.median(total_da_usage_estimates),
            "std": np.std(total_da_usage_estimates),
            "percentiles": {
                "p25": np.percentile(total_da_usage_estimates, 25),
                "p50": np.percentile(total_da_usage_estimates, 50),
                "p75": np.percentile(total_da_usage_estimates, 75),
                "p90": np.percentile(total_da_usage_estimates, 90),
                "p95": np.percentile(total_da_usage_estimates, 95),
                "p99": np.percentile(total_da_usage_estimates, 99)
            }
        },
        "calldata_sizes": {
            "min": min(calldata_sizes),
            "max": max(calldata_sizes),
            "mean": np.mean(calldata_sizes),
            "total": sum(calldata_sizes)
        },
        "compression_ratios": {
            "mean": np.mean(compression_ratios) if compression_ratios else 0,
            "median": np.median(compression_ratios) if compression_ratios else 0,
            "distribution": create_compression_ratio_bins(compression_ratios) if compression_ratios else {}
        }
    }


if __name__ == "__main__":
    # Test the analysis functions
    print("Testing Jovian Analysis Functions")
    print("=" * 60)

    # This would normally use real data from clickhouse_fetcher
    print("✅ Jovian analysis functions module loaded successfully")
    print("   Available functions:")
    print("   - perform_jovian_analysis()")
    print("   - calculate_scalar_limits()")
    print("   - generate_jovian_recommendation()")
    print("   - analyze_block_size_distribution()")
    print("   - calculate_compression_metrics()")
    print("\n📊 DA footprint gas scalars (Jovian): ", DEFAULT_DA_FOOTPRINT_GAS_SCALARS)
