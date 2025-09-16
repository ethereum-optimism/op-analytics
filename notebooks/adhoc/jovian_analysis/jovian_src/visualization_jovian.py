"""
Consolidated Jovian visualization functions with all required plots.
Includes compression analysis and multi-chain support.
"""

import matplotlib.pyplot as plt
import seaborn as sns
import polars as pl
import numpy as np
from typing import Dict, List, Any, Optional
from pathlib import Path
from matplotlib.ticker import PercentFormatter
from typing import Callable, Optional, List

from .analysis_functions import JovianAnalysisResult
from .chain_config import get_chain_display_name, get_chain_color
from .core import BlockAnalysis
from .constants import (
    DEFAULT_GAS_LIMIT,
    DEFAULT_DA_FOOTPRINT_GAS_SCALARS,
    HISTOGRAM_BINS,
    FIGURE_SIZE_LARGE,
    PLOT_DPI
)

# Set style for consistent visualizations
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")


def plot_da_usage_estimates_histogram(
    block_analyses: List[BlockAnalysis],
    chain: str = "base",
    start_date: str = None,
    end_date: str = None,
    title: str = None,
    save_path: Optional[Path] = None,
    gas_limit: int = DEFAULT_GAS_LIMIT,
    eip1559_elasticity: float = 2.0
) -> plt.Figure:
    """
    Plot histogram of total size estimates - two versions: with and without scalar limits.
    """
    if not block_analyses:
        print("❌ No data to plot")
        return None

    if title is None:
        date_suffix = f" ({start_date} → {end_date})" if start_date and end_date else ""
        title = f"{get_chain_display_name(chain)}: Distribution of Block DA Usage Estimates{date_suffix}"

    # Extract DA usage estimates
    da_usage_estimates = [block.total_da_usage_estimate for block in block_analyses]

    # Create figure with three subplots side by side
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(24, 7))

    # Use chain color
    color = get_chain_color(chain)

    # ========== LEFT PLOT: Basic distribution ==========
    n1, bins1, patches1 = ax1.hist(da_usage_estimates, bins=HISTOGRAM_BINS, edgecolor='black', alpha=0.7, color=color)
    ax1.set_xlabel('DA usage estimate (bytes)', fontsize=11)
    ax1.set_ylabel('Number of Blocks', fontsize=11)
    ax1.set_title(f'{title}', fontsize=12, fontweight='bold')

    # Add only mean and median lines
    ax1.axvline(np.median(da_usage_estimates), color='green', linestyle='-', linewidth=2,
                label=f'Median: {np.median(da_usage_estimates):,.0f}')
    ax1.axvline(np.mean(da_usage_estimates), color='red', linestyle='--', linewidth=2,
                label=f'Mean: {np.mean(da_usage_estimates):,.0f}')

    ax1.legend(loc='upper right', fontsize=10)
    ax1.grid(True, alpha=0.3)

    # ========== MIDDLE PLOT: With scalar limits ==========
    n2, bins2, patches2 = ax2.hist(da_usage_estimates, bins=HISTOGRAM_BINS, edgecolor='black', alpha=0.7, color=color)
    ax2.set_xlabel('DA usage estimate (bytes)', fontsize=11)
    ax2.set_ylabel('Number of Blocks', fontsize=11)
    ax2.set_title(f'{title} (with Scalar Limits)', fontsize=12, fontweight='bold')

    # Add mean and median
    ax2.axvline(np.median(da_usage_estimates), color='green', linestyle='-', linewidth=2,
                label=f'Median: {np.median(da_usage_estimates):,.0f}')
    ax2.axvline(np.mean(da_usage_estimates), color='red', linestyle='--', linewidth=2,
                label=f'Mean: {np.mean(da_usage_estimates):,.0f}')

    # Add vertical lines for scalar limits only
    scalars = DEFAULT_DA_FOOTPRINT_GAS_SCALARS
    scalar_colors = ['blue', 'purple', 'brown', 'pink']
    for scalar, scolor in zip(scalars, scalar_colors):
        # DA usage limit: gas_limit / da_footprint_gas_scalar
        limit = gas_limit / scalar
        ax2.axvline(limit, color=scolor, linestyle=':', linewidth=1.5, alpha=0.7,
                   label=f'Limit @ {scalar}: {limit:,.0f}')

    ax2.legend(loc='upper right', fontsize=9, ncol=1)
    ax2.grid(True, alpha=0.3)

    # ========== RIGHT PLOT: With scalar targets ==========
    n3, bins3, patches3 = ax3.hist(da_usage_estimates, bins=HISTOGRAM_BINS, edgecolor='black', alpha=0.7, color=color)
    ax3.set_xlabel('DA usage estimate (bytes)', fontsize=11)
    ax3.set_ylabel('Number of Blocks', fontsize=11)
    ax3.set_title(f'{title} (with Scalar Targets)', fontsize=12, fontweight='bold')

    # Add mean and median
    ax3.axvline(np.median(da_usage_estimates), color='green', linestyle='-', linewidth=2,
                label=f'Median: {np.median(da_usage_estimates):,.0f}')
    ax3.axvline(np.mean(da_usage_estimates), color='red', linestyle='--', linewidth=2,
                label=f'Mean: {np.mean(da_usage_estimates):,.0f}')


    # Add vertical lines for scalar targets only
    for scalar, scolor in zip(scalars, scalar_colors):
        target = gas_limit / (eip1559_elasticity * scalar)
        ax3.axvline(target, color=scolor, linestyle='--', linewidth=1.5, alpha=0.7,
                   label=f'Target @ {scalar}: {target:,.0f}')

    ax3.legend(loc='upper right', fontsize=9, ncol=1)
    ax3.grid(True, alpha=0.3)

    # Add statistics text to left plot only
    stats_text = (
        f"Total Blocks: {len(da_usage_estimates):,}\n"
        f"Min: {min(da_usage_estimates):,.0f}\n"
        f"Max: {max(da_usage_estimates):,.0f}\n"
        f"Mean: {np.mean(da_usage_estimates):,.0f}\n"
        f"Median: {np.median(da_usage_estimates):,.0f}\n"
        f"Std Dev: {np.std(da_usage_estimates):,.0f}"
    )
    ax1.text(0.02, 0.98, stats_text, transform=ax1.transAxes,
             verticalalignment='top', horizontalalignment='right',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8),
             fontsize=8)

    plt.tight_layout()

    if save_path:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, dpi=PLOT_DPI, bbox_inches='tight')
        print(f"✅ Saved DA usage estimates histogram (3 plots: basic, limits, targets) to {save_path}")

    return fig


def plot_compression_ratio_histogram(
    block_analyses: List[BlockAnalysis],
    chain: str = "base",
    start_date: str = None,
    end_date: str = None,
    title: str = None,
    save_path: Optional[Path] = None
) -> plt.Figure:
    """
    Plot histogram of compression ratios with enhanced vertical lines and zones.
    """
    if not block_analyses:
        print("❌ No data to plot")
        return None

    if title is None:
        date_suffix = f" ({start_date} → {end_date})" if start_date and end_date else ""
        title = f"{get_chain_display_name(chain)}: Compression Ratio Distribution{date_suffix}"

    # Calculate compression ratios
    compression_ratios = []
    da_efficiencies = []

    for block in block_analyses:
        if block.total_fastlz_size > 0:
            ratio = block.total_calldata_size / block.total_fastlz_size
            compression_ratios.append(ratio)

            # DA efficiency
            if block.total_calldata_size > 0:
                da_eff = block.total_da_usage_estimate / (block.total_calldata_size + 100 * block.tx_count)
                da_efficiencies.append(da_eff * 100)  # Convert to percentage

    if not compression_ratios:
        print("❌ No compression data available")
        return None

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))

    color = get_chain_color(chain)

    # Compression ratio histogram
    n1, bins1, patches1 = ax1.hist(compression_ratios, bins=30, edgecolor='black', alpha=0.7, color=color)
    ax1.set_xlabel('Compression Ratio (Original/Compressed)', fontsize=11)
    ax1.set_ylabel('Number of Blocks', fontsize=11)
    ax1.set_title(title, fontsize=12, fontweight='bold')

    # Add vertical lines for compression ratio (simplified - only mean and median)
    median_val = np.median(compression_ratios)
    mean_val = np.mean(compression_ratios)

    ax1.axvline(median_val, color='green', linestyle='-', linewidth=2,
                label=f'Median: {median_val:.2f}x')
    ax1.axvline(mean_val, color='red', linestyle='--', linewidth=2,
                label=f'Mean: {mean_val:.2f}x')

    ax1.legend(loc='upper right', fontsize=8, framealpha=0.9)
    ax1.grid(True, alpha=0.3)

    # DA efficiency histogram
    if da_efficiencies:
        n2, bins2, patches2 = ax2.hist(da_efficiencies, bins=30, edgecolor='black', alpha=0.7, color=color)
        ax2.set_xlabel('DA Efficiency (%)', fontsize=11)
        ax2.set_ylabel('Number of Blocks', fontsize=11)
        date_suffix = f" ({start_date} → {end_date})" if start_date and end_date else ""
        ax2.set_title(f'{get_chain_display_name(chain)}: DA Efficiency Distribution{date_suffix}', fontsize=12, fontweight='bold')

        # Add vertical lines for DA efficiency (simplified - only mean and median)
        da_median = np.median(da_efficiencies)
        da_mean = np.mean(da_efficiencies)

        ax2.axvline(da_median, color='green', linestyle='-', linewidth=2,
                    label=f'Median: {da_median:.1f}%')
        ax2.axvline(da_mean, color='red', linestyle='--', linewidth=2,
                    label=f'Mean: {da_mean:.1f}%')

        ax2.legend(loc='upper right', fontsize=8, framealpha=0.9)
        ax2.grid(True, alpha=0.3)

    # Add statistics for compression ratio
    stats_text = (
        f"Blocks analyzed: {len(compression_ratios):,}\n"
        f"Range: {min(compression_ratios):.2f}x - {max(compression_ratios):.2f}x\n"
        f"Mean: {np.mean(compression_ratios):.2f}x\n"
        f"Median: {np.median(compression_ratios):.2f}x\n"
        f"Std Dev: {np.std(compression_ratios):.2f}"
    )
    ax1.text(0.02, 0.98, stats_text, transform=ax1.transAxes,
             verticalalignment='top', horizontalalignment='left',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8),
             fontsize=8)

    plt.tight_layout()

    if save_path:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"✅ Saved enhanced compression histogram to {save_path}")

    return fig


def plot_exceeding_limits_analysis(
    results_by_scalar: Dict[int, JovianAnalysisResult],
    chain: str = "base",
    start_date: str = None,
    end_date: str = None,
    title: str = None,
    save_path: Optional[Path] = None
) -> plt.Figure:
    """
    Plot analysis of blocks exceeding limits.
    """
    if title is None:
        date_suffix = f" ({start_date} → {end_date})" if start_date and end_date else ""
        title = f"{get_chain_display_name(chain)}: Blocks Exceeding Limit Analysis{date_suffix}"

    fig, axes = plt.subplots(2, 2, figsize=(15, 12))

    # Extract data
    scalars = sorted(results_by_scalar.keys())
    exceeding_counts = [results_by_scalar[s].blocks_exceeding for s in scalars]
    exceeding_pcts = [results_by_scalar[s].percentage_exceeding for s in scalars]
    total_blocks = [results_by_scalar[s].total_blocks for s in scalars]

    # Use chain color
    color = get_chain_color(chain)

    # Plot 1: Blocks exceeding by scalar
    ax = axes[0, 0]
    bars = ax.bar(range(len(scalars)), exceeding_counts, color=color, alpha=0.7)
    ax.set_xlabel('Calldata Footprint Gas Scalar')
    ax.set_ylabel('Number of Blocks Exceeding')
    ax.set_title('Blocks Exceeding Limit by Scalar')
    ax.set_xticks(range(len(scalars)))
    ax.set_xticklabels(scalars)

    # Add value labels on bars
    for i, (bar, val) in enumerate(zip(bars, exceeding_counts)):
        if val > 0:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                   f'{val}\n({exceeding_pcts[i]:.1f}%)',
                   ha='center', va='bottom')

    # Plot 2: Percentage exceeding
    ax = axes[0, 1]
    ax.plot(scalars, exceeding_pcts, marker='o', linewidth=2, markersize=8, color=color)
    ax.set_xlabel('Calldata Footprint Gas Scalar')
    ax.set_ylabel('Percentage of Blocks Exceeding (%)')
    ax.set_title('Percentage of Blocks Exceeding Limit')
    ax.grid(True, alpha=0.3)
    ax.axhline(y=1, color='red', linestyle='--', alpha=0.5, label='1% threshold')
    ax.legend()

    # Plot 3: Utilization distribution
    ax = axes[1, 0]
    for scalar in scalars:
        result = results_by_scalar[scalar]
        utilizations = [b.calldata_utilization * 100 for b in result.block_analyses]
        if utilizations:
            ax.hist(utilizations, bins=30, alpha=0.5, label=f'Scalar {scalar}')

    ax.set_xlabel('Utilization (%)')
    ax.set_ylabel('Number of Blocks')
    ax.set_title('Utilization Distribution by Scalar')
    ax.axvline(x=100, color='red', linestyle='--', alpha=0.7, label='100% limit')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # Plot 4: Summary table
    ax = axes[1, 1]
    ax.axis('tight')
    ax.axis('off')

    table_data = []
    for scalar in scalars:
        result = results_by_scalar[scalar]
        table_data.append([
            f"{scalar}",
            f"{result.gas_limit // scalar:,}",
            f"{result.blocks_exceeding}/{result.total_blocks}",
            f"{result.percentage_exceeding:.2f}%",
            f"{result.avg_utilization:.1%}"
        ])

    table = ax.table(cellText=table_data,
                    colLabels=['Scalar', 'Effective\nLimit', 'Blocks\nExceeding',
                              'Percentage\nExceeding', 'Avg\nUtilization'],
                    cellLoc='center',
                    loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.5)

    # Color code the table
    for i, scalar in enumerate(scalars):
        if results_by_scalar[scalar].percentage_exceeding > 5:
            color_code = '#ffcccc'  # Red tint
        elif results_by_scalar[scalar].percentage_exceeding > 1:
            color_code = '#ffffcc'  # Yellow tint
        else:
            color_code = '#ccffcc'  # Green tint

        for j in range(5):
            table[(i+1, j)].set_facecolor(color_code)

    plt.suptitle(title, fontsize=16, y=1.02)
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"✅ Saved exceeding limits analysis to {save_path}")

    return fig


def plot_excess_distribution(
    results_by_scalar: Dict[int, JovianAnalysisResult],
    chain: str = "base",
    start_date: str = None,
    end_date: str = None,
    title: str = None,
    save_path: Optional[Path] = None
) -> plt.Figure:
    """
    Plot histogram of excess amounts for blocks exceeding limit.
    """
    if title is None:
        date_suffix = f" ({start_date} → {end_date})" if start_date and end_date else ""
        title = f"{get_chain_display_name(chain)}: Distribution of Excess for Blocks Over Limit{date_suffix}"

    # Find which scalars have exceeding blocks
    scalars_with_excess = []
    for scalar, result in results_by_scalar.items():
        if result.blocks_exceeding > 0:
            scalars_with_excess.append(scalar)

    if not scalars_with_excess:
        print("✅ No blocks exceed limits for any scalar")
        return None

    # Create subplots
    n_plots = len(scalars_with_excess)
    n_cols = min(2, n_plots)
    n_rows = (n_plots + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 5 * n_rows))
    if n_plots == 1:
        axes = [axes]
    else:
        axes = axes.flatten() if n_plots > 1 else [axes]

    color = get_chain_color(chain)

    for idx, scalar in enumerate(scalars_with_excess):
        ax = axes[idx]
        result = results_by_scalar[scalar]

        # Calculate excess percentages for exceeding blocks
        exceeding_blocks = [b for b in result.block_analyses if b.exceeds_limit]
        excess_pcts = [
            (b.total_da_footprint - result.gas_limit) / result.gas_limit * 100
            for b in exceeding_blocks
        ]

        if excess_pcts:
            ax.hist(excess_pcts, bins=20, edgecolor='black', alpha=0.7, color=color)
            ax.set_xlabel('Excess Percentage (%)')
            ax.set_ylabel('Number of Blocks')
            ax.set_title(f'Scalar {scalar}\n{len(excess_pcts)} blocks exceeding')

            # Add statistics
            stats_text = (
                f"Min excess: {min(excess_pcts):.1f}%\n"
                f"Max excess: {max(excess_pcts):.1f}%\n"
                f"Mean excess: {np.mean(excess_pcts):.1f}%\n"
                f"Median excess: {np.median(excess_pcts):.1f}%"
            )
            ax.text(0.98, 0.98, stats_text, transform=ax.transAxes,
                   verticalalignment='top', horizontalalignment='right',
                   bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

            ax.grid(True, alpha=0.3)

    # Hide unused subplots
    for idx in range(n_plots, len(axes)):
        axes[idx].set_visible(False)

    plt.suptitle(title, fontsize=16, y=1.02)
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"✅ Saved excess distribution to {save_path}")

    return fig


def plot_over_utilization_percentages(
    results_by_scalar: Dict[int, JovianAnalysisResult],
    chain: str = "base",
    start_date: str = None,
    end_date: str = None,
    title: str = None,
    save_path: Optional[Path] = None
) -> plt.Figure:
    """
    Quantify and visualize over-utilization percentages.
    """
    if title is None:
        date_suffix = f" ({start_date} → {end_date})" if start_date and end_date else ""
        title = f"{get_chain_display_name(chain)}: Over-Utilization Analysis{date_suffix}"

    fig, axes = plt.subplots(2, 2, figsize=(15, 12))

    scalars = sorted(results_by_scalar.keys())
    color = get_chain_color(chain)

    # Plot 1: Stacked bar showing exceeding vs non-exceeding
    ax = axes[0, 0]
    exceeding = [results_by_scalar[s].blocks_exceeding for s in scalars]
    non_exceeding = [results_by_scalar[s].total_blocks - results_by_scalar[s].blocks_exceeding for s in scalars]

    x = np.arange(len(scalars))
    width = 0.6

    p1 = ax.bar(x, non_exceeding, width, label='Within Limit', color='lightgreen', alpha=0.7)
    p2 = ax.bar(x, exceeding, width, bottom=non_exceeding, label='Exceeding', color='coral', alpha=0.7)

    ax.set_xlabel('Calldata Footprint Gas Scalar')
    ax.set_ylabel('Number of Blocks')
    ax.set_title('Block Distribution: Within vs Exceeding Limit')
    ax.set_xticks(x)
    ax.set_xticklabels(scalars)
    ax.legend()

    # Add annotations
    for i, scalar in enumerate(scalars):
        result = results_by_scalar[scalar]
        if result.blocks_exceeding > 0:
            ax.text(i, result.total_blocks + 5,
                   f"{result.blocks_exceeding}/{result.total_blocks}\n"
                   f"({result.percentage_exceeding:.1f}%)",
                   ha='center', fontsize=9)

    # Plot 2: Average excess percentage
    ax = axes[0, 1]
    avg_excess = []
    max_excess = []

    for scalar in scalars:
        result = results_by_scalar[scalar]
        avg_excess.append(result.avg_excess_percentage if result.blocks_exceeding > 0 else 0)
        max_excess.append(result.max_excess_percentage if result.blocks_exceeding > 0 else 0)

    x = np.arange(len(scalars))
    width = 0.35

    bars1 = ax.bar(x - width/2, avg_excess, width, label='Average Excess', color='orange', alpha=0.7)
    bars2 = ax.bar(x + width/2, max_excess, width, label='Maximum Excess', color='red', alpha=0.7)

    ax.set_xlabel('Calldata Footprint Gas Scalar')
    ax.set_ylabel('Excess Percentage (%)')
    ax.set_title('Excess Percentage for Blocks Over Limit')
    ax.set_xticks(x)
    ax.set_xticklabels(scalars)
    ax.legend()
    ax.grid(True, alpha=0.3)

    # Plot 3: Heatmap of utilization ranges
    ax = axes[1, 0]

    # Create utilization range matrix
    util_ranges = ['0-50%', '50-70%', '70-90%', '90-100%', '>100%']
    heatmap_data = []

    for scalar in scalars:
        result = results_by_scalar[scalar]
        utils = [b.calldata_utilization * 100 for b in result.block_analyses]

        row = []
        row.append(sum(1 for u in utils if u <= 50))
        row.append(sum(1 for u in utils if 50 < u <= 70))
        row.append(sum(1 for u in utils if 70 < u <= 90))
        row.append(sum(1 for u in utils if 90 < u <= 100))
        row.append(sum(1 for u in utils if u > 100))
        heatmap_data.append(row)

    im = ax.imshow(heatmap_data, cmap='YlOrRd', aspect='auto')
    ax.set_xticks(np.arange(len(util_ranges)))
    ax.set_yticks(np.arange(len(scalars)))
    ax.set_xticklabels(util_ranges)
    ax.set_yticklabels([f'Scalar {s}' for s in scalars])
    ax.set_xlabel('Utilization Range')
    ax.set_ylabel('Calldata Footprint Gas Scalar')
    ax.set_title('Block Count by Utilization Range')

    # Add text annotations
    for i in range(len(scalars)):
        for j in range(len(util_ranges)):
            text = ax.text(j, i, heatmap_data[i][j],
                         ha="center", va="center", color="black")

    plt.colorbar(im, ax=ax)

    # Plot 4: Summary statistics table
    ax = axes[1, 1]
    ax.axis('tight')
    ax.axis('off')

    table_data = []
    for scalar in scalars:
        result = results_by_scalar[scalar]
        table_data.append([
            f"{scalar}",
            f"{result.blocks_exceeding}",
            f"{result.total_blocks}",
            f"{result.percentage_exceeding:.2f}%",
            f"{result.avg_excess_percentage:.1f}%" if result.blocks_exceeding > 0 else "N/A"
        ])

    table = ax.table(cellText=table_data,
                    colLabels=['Scalar', 'Blocks\nExceeding', 'Total\nBlocks',
                              'Percentage\nExceeding', 'Avg Excess\n(if exceeding)'],
                    cellLoc='center',
                    loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.5)

    ax.set_title('Over-Utilization Summary', fontsize=12, pad=20)

    plt.suptitle(title, fontsize=16, y=1.02)
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"✅ Saved over-utilization analysis to {save_path}")

    return fig


def plot_da_footprint_gas_scalar_comparison(
    results_by_scalar: Dict[int, JovianAnalysisResult],
    chain: str = "base",
    start_date: str = None,
    end_date: str = None,
    title: str = None,
    save_path: Optional[Path] = None
) -> plt.Figure:
    """
    Compare results across different calldata footprint gas scalars.
    """
    if title is None:
        date_suffix = f" ({start_date} → {end_date})" if start_date and end_date else ""
        title = f"{get_chain_display_name(chain)}: Scalar Comparison{date_suffix}"

    fig, axes = plt.subplots(2, 3, figsize=(18, 12))

    scalars = sorted(results_by_scalar.keys())
    colors = plt.cm.viridis(np.linspace(0.2, 0.9, len(scalars)))
    chain_color = get_chain_color(chain)

    # Plot 1: Effective limits
    ax = axes[0, 0]
    limits = [results_by_scalar[s].gas_limit // s for s in scalars]
    bars = ax.bar(range(len(scalars)), limits, color=chain_color, alpha=0.7)
    ax.set_xlabel('Calldata Footprint Gas Scalar')
    ax.set_ylabel('Effective Calldata Limit (bytes)')
    ax.set_title('Effective Limits by Scalar')
    ax.set_xticks(range(len(scalars)))
    ax.set_xticklabels(scalars)

    for bar, limit in zip(bars, limits):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
               f'{limit:,.0f}', ha='center', va='bottom')

    # Plot 2: Utilization comparison
    ax = axes[0, 1]
    for i, scalar in enumerate(scalars):
        result = results_by_scalar[scalar]
        utils = [b.calldata_utilization * 100 for b in result.block_analyses]
        if utils:
            bp = ax.boxplot(utils, positions=[i], widths=0.6,
                          patch_artist=True, labels=[str(scalar)])
            bp['boxes'][0].set_facecolor(colors[i])

    ax.set_xlabel('Calldata Footprint Gas Scalar')
    ax.set_ylabel('Utilization (%)')
    ax.set_title('Utilization Distribution by Scalar')
    ax.axhline(y=100, color='red', linestyle='--', alpha=0.5, label='100% limit')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # Plot 3: Risk assessment
    ax = axes[0, 2]
    categories = ['Low Risk\n(≤70%)', 'Medium Risk\n(70-90%)', 'High Risk\n(90-100%)', 'Critical\n(>100%)']

    risk_data = []
    for scalar in scalars:
        result = results_by_scalar[scalar]
        utils = [b.calldata_utilization * 100 for b in result.block_analyses]

        low = sum(1 for u in utils if u <= 70)
        medium = sum(1 for u in utils if 70 < u <= 90)
        high = sum(1 for u in utils if 90 < u <= 100)
        critical = sum(1 for u in utils if u > 100)

        risk_data.append([low, medium, high, critical])

    x = np.arange(len(categories))
    width = 0.15

    for i, (scalar, data) in enumerate(zip(scalars, risk_data)):
        offset = (i - len(scalars)/2 + 0.5) * width
        ax.bar(x + offset, data, width, label=f'Scalar {scalar}', color=colors[i], alpha=0.7)

    ax.set_xlabel('Risk Category')
    ax.set_ylabel('Number of Blocks')
    ax.set_title('Risk Distribution by Scalar')
    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.legend()

    # Plot 4: Percentage exceeding vs scalar
    ax = axes[1, 0]
    pcts = [results_by_scalar[s].percentage_exceeding for s in scalars]
    ax.plot(scalars, pcts, marker='o', linewidth=2, color=chain_color)
    ax.set_xlabel('Calldata Footprint Gas Scalar')
    ax.set_ylabel('Percentage Exceeding (%)')
    ax.set_title('Blocks Exceeding vs Scalar')
    ax.grid(True, alpha=0.3)
    ax.set_xscale('log')

    # Plot 5: Compression efficiency
    ax = axes[1, 1]

    compression_ratios = []
    for scalar in scalars:
        result = results_by_scalar[scalar]
        if result.compression_metrics:
            compression_ratios.append(result.compression_metrics.get('avg_compression_ratio', 0))
        else:
            ratios = []
            for block in result.block_analyses:
                if block.total_fastlz_size > 0:
                    ratios.append(block.total_calldata_size / block.total_fastlz_size)
            compression_ratios.append(np.mean(ratios) if ratios else 0)

    ax.bar(range(len(scalars)), compression_ratios, color=chain_color, alpha=0.7)
    ax.set_xlabel('Calldata Footprint Gas Scalar')
    ax.set_ylabel('Average Compression Ratio')
    ax.set_title('FastLZ Compression Efficiency')
    ax.set_xticks(range(len(scalars)))
    ax.set_xticklabels(scalars)
    ax.axhline(y=3.0, color='red', linestyle='--', alpha=0.5, label='Expected ~3x')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # Plot 6: Recommendation matrix
    ax = axes[1, 2]
    ax.axis('tight')
    ax.axis('off')

    # Create recommendation scoring
    scores = []
    for scalar in scalars:
        result = results_by_scalar[scalar]
        score = 0

        # Score based on percentage exceeding
        if result.percentage_exceeding == 0:
            score += 3
        elif result.percentage_exceeding <= 1:
            score += 2
        elif result.percentage_exceeding <= 5:
            score += 1

        # Score based on average utilization
        avg_util = result.avg_utilization * 100
        if 50 <= avg_util <= 80:
            score += 3
        elif 40 <= avg_util <= 90:
            score += 2
        elif 30 <= avg_util <= 95:
            score += 1

        scores.append((scalar, score, result))

    # Sort by score
    scores.sort(key=lambda x: x[1], reverse=True)

    table_data = []
    for scalar, score, result in scores[:5]:  # Top 5 options
        assessment = "⭐" * min(score, 5)
        table_data.append([
            f"{scalar}",
            f"{result.percentage_exceeding:.2f}%",
            f"{result.avg_utilization:.1%}",
            f"{result.gas_limit // scalar:,}",
            assessment
        ])

    table = ax.table(cellText=table_data,
                    colLabels=['Scalar', 'Blocks\nExceeding', 'Avg\nUtilization',
                              'Effective\nLimit', 'Assessment'],
                    cellLoc='center',
                    loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.5)

    # Highlight best option
    table[(1, 0)].set_facecolor('#ccffcc')
    table[(1, 1)].set_facecolor('#ccffcc')
    table[(1, 2)].set_facecolor('#ccffcc')
    table[(1, 3)].set_facecolor('#ccffcc')
    table[(1, 4)].set_facecolor('#ccffcc')

    ax.set_title('Recommendation Matrix', fontsize=12, pad=20)

    plt.suptitle(title, fontsize=16, y=1.02)
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"✅ Saved scalar comparison to {save_path}")

    return fig


def generate_all_visualizations(
    results_by_scalar: Dict[int, JovianAnalysisResult],
    output_dir: Optional[Path] = None,
    chain: str = "base",
    start_date: str = None,
    end_date: str = None,
    eip1559_elasticity: float = 2.0
) -> Dict[str, plt.Figure]:
    """
    Generate all required visualizations for Jovian analysis.

    Args:
        results_by_scalar: Analysis results for different scalars
        output_dir: Directory to save plots
        chain: Chain name for context
        start_date: Analysis start date for plot titles
        end_date: Analysis end date for plot titles
    """
    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

    figures = {}

    # Get block analyses and gas limit from the first result
    first_result = next(iter(results_by_scalar.values()))
    block_analyses = first_result.block_analyses
    gas_limit = first_result.gas_limit

    # 1. DA usage estimates histogram (with gas_limit for vertical lines)
    print("📊 Generating enhanced DA usage estimates histogram...")
    fig1 = plot_da_usage_estimates_histogram(
        block_analyses,
        chain=chain,
        start_date=start_date,
        end_date=end_date,
        save_path=output_dir / "1_da_usage_estimates_histogram.png" if output_dir else None,
        gas_limit=gas_limit,
        eip1559_elasticity=eip1559_elasticity
    )
    figures["da_usage_estimates"] = fig1

    # 2. Compression ratio histogram (Jovian feature)
    print("📊 Generating compression ratio histogram...")
    fig2 = plot_compression_ratio_histogram(
        block_analyses,
        chain=chain,
        start_date=start_date,
        end_date=end_date,
        save_path=output_dir / "2_compression_histogram.png" if output_dir else None
    )
    figures["compression_ratios"] = fig2

    # 3. Exceeding limits analysis
    print("📊 Generating exceeding limits analysis...")
    fig3 = plot_exceeding_limits_analysis(
        results_by_scalar,
        chain=chain,
        start_date=start_date,
        end_date=end_date,
        save_path=output_dir / "3_exceeding_limits_analysis.png" if output_dir else None
    )
    figures["exceeding_limits"] = fig3

    # 4. Excess distribution
    print("📊 Generating excess distribution...")
    fig4 = plot_excess_distribution(
        results_by_scalar,
        chain=chain,
        start_date=start_date,
        end_date=end_date,
        save_path=output_dir / "4_excess_distribution.png" if output_dir else None
    )
    figures["excess_distribution"] = fig4

    # 5. Over-utilization percentages
    print("📊 Generating over-utilization analysis...")
    fig5 = plot_over_utilization_percentages(
        results_by_scalar,
        chain=chain,
        start_date=start_date,
        end_date=end_date,
        save_path=output_dir / "5_over_utilization_analysis.png" if output_dir else None
    )
    figures["over_utilization"] = fig5

    # 6. Scalar comparison
    print("📊 Generating scalar comparison...")
    fig6 = plot_da_footprint_gas_scalar_comparison(
        results_by_scalar,
        chain=chain,
        start_date=start_date,
        end_date=end_date,
        save_path=output_dir / "6_scalar_comparison.png" if output_dir else None
    )
    figures["scalar_comparison"] = fig6

    # 7. Comprehensive summary (NEW)
    print("📊 Generating comprehensive summary...")
    fig7 = plot_comprehensive_summary(
        results_by_scalar,
        chain=chain,
        start_date=start_date,
        end_date=end_date,
        save_path=output_dir / "7_comprehensive_summary.png" if output_dir else None,
        eip1559_elasticity=eip1559_elasticity
    )
    figures["comprehensive_summary"] = fig7

    print(f"✅ Generated {len(figures)} visualizations")

    return figures

def plot_util_vs_gas_used_hist(block_analyses, chain, start_date=None, end_date=None, save_path=None):
    import numpy as np, matplotlib.pyplot as plt
    vals = [ba.utilization_vs_gas_used for ba in block_analyses if ba.utilization_vs_gas_used is not None]
    if not vals:
        print("No blocks with block_gas_used available.")
        return None
    title = f"{chain.upper()}: DA footprint / gas used"
    if start_date and end_date:
        title += f" ({start_date} → {end_date})"
    fig = plt.figure(figsize=(8,5))
    plt.hist(vals, bins=40, edgecolor='black', alpha=0.75)
    plt.axvline(np.mean(vals), linestyle='--', linewidth=2, label=f"Mean: {np.mean(vals):.2f}")
    plt.axvline(np.median(vals), linestyle='-',  linewidth=2, label=f"Median: {np.median(vals):.2f}")
    plt.legend(); plt.grid(alpha=0.25); plt.title(title); plt.xlabel("DA footprint ÷ gas used"); plt.ylabel("Blocks")
    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
    return fig


def plot_comprehensive_summary(
    results_by_scalar: Dict[int, JovianAnalysisResult],
    chain: str = "base",
    start_date: str = None,
    end_date: str = None,
    title: str = None,
    save_path: Optional[Path] = None,
    eip1559_elasticity: float = 2.0
) -> plt.Figure:
    """
    Create a comprehensive single-page summary of all key metrics.
    """
    if not results_by_scalar:
        print("❌ No results to plot")
        return None

    if title is None:
        date_suffix = f" ({start_date} → {end_date})" if start_date and end_date else ""
        title = f"{get_chain_display_name(chain)}: Comprehensive Analysis Summary{date_suffix}"

    # Create figure with 6 subplots (2 rows, 3 columns)
    fig = plt.figure(figsize=(24, 12))
    gs = fig.add_gridspec(2, 3, hspace=0.3, wspace=0.25)

    scalars = sorted(results_by_scalar.keys())
    chain_color = get_chain_color(chain)

    # Get a reference result for block analyses
    ref_result = results_by_scalar[scalars[0]]

    # ========== Plot 1: Basic Distribution ==========
    ax1 = fig.add_subplot(gs[0, 0])

    # Collect all DA usage estimates
    all_da_usage_estimates = []
    for result in results_by_scalar.values():
        for block in result.block_analyses:
            all_da_usage_estimates.append(block.total_da_usage_estimate)

    if all_da_usage_estimates:
        # Create histogram
        n, bins, patches = ax1.hist(all_da_usage_estimates, bins=40, edgecolor='black',
                                    alpha=0.7, color=chain_color)

        # Add mean and median lines
        median_val = np.median(all_da_usage_estimates)
        mean_val = np.mean(all_da_usage_estimates)
        ax1.axvline(median_val, color='green', linestyle='-', linewidth=2,
                   label=f'Median: {median_val:,.0f}')
        ax1.axvline(mean_val, color='red', linestyle='--', linewidth=2,
                   label=f'Mean: {mean_val:,.0f}')

        ax1.set_xlabel('DA usage estimate (bytes)', fontsize=11)
        ax1.set_ylabel('Number of Blocks', fontsize=11)
        ax1.set_title('DA Usage Distribution', fontsize=12, fontweight='bold')
        ax1.legend(loc='upper right', fontsize=8, ncol=1)
        ax1.grid(True, alpha=0.3)

    # ========== Plot 2: Distribution with Scalar Limits ==========
    ax2 = fig.add_subplot(gs[0, 1])

    if all_da_usage_estimates:
        # Create histogram
        n, bins, patches = ax2.hist(all_da_usage_estimates, bins=40, edgecolor='black',
                                    alpha=0.7, color=chain_color)

        # Add vertical lines for each scalar limit only
        scalar_colors = ['blue', 'purple', 'brown', 'pink']
        for scalar, scolor in zip(scalars, scalar_colors):
            # DA usage limit: gas_limit / da_footprint_gas_scalar
            limit = ref_result.gas_limit / scalar
            ax2.axvline(limit, color=scolor, linestyle='--', linewidth=1.5, alpha=0.7,
                       label=f'Limit @ {scalar}: {limit:,.0f}')

        # Add mean and median lines
        ax2.axvline(median_val, color='green', linestyle='-', linewidth=2,
                   label=f'Median: {median_val:,.0f}')
        ax2.axvline(mean_val, color='red', linestyle='--', linewidth=2,
                   label=f'Mean: {mean_val:,.0f}')

        ax2.set_xlabel('DA usage estimate (bytes)', fontsize=11)
        ax2.set_ylabel('Number of Blocks', fontsize=11)
        ax2.set_title('DA Usage Distribution with Scalar Limits', fontsize=12, fontweight='bold')
        ax2.legend(loc='upper right', fontsize=8, ncol=1)
        ax2.grid(True, alpha=0.3)

    # ========== Plot 3: Distribution with Scalar Targets ==========
    ax3 = fig.add_subplot(gs[0, 2])

    if all_da_usage_estimates:
        # Create histogram
        n, bins, patches = ax3.hist(all_da_usage_estimates, bins=40, edgecolor='black',
                                    alpha=0.7, color=chain_color)


        # Add vertical lines for each scalar target only
        for scalar, scolor in zip(scalars, scalar_colors):
            target = ref_result.gas_limit / (eip1559_elasticity * scalar)
            ax3.axvline(target, color=scolor, linestyle=':', linewidth=1.5, alpha=0.7,
                       label=f'Target @ {scalar}: {target:,.0f}')

        # Add mean and median lines
        ax3.axvline(median_val, color='green', linestyle='-', linewidth=2,
                   label=f'Median: {median_val:,.0f}')
        ax3.axvline(mean_val, color='red', linestyle='--', linewidth=2,
                   label=f'Mean: {mean_val:,.0f}')

        ax3.set_xlabel('DA usage estimate (bytes)', fontsize=11)
        ax3.set_ylabel('Number of Blocks', fontsize=11)
        ax3.set_title('DA Usage Distribution with Scalar Targets', fontsize=12, fontweight='bold')
        ax3.legend(loc='upper right', fontsize=8, ncol=1)
        ax3.grid(True, alpha=0.3)

    # ========== Plot 4: Blocks Exceeding by Scalar ==========
    ax4 = fig.add_subplot(gs[1, 0])

    # Simple bar chart showing blocks exceeding for each scalar
    exceeding_counts = [results_by_scalar[s].blocks_exceeding for s in scalars]
    exceeding_pcts = [results_by_scalar[s].percentage_exceeding for s in scalars]

    bars = ax4.bar(range(len(scalars)), exceeding_counts, color=chain_color, alpha=0.7)
    ax4.set_xlabel('Calldata Footprint Gas Scalar', fontsize=11)
    ax4.set_ylabel('Number of Blocks Exceeding', fontsize=11)
    ax4.set_title('Blocks Exceeding Limit by Scalar', fontsize=12, fontweight='bold')
    ax4.set_xticks(range(len(scalars)))
    ax4.set_xticklabels(scalars)

    # Add value labels on bars
    for i, (bar, val, pct) in enumerate(zip(bars, exceeding_counts, exceeding_pcts)):
        if val > 0:
            ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                   f'{val}\n({pct:.1f}%)',
                   ha='center', va='bottom')

    ax4.grid(True, alpha=0.3, axis='y')

    # ========== Plot 5: Compression & Efficiency Metrics ==========
    ax5 = fig.add_subplot(gs[1, 1])

    # Calculate compression metrics
    compression_ratios = []
    da_efficiencies = []

    for block in ref_result.block_analyses:
        if block.total_fastlz_size > 0:
            ratio = block.total_calldata_size / block.total_fastlz_size
            compression_ratios.append(ratio)
        if block.total_calldata_size > 0:
            da_eff = block.total_da_usage_estimate / block.total_calldata_size * 100
            da_efficiencies.append(da_eff)

    if compression_ratios:
        # Create box plots
        bp1 = ax5.boxplot([compression_ratios], positions=[1], widths=0.6,
                          patch_artist=True, labels=['Compression\nRatio'])
        bp1['boxes'][0].set_facecolor(chain_color)
        bp1['boxes'][0].set_alpha(0.7)

        # Add text annotations
        ax5.text(1, np.median(compression_ratios), f'{np.median(compression_ratios):.2f}x',
                ha='center', va='bottom', fontweight='bold')

    if da_efficiencies:
        # Create second y-axis for DA efficiency
        ax5_2 = ax5.twinx()
        bp2 = ax5_2.boxplot([da_efficiencies], positions=[2], widths=0.6,
                           patch_artist=True, labels=['DA\nEfficiency'])
        bp2['boxes'][0].set_facecolor('orange')
        bp2['boxes'][0].set_alpha(0.7)

        ax5_2.set_ylabel('DA Efficiency (%)', fontsize=11)

        # Add text annotation
        ax5_2.text(2, np.median(da_efficiencies), f'{np.median(da_efficiencies):.1f}%',
                  ha='center', va='bottom', fontweight='bold')

    ax5.set_xlim(0.5, 2.5)
    ax5.set_xticks([1, 2])
    ax5.set_xticklabels(['Compression\nRatio', 'DA\nEfficiency'])
    ax5.set_ylabel('Compression Ratio (x)', fontsize=11)
    ax5.set_title('Compression & Efficiency Metrics', fontsize=12, fontweight='bold')
    ax5.grid(True, alpha=0.3, axis='y')

    # ========== Plot 6: Summary Table ==========
    ax6 = fig.add_subplot(gs[1, 2])
    ax6.axis('tight')
    ax6.axis('off')

    # Create summary table
    table_data = []
    for scalar in scalars:
        result = results_by_scalar[scalar]

        # Determine recommendation
        if result.percentage_exceeding <= 1:
            status = "✅"
            color = '#ccffcc'
        elif result.percentage_exceeding <= 5:
            status = "⚠️"
            color = '#ffffcc'
        else:
            status = "❌"
            color = '#ffcccc'

        compression = "N/A"
        if result.compression_metrics:
            compression = f"{result.compression_metrics.get('avg_compression_ratio', 0):.2f}x"

        table_data.append([
            f"{scalar}",
            f"{result.gas_limit // scalar:,}",
            f"{result.blocks_exceeding}/{result.total_blocks}",
            f"{result.percentage_exceeding:.2f}%",
            f"{result.avg_utilization:.1%}",
            compression,
            status
        ])

    table = ax6.table(cellText=table_data,
                     colLabels=['Scalar', 'Effective\nLimit', 'Blocks\nExceeding',
                               '% Exceed', 'Avg\nUtil', 'Compression', 'Status'],
                     cellLoc='center',
                     loc='center',
                     colWidths=[0.1, 0.18, 0.15, 0.12, 0.12, 0.15, 0.1])

    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 2)

    # Color code the status column
    for i, scalar in enumerate(scalars):
        result = results_by_scalar[scalar]
        if result.percentage_exceeding <= 1:
            color = '#ccffcc'
        elif result.percentage_exceeding <= 5:
            color = '#ffffcc'
        else:
            color = '#ffcccc'

        for j in range(7):  # Color entire row
            table[(i+1, j)].set_facecolor(color)
            table[(i+1, j)].set_alpha(0.3)

    # Make header row bold
    for j in range(7):
        table[(0, j)].set_facecolor('#e6e6e6')
        table[(0, j)].set_text_props(weight='bold')

    ax6.set_title('Summary Metrics by Scalar', fontsize=12, fontweight='bold', pad=20)

    # Add overall statistics text
    total_blocks = ref_result.total_blocks
    stats_text = (
        f"Analysis Summary for {get_chain_display_name(chain)}\n"
        f"Total Blocks Analyzed: {total_blocks:,}\n"
        f"Sampling Method: {ref_result.sampling_method}\n"
        f"Gas Limit: {ref_result.gas_limit:,}"
    )
    ax6.text(0.5, -0.15, stats_text, transform=ax6.transAxes,
            ha='center', va='top', fontsize=10,
            bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.3))

    # Add main title
    plt.suptitle(title, fontsize=16, fontweight='bold', y=0.98)

    plt.tight_layout()

    if save_path:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"✅ Saved comprehensive summary to {save_path}")

    return fig


def plot_block_metric_distribution(
    scalar_to_peek: int,
    analysis_results: dict,
    metric_fn: Callable,  # function from block → scalar
    title: str,
    xlabel: str,
    percent_scale: bool = True,
    value_filter: Optional[Callable] = None,
    chain_name: str = "CHAIN",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    min_calldata: bool = False,
):
    """
    Generic block-level metric distribution plot.

    Parameters:
    - scalar_to_peek: int key into analysis_results
    - analysis_results: dict of scalar → result
    - metric_fn: function that computes a float value from a block
    - title: str, for plot title
    - xlabel: str, for x-axis label
    - percent_scale: whether to scale values ×100 and format as %
    - value_filter: optional filter to apply on computed values
    - chain_name: str for chain display name
    - start_date, end_date: optional range display
    - min_calldata: if True, only include blocks with total_calldata_size > 0
    """
    res = analysis_results[scalar_to_peek]
    blocks = res.block_analyses
    if min_calldata:
        blocks = [b for b in blocks if b.total_calldata_size > 0]

    # Compute values
    values = []
    for b in blocks:
        try:
            val = metric_fn(b)
            if val is not None and (value_filter is None or value_filter(val)):
                values.append(val)
        except Exception:
            continue

    if not values:
        print(f"⚠️ No valid values to plot for: {title}")
        return

    values = np.array(values)
    mean_val   = float(np.mean(values))
    median_val = float(np.median(values))

    range_str = f" ({start_date} → {end_date})" if start_date and end_date else ""
    title_str = f"{chain_name}: {title}{range_str}"

    print(f"📊 {title}")
    print(f"   n blocks: {len(values):,}")
    print(f"   mean:   {mean_val:.2%}" if percent_scale else f"   mean:   {mean_val:.4f}")
    print(f"   median: {median_val:.2%}" if percent_scale else f"   median: {median_val:.4f}")

    # Plot
    plt.figure(figsize=(9, 5))
    plot_vals = values * 100 if percent_scale else values
    plt.hist(plot_vals, bins=40, edgecolor='black', alpha=0.75)

    # Add mean / median lines
    plt.axvline(mean_val * 100 if percent_scale else mean_val, linestyle='--', linewidth=2, label=f"Mean: {mean_val*100:.1f}%" if percent_scale else f"Mean: {mean_val:.4f}")
    plt.axvline(median_val * 100 if percent_scale else median_val, linestyle='-', linewidth=2, label=f"Median: {median_val*100:.1f}%" if percent_scale else f"Median: {median_val:.4f}")

    plt.xlabel(xlabel)
    plt.ylabel("Blocks")
    plt.title(title_str)

    if percent_scale:
        plt.gca().xaxis.set_major_formatter(PercentFormatter(xmax=100))
    plt.grid(alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.show()
