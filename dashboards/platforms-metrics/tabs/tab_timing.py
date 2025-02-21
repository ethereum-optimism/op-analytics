import pandas as pd
import streamlit as st
from utils import DATE_COLUMN_END, FRIENDLY_LABELS, plot_line_chart, plot_box_plot


def render_tab(
    filtered_data: pd.DataFrame,
    numeric_cols: list,
    latest_data: pd.DataFrame,
    previous_data: pd.DataFrame,
    lookback_window: str,
):
    st.subheader("PR Timing Metrics")
    with st.expander("Metric legend"):
        st.markdown(
            "- **Median Time to Merge (hrs):** Time from PR creation to merge.\n"
            "- **Median Time to First Non-Bot Comment (hrs):** How quickly humans start discussing the PR.\n"
            "- **Median Time to First Review (hrs):** Time until first review.\n"
            "- **Approval Ratio:** Percentage of PRs that were approved."
        )

    time_metrics = [
        "median_time_to_merge_hours",
        "median_time_to_first_non_bot_comment_hours",
        "median_time_to_first_review_hours",
        "approval_ratio",
    ]
    time_metrics_filtered = [m for m in time_metrics if m in numeric_cols]

    col_spots = st.columns(len(time_metrics_filtered))
    for colname, col_slot in zip(time_metrics_filtered, col_spots):
        current_val = latest_data[colname].mean()
        prev_val = previous_data[colname].mean()
        delta_val = current_val - prev_val
        label = FRIENDLY_LABELS.get(colname, colname)
        col_slot.metric(label, f"{current_val:.2f}", f"{delta_val:.2f} vs. prev")

    st.write("---")
    st.subheader("'Time-to-X' Metrics Over Time")
    with st.expander("Metric legend"):
        st.markdown(
            "- **Median Time to Merge (hrs):** Time from PR creation to merge.\n"
            "- **Median Time to First Non-Bot Comment (hrs):** How quickly humans start discussing the PR.\n"
            "- **Median Time to First Review (hrs):** Time until first review.\n"
            "- **Approval Ratio:** Percentage of PRs that were approved."
        )
    timing_chart_df = (
        filtered_data[[DATE_COLUMN_END] + time_metrics_filtered]
        .groupby(DATE_COLUMN_END, as_index=False)
        .mean()
        .fillna(0)
    )
    if not timing_chart_df.empty:
        plot_line_chart(
            df=timing_chart_df,
            x_col=DATE_COLUMN_END,
            y_cols=time_metrics_filtered,
            title="Timing Metrics Over Time",
            markers=False,
            ylabel="Time (hrs)",
        )

    st.subheader("Distributions")
    box_df = filtered_data[time_metrics_filtered].dropna()
    if not box_df.empty:
        plot_box_plot(
            df=filtered_data,
            metric_cols=time_metrics_filtered,
            title="Distribution of Timing Metrics (Log Scale)",
            ylabel="Time (hrs)",
        )

    st.write("---")
    st.subheader("Metric Percentiles")
    with st.expander("Metric legend"):
        st.markdown(
            "- **Min:** Minimum value.\n"
            "- **25th:** 25th percentile.\n"
            "- **Median:** Median value.\n"
            "- **75th:** 75th percentile.\n"
        )
    if time_metrics_filtered:
        desc_stats = filtered_data[time_metrics_filtered].describe()
        metric_cols = st.columns(len(time_metrics_filtered))
        for colname, col in zip(time_metrics_filtered, metric_cols):
            friendly_label = FRIENDLY_LABELS.get(colname, colname)
            with col:
                st.markdown(
                    f"**{friendly_label}**\n"
                    f"- Min: {desc_stats[colname]['min']:.2f}\n"
                    f"- 25th: {desc_stats[colname]['25%']:.2f}\n"
                    f"- Median: {desc_stats[colname]['50%']:.2f}\n"
                    f"- 75th: {desc_stats[colname]['75%']:.2f}\n"
                    f"- Max: {desc_stats[colname]['max']:.2f}"
                )
