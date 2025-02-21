import pandas as pd
import streamlit as st
from utils import DATE_COLUMN_END, FRIENDLY_LABELS, get_prev_date_offset, plot_line_chart


def render_tab(
    data: pd.DataFrame,
    filtered_data: pd.DataFrame,
    numeric_cols: list,
    repo: str,
    lookback_window: str,
):
    # Sort filtered data descending by period_end
    sorted_data = filtered_data.sort_values(by=DATE_COLUMN_END, ascending=False)
    if sorted_data.empty:
        st.error("No data available for the selected filters.")
        return

    # Identify latest and previous period data
    latest_data = sorted_data.head(1)
    latest_end_date = latest_data[DATE_COLUMN_END].iloc[0]
    prev_date_target = get_prev_date_offset(latest_end_date, lookback_window)
    prev_data_index = (sorted_data[DATE_COLUMN_END] - prev_date_target).abs().idxmin()
    previous_data = sorted_data.loc[[prev_data_index]]

    # --- Snapshot Section ---
    with st.container():
        left_space, center, right_space = st.columns([1, 2, 1])
        with left_space:
            st.subheader("Main Metrics")
            st.markdown(f"| For `{repo}` vs prev `{lookback_window}`")

        status_cols = ["new_prs", "merged_prs", "closed_prs", "active_prs", "unique_contributors"]
        ratio_cols = ["approval_ratio", "merge_ratio", "closed_ratio", "active_ratio"]

        st.write("")
        col_spots = st.columns(len(status_cols))
        for colname, col_slot in zip(status_cols, col_spots):
            if colname in numeric_cols:
                label = FRIENDLY_LABELS.get(colname, colname)
                curr_val = latest_data[colname].sum()
                prev_val = previous_data[colname].sum()
                delta_val = curr_val - prev_val
                col_slot.metric(label, int(curr_val), f"{delta_val:.0f} vs. prev")

        st.write("")
        c_spots_ratio = st.columns(len(ratio_cols))
        for colname, col_slot in zip(ratio_cols, c_spots_ratio):
            if colname in numeric_cols:
                label = FRIENDLY_LABELS.get(colname, colname)
                curr_val = latest_data[colname].mean()
                prev_val = previous_data[colname].mean()
                delta_val = curr_val - prev_val
                col_slot.metric(label, f"{curr_val:.2f}", f"{delta_val:.2f} vs. prev")

        st.markdown("---")
        st.subheader("Status Metrics Over Time")
        with st.expander("Metric legend"):
            st.markdown(
                "- **New PRs:** PRs that were created.\n"
                "- **Merged PRs:** PRs that were merged.\n"
                "- **Closed PRs:** PRs that were closed.\n"
                "- **Active PRs:** PRs that were active.\n"
                "- **Unique Contributors:** Number of unique contributors.\n"
            )

        st.markdown(f"| `{repo}` - `rolling {lookback_window}`")
        subset_df = (
            filtered_data[[DATE_COLUMN_END] + status_cols]
            .groupby(DATE_COLUMN_END, as_index=False)
            .sum()
        )
        plot_cols = [col for col in status_cols if col != "unique_contributors"]
        if not subset_df.empty and plot_cols:
            plot_line_chart(
                df=subset_df,
                x_col=DATE_COLUMN_END,
                y_cols=plot_cols,
                title="PRs over time (New, Merged, Closed, Active)",
                markers=False,
                ylabel="Count",
            )

    # --- Engagement Section ---
    st.markdown("---")
    with st.container():
        st.subheader("Engagement Metrics")
        with st.expander("Metric legend"):
            st.markdown(
                "- **Approved PRs:** PRs that were approved.\n"
                "- **Rejected PRs:** PRs that were rejected.\n"
            )
        eng_cols = ["approved_prs", "rejected_prs"]
        ratio_eng_cols = ["review_intensity", "response_time_ratio", "contributor_engagement"]

        eng_spots = st.columns(len(eng_cols))
        for colname, col_slot in zip(eng_cols, eng_spots):
            if colname in numeric_cols:
                label = FRIENDLY_LABELS.get(colname, colname)
                curr_val = latest_data[colname].sum()
                prev_val = previous_data[colname].sum()
                delta_val = curr_val - prev_val
                col_slot.metric(label, int(curr_val), f"{delta_val:.0f} vs. prev")

        ratio_spots = st.columns(len(ratio_eng_cols))
        for colname, col_slot in zip(ratio_eng_cols, ratio_spots):
            if colname in numeric_cols:
                label = FRIENDLY_LABELS.get(colname, colname)
                curr_val = latest_data[colname].mean()
                prev_val = previous_data[colname].mean()
                delta_val = curr_val - prev_val
                col_slot.metric(label, f"{curr_val:.2f}", f"{delta_val:.2f} vs. prev")

        st.write("---")
        eng_df = (
            filtered_data[[DATE_COLUMN_END] + eng_cols]
            .groupby(DATE_COLUMN_END, as_index=False)
            .mean()
            .fillna(0)
        )
        if not eng_df.empty:
            plot_line_chart(
                df=eng_df,
                x_col=DATE_COLUMN_END,
                y_cols=eng_cols,
                title="Engagement Over Time (Approved, Rejected)",
                markers=False,
                ylabel="Count",
            )
