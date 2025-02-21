import streamlit as st
from utils import (
    DATE_COLUMN_END,
    FRIENDLY_LABELS,
    plot_line_chart,
    plot_line_dual_axis,
)


def render_tab1(
    filtered_data,
    numeric_cols,
    latest_data,
    previous_data,
    repo,
    lookback_window,
):
    # --- Snapshot & Engagement (originally under with tab1:) ---
    # ========== SNAPSHOT SECTION ==========
    with st.container():
        left_space, center, right_space = st.columns([1, 2, 1])
        with left_space:
            st.subheader("Status Overview")
            st.markdown(f"| For `{repo}` vs prev `{lookback_window}`")

        status_cols = [
            "new_prs",
            "merged_prs",
            "closed_prs",
            "active_prs",
            "unique_contributors",
        ]
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
            filtered_data[[DATE_COLUMN_END] + [c for c in status_cols if c in numeric_cols]]
            .groupby(DATE_COLUMN_END, as_index=False)
            .sum()
        )
        plot_cols = [
            col for col in status_cols if col in subset_df.columns and col != "unique_contributors"
        ]
        if not subset_df.empty and plot_cols:
            plot_line_chart(
                df=subset_df,
                x_col=DATE_COLUMN_END,
                y_cols=plot_cols,
                title="PRs over time (New, Merged, Closed, Active)",
                markers=False,
                ylabel="Count",
            )

        st.subheader("Action Ratios Over Time")
        with st.expander("Metric legend"):
            st.markdown(
                "- **Approval Ratio:** Percentage of PRs that were approved.\n"
                "- **Merge Ratio:** Percentage of PRs that were merged.\n"
                "- **Closed Ratio:** Percentage of PRs that were closed.\n"
                "- **Active Ratio:** Percentage of PRs that were active.\n"
            )
        st.markdown(f"| `{repo}` - `rolling {lookback_window}`")
        subset_ratio_df = (
            filtered_data[[DATE_COLUMN_END] + [c for c in ratio_cols if c in numeric_cols]]
            .groupby(DATE_COLUMN_END, as_index=False)
            .mean()
        )
        if not subset_ratio_df.empty:
            existing_ratio_cols = [c for c in ratio_cols if c in subset_ratio_df.columns]
            plot_line_chart(
                df=subset_ratio_df,
                x_col=DATE_COLUMN_END,
                y_cols=existing_ratio_cols,
                title="Action Ratios Over Time (Approval, Merge, Closed, Active)",
                markers=False,
                ylabel="Ratio",
            )

    # ========== ENGAGEMENT SECTION ==========
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
            filtered_data[[DATE_COLUMN_END] + [c for c in eng_cols if c in numeric_cols]]
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

        ratio_eng_df = (
            filtered_data[[DATE_COLUMN_END] + [c for c in ratio_eng_cols if c in numeric_cols]]
            .groupby(DATE_COLUMN_END, as_index=False)
            .mean()
            .fillna(0)
        )
        if not ratio_eng_df.empty:
            plot_line_chart(
                df=ratio_eng_df,
                x_col=DATE_COLUMN_END,
                y_cols=ratio_eng_cols,
                title="Engagement Ratios Over Time (Review Intensity, Response Time Ratio, Contributor Engagement)",
                markers=False,
                ylabel="Ratio",
            )

        st.subheader("Comments & Reviews")
        with st.expander("Metric legend"):
            st.markdown(
                "- **Total Comments:** Total number of comments on PRs.\n"
                "- **Unique Commenters:** Number of unique commenters.\n"
                "- **Total Reviews:** Total number of reviews.\n"
                "- **Unique Reviewers:** Number of unique reviewers.\n"
            )
        cr_cols = ["total_comments", "unique_commenters", "total_reviews", "unique_reviewers"]
        valid_cr_cols = [c for c in cr_cols if c in numeric_cols]
        if valid_cr_cols:
            cr_slots = st.columns(len(valid_cr_cols))
            for colname, col_slot in zip(valid_cr_cols, cr_slots):
                curr_val = latest_data[colname].sum()
                prev_val = previous_data[colname].sum()
                delta_val = curr_val - prev_val
                label = FRIENDLY_LABELS.get(colname, colname)
                col_slot.metric(label, int(curr_val), f"{delta_val:.0f} vs. prev")

            cr_df = (
                filtered_data[[DATE_COLUMN_END] + valid_cr_cols]
                .groupby(DATE_COLUMN_END, as_index=False)
                .sum()
            )
            if not cr_df.empty:
                plot_line_chart(
                    df=cr_df,
                    x_col=DATE_COLUMN_END,
                    y_cols=valid_cr_cols,
                    title="Comments & Reviews Over Time (Total, Unique Commenters, Total Reviews, Unique Reviewers)",
                    ylabel="Count",
                )

        st.subheader("Comment & Review Intensity")
        with st.expander("Metric legend"):
            st.markdown(
                "- **Comment Intensity:** Average number of comments per PR.\n"
                "- **Review Intensity:** Average number of reviews per PR.\n"
            )
        intensity_cols = ["comment_intensity", "review_intensity"]
        valid_intensity_cols = [c for c in intensity_cols if c in numeric_cols]
        if valid_intensity_cols:
            col_slots_int = st.columns(len(valid_intensity_cols))
            for colname, col_slot in zip(valid_intensity_cols, col_slots_int):
                curr_val = latest_data[colname].mean()
                prev_val = previous_data[colname].mean()
                delta_val = curr_val - prev_val
                label = FRIENDLY_LABELS.get(colname, colname)
                col_slot.metric(label, f"{curr_val:.2f}", f"{delta_val:.2f} vs. prev")

            int_df = (
                filtered_data[[DATE_COLUMN_END] + valid_intensity_cols]
                .groupby(DATE_COLUMN_END, as_index=False)
                .mean()
                .fillna(0)
            )
            if not int_df.empty:
                plot_line_chart(
                    df=int_df,
                    x_col=DATE_COLUMN_END,
                    y_cols=valid_intensity_cols,
                    title="Comment & Review Intensity Over Time (Comment Intensity, Review Intensity)",
                    markers=False,
                    ylabel="Ratio",
                )

        if "review_requested_prs" in numeric_cols and "approved_prs" in numeric_cols:
            st.subheader("Compare Review Requests vs. Approvals")
            with st.expander("Metric legend"):
                st.markdown(
                    "- **Review Requests:** Number of review requests.\n"
                    "- **Approved PRs:** Number of approved PRs.\n"
                )
            compare_df = (
                filtered_data[[DATE_COLUMN_END, "review_requested_prs", "approved_prs"]]
                .groupby(DATE_COLUMN_END, as_index=False)
                .mean()
                .fillna(0)
            )
            if not compare_df.empty:
                plot_line_dual_axis(
                    df=compare_df,
                    x_col=DATE_COLUMN_END,
                    y_left="review_requested_prs",
                    y_right="approved_prs",
                    title="Review Requests vs. Approved PRs Over Time",
                    y_left_label="Review Requests",
                )
