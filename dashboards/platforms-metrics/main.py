from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import clickhouse_connect
import queries
import numpy as np

# Constants
DATE_COLUMN_START = "period_start"
DATE_COLUMN_END = "period_end"
DATA_PATH = Path(__file__).parent / "test_data/github_pr_metrics.csv"
LOAD_CSV = False
# Dictionary to map raw column names to friendlier labels
FRIENDLY_LABELS = {
    "repo": "Repository",
    "period_start": "Start Date",
    "period_end": "End Date",
    "new_prs": "New PRs",
    "merged_prs": "Merged PRs",
    "closed_prs": "Closed PRs",
    "active_prs": "Active PRs",
    "median_time_to_first_review_hours": "Time to First Review (hrs)",
    "median_time_to_first_non_bot_comment_hours": "Time to First Non-Bot Comment (hrs)",
    "median_time_to_merge_hours": "Time to Merge (hrs)",
    "median_time_to_first_approval_hours": "Time to First Approval (hrs)",
    "total_comments": "Total Comments",
    "total_reviews": "Total Reviews",
    "unique_commenters": "Unique Commenters",
    "unique_reviewers": "Unique Reviewers",
    "unique_contributors": "Unique Contributors",
    "approved_prs": "Approved PRs",
    "rejected_prs": "Rejected PRs",
    # "review_requested_prs": "Review-Requested PRs",
    "period_type": "Lookback Window",
    "approval_ratio": "Approval Ratio",
    "merge_ratio": "Merge Ratio",
    "closed_ratio": "Closed Ratio",
    "comment_intensity": "Comment Intensity",
    "review_intensity": "Review Intensity",
    "active_ratio": "Active Ratio",
    "response_time_ratio": "Response Time Ratio",
    "contributor_engagement": "Contributor Engagement",
    "dt": "Data Timestamp",
}

PERIOD_TYPE_MAP = {
    "rolling_week": "week",
    "rolling_month": "month",
    "rolling_3months": "3 months",
    "rolling_6months": "6 months",
    "rolling_year": "year",
}


def get_prev_date_offset(latest_date: pd.Timestamp, window_label: str) -> pd.Timestamp:
    """
    Return a 'target' date from the latest_date based on the chosen lookback window.
    We can adjust these heuristics as needed.
    """
    w = window_label.lower()
    if "day" in w:
        return latest_date - pd.Timedelta(days=1)
    elif "week" in w:
        return latest_date - pd.Timedelta(weeks=1)
    elif "month" in w:
        return latest_date - pd.DateOffset(months=1)
    elif "quarter" in w:
        return latest_date - pd.DateOffset(months=3)
    else:
        return latest_date - pd.Timedelta(weeks=1)


def load_github_pr_data() -> pd.DataFrame:
    """
    Load the GitHub PR metrics dataset from a CSV file or from the ClickHouse client (commented out).
    """
    if LOAD_CSV:
        df = pd.read_csv(DATA_PATH)
    else:
        client = clickhouse_connect.get_client(
            host=st.secrets.clickhouse.host,
            port=st.secrets.clickhouse.port,
            username=st.secrets.clickhouse.username,
            password=st.secrets.clickhouse.password,
            secure=True,
        )
        df = client.query_df(queries.github_pr_metrics)
        df.drop(columns=["dt"], inplace=True)
        # df.to_csv(Path(__file__).parent / "test_data/github_pr_metrics_v1.csv", index=False) # Debug
    df[DATE_COLUMN_START] = pd.to_datetime(df[DATE_COLUMN_START])
    df[DATE_COLUMN_END] = pd.to_datetime(df[DATE_COLUMN_END])
    df["period_type"] = df["period_type"].map(lambda x: PERIOD_TYPE_MAP[x])
    df.drop(columns=["review_requested_prs"], inplace=True)  # Todo: fix this metric
    # Filter out data before 2024-01-01, as in original code
    df = df[df[DATE_COLUMN_START] >= pd.to_datetime("2024-01-01")]
    return df


def plot_line_chart(
    df: pd.DataFrame,
    x_col: str,
    y_cols: list[str],
    title: str = "",
    markers: bool = False,
    ylabel: str = "",
) -> None:
    fig = px.line(
        df,
        x=x_col,
        y=y_cols,
        markers=markers,
        title=title,
        labels={col: FRIENDLY_LABELS.get(col, col) for col in y_cols + [x_col]},
    )
    fig.update_layout(legend_title_text="Metrics", xaxis_title="", yaxis_title=ylabel)
    st.plotly_chart(fig, use_container_width=True)


def plot_cumulative_chart(
    df: pd.DataFrame, x_col: str, y_cols: list[str], title: str = "Cumulative Metrics Over Time"
) -> None:
    grouped_df = df.groupby(x_col, as_index=False)[y_cols].sum()
    cum_df = grouped_df.copy()
    for col in y_cols:
        cum_df[col] = cum_df[col].cumsum()

    fig = px.line(
        cum_df,
        x=x_col,
        y=y_cols,
        title=title,
        labels={col: FRIENDLY_LABELS.get(col, col) for col in y_cols + [x_col]},
    )
    fig.update_layout(legend_title_text="Metrics", xaxis_title="", yaxis_title="Cumulative Counts")
    st.plotly_chart(fig, use_container_width=True)


def plot_box_plot(
    df: pd.DataFrame, metric_cols: list[str], title: str = "Box Plot of Metrics", ylabel: str = ""
) -> None:
    melted_df = df[metric_cols].melt(var_name="Metric", value_name="Value")
    # Replace the melted metric labels with friendlier versions
    melted_df["Metric"] = melted_df["Metric"].apply(lambda x: FRIENDLY_LABELS.get(x, x))
    fig = px.box(melted_df, x="Metric", y="Value", points="outliers", title=title)
    fig.update_layout(
        legend_title_text="Metrics", xaxis_title="", yaxis_title=ylabel, yaxis_type="log"
    )
    st.plotly_chart(fig, use_container_width=True)


def plot_line_dual_axis(
    df: pd.DataFrame,
    x_col: str,
    y_left: str,
    y_right: str,
    title: str = "Dual-Axis Chart",
    y_left_label: str = "",
) -> None:
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(
            x=df[x_col],
            y=df[y_left],
            mode="lines",
            name=FRIENDLY_LABELS.get(y_left, y_left),
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=df[x_col],
            y=df[y_right],
            mode="lines",
            name=FRIENDLY_LABELS.get(y_right, y_right),
        ),
        secondary_y=True,
    )
    fig.update_layout(title_text=title)
    fig.update_xaxes(title_text="")
    fig.update_yaxes(title_text=FRIENDLY_LABELS.get(y_left, y_left), secondary_y=False)
    fig.update_yaxes(title_text=FRIENDLY_LABELS.get(y_right, y_right), secondary_y=True)
    fig.update_layout(legend_title_text="Metrics", xaxis_title="", yaxis_title=y_left_label)
    st.plotly_chart(fig, use_container_width=True)


def plot_scatter_two_metrics(
    df: pd.DataFrame,
    repos: list[str],
    window: str,
    metric_x: str,
    metric_y: str,
    color_by: str = "repo",
    log_x: bool = False,
    log_y: bool = False,
    show_regression: bool = False,
) -> go.Figure:
    """
    Create a scatter plot of metric_x vs. metric_y, optionally coloring by a chosen dimension,
    with an optional manual regression line and log axes.
    """
    all_selected = any(r.lower() == "all" for r in repos)
    if not all_selected:
        df = df[df["repo"].isin(repos)]
    if window and window.lower() != "all":
        df = df[df["period_type"] == window]

    plot_df = df[[metric_x, metric_y, "repo", "period_type"]].dropna().copy()

    x_label = FRIENDLY_LABELS.get(metric_x, metric_x)
    y_label = FRIENDLY_LABELS.get(metric_y, metric_y)
    title = f"Scatter: {x_label} vs. {y_label}"

    # Decide how to color
    color_col = None
    if color_by == "repo":
        color_col = "repo"
    elif color_by == "period_type":
        color_col = "period_type"

    fig = px.scatter(
        plot_df,
        x=metric_x,
        y=metric_y,
        color=color_col if color_col else None,
        title=title,
        labels={metric_x: x_label, metric_y: y_label},
        log_x=log_x,
        log_y=log_y,
        hover_data=["repo", "period_type"],
    )

    # If regression is requested, add a manual line
    if show_regression and not plot_df.empty:
        # We'll do a simple linear fit using np.polyfit
        x_vals = plot_df[metric_x].values
        y_vals = plot_df[metric_y].values

        # If we're in log scale, we typically do linear regression on log coords,
        # but let's keep it simple and do normal scale unless you prefer otherwise.
        slope, intercept = np.polyfit(x_vals, y_vals, 1)
        x_line = np.linspace(x_vals.min(), x_vals.max(), 50)
        y_line = slope * x_line + intercept

        # Add the regression line
        fig.add_trace(
            go.Scatter(
                x=x_line,
                y=y_line,
                mode="lines",
                name="Regression Line",
                line=dict(color="red", dash="dot"),
            )
        )

    fig.update_layout(legend_title="Legend")
    return fig


def main() -> None:
    st.set_page_config(
        page_title="Platforms Metrics Dashboard", layout="wide", initial_sidebar_state="expanded"
    )

    st.title("Platforms Metrics Dashboard")
    with st.expander("About"):
        st.markdown("""
        * This dashboard shows metrics for GitHub PRs over time.
        * The lookback window is the rolling or fixed interval (e.g., daily, weekly, monthly) used to group these PR metrics.
        * For 'previous period', we look back one lookback window from the latest data point.
        * The data is refreshed daily.
        """)
    # -- Sidebar
    st.sidebar.header("Data Filters")
    with st.spinner("Loading GitHub PR metrics..."):
        data = load_github_pr_data()

    # Identify numeric columns
    exclude_cols = {"repo", DATE_COLUMN_START, DATE_COLUMN_END, "period_type", "dt"}
    numeric_cols = [
        col
        for col in data.columns
        if col not in exclude_cols and pd.api.types.is_numeric_dtype(data[col])
    ]

    if not numeric_cols:
        st.warning("No numeric columns found in the dataset. Please check the CSV structure.")
        st.stop()

    # Minimum Date Filter
    min_date_default = data[DATE_COLUMN_END].min().date()
    min_date = st.sidebar.date_input("Filter by Minimum Date", value=min_date_default)

    # Repo Filter
    st.sidebar.header("Repository and Lookback Window")
    repo_list = sorted(list(data["repo"].unique()), key=lambda x: len(x))
    repo = st.sidebar.selectbox(
        "Repository",
        repo_list,
        index=repo_list.index("optimism"),
        help="Which repository's metrics?",
    )

    # Lookback Window (was 'period_type')
    period_type_list = sorted(list(data["period_type"].unique()), key=lambda x: len(x))
    lookback_window = st.sidebar.selectbox(
        "Lookback Window (rolling)",
        period_type_list,
        index=period_type_list.index("month"),
        help="Determines how metrics are grouped (daily, weekly, monthly, etc.) for comparison.",
    )

    with st.sidebar.expander("What's a Lookback Window?"):
        st.markdown(
            "A **Lookback Window** is the rolling or fixed interval (e.g., daily, weekly, monthly) "
            "used to group these PR metrics. It also affects the 'previous period' comparisons."
        )

    # Filter data
    filtered_data = data.copy()
    filtered_data = filtered_data[filtered_data[DATE_COLUMN_END] >= pd.to_datetime(min_date)]
    if repo:
        filtered_data = filtered_data[filtered_data["repo"] == repo]
    if lookback_window:
        filtered_data = filtered_data[filtered_data["period_type"] == lookback_window]

    # Sort data descending by period_end
    sorted_data = filtered_data.sort_values(by=DATE_COLUMN_END, ascending=False)
    if sorted_data.empty:
        st.error("No data available for the selected filters.")
        st.stop()

    # Identify the "latest_data" and "previous_data"
    latest_data = sorted_data.head(1)
    latest_end_date = latest_data[DATE_COLUMN_END].iloc[0]
    prev_date_target = get_prev_date_offset(latest_end_date, lookback_window)

    # Closest row in sorted_data to prev_date_target
    prev_data_index = (sorted_data[DATE_COLUMN_END] - prev_date_target).abs().idxmin()
    previous_data = sorted_data.loc[[prev_data_index]]

    # -- Tabs
    tab1, tab2, tab3 = st.tabs(["Main metrics", "Timing", "Dynamic Analysis"])

    # --- Tab 1: Snapshot & Engagement
    with tab1:
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

            # Status metrics
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
            # Ratio metrics
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
                col
                for col in status_cols
                if col in subset_df.columns and col != "unique_contributors"
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
                    # "- **Review Requested PRs:** PRs that had at least one review requested.\n"
                )
            eng_cols = ["approved_prs", "rejected_prs"]
            ratio_eng_cols = ["review_intensity", "response_time_ratio", "contributor_engagement"]

            # Summaries
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
            # Engagement Over Time
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

            # Engagement Ratios Over Time
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

    # --- Tab 2: Timing
    with tab2:
        with st.container():
            st.subheader("PR Timing Metrics")
            with st.expander("Metric legend"):
                st.markdown(
                    "- **Median Time to Merge (hrs):** How long it typically takes (median) from PR creation to merge.\n"
                    "- **Median Time to First Non-Bot Comment (hrs):** How quickly humans start discussing the PR.\n"
                    "- **Median Time to First Review (hrs):** Time from PR creation until first review.\n"
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
            # Time metrics over time (line chart)
            st.write("---")
            st.subheader("'Time-to-X' Metrics Over Time")
            with st.expander("Metric legend"):
                st.markdown(
                    "- **Median Time to Merge (hrs):** How long it typically takes (median) from PR creation to merge.\n"
                    "- **Median Time to First Non-Bot Comment (hrs):** How quickly humans start discussing the PR.\n"
                    "- **Median Time to First Review (hrs):** Time from PR creation until first review.\n"
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
                    title="Timing Metrics Over Time (Median Time to Merge, Median Time to First Non-Bot Comment, Median Time to First Review, Approval Ratio)",
                    markers=False,
                    ylabel="Time (hrs)",
                )

            # Box plot of timing metrics
            st.subheader("Distribution of Timing Metrics (Log Scale)")
            box_df = filtered_data[time_metrics_filtered].dropna()
            if not box_df.empty:
                plot_box_plot(
                    df=filtered_data,
                    metric_cols=time_metrics_filtered,
                    title="Box Plot of Timing Metrics",
                    ylabel="Time (hrs)",
                )
            st.write("---")
            st.subheader("Distribution Percentiles")
            with st.expander("Metric legend"):
                st.markdown(
                    "- **Min:** Minimum value.\n"
                    "- **25th:** 25th percentile.\n"
                    "- **Median:** Median value.\n"
                    "- **75th:** 75th percentile.\n"
                )
            # Create columns for each metric
            if time_metrics_filtered:
                desc_stats = filtered_data[time_metrics_filtered].describe()
                metric_cols = st.columns(len(time_metrics_filtered))

                # Display stats for each metric in its own column
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
            st.write("---")
    # --- Tab 3: Dynamic Analysis
    with tab3:
        with st.container():
            st.subheader("Scatter Plot: Compare Any Two Metrics")
            st.markdown(
                "Select one or multiple repositories, pick two numeric metrics, and optionally add a regression line. "
                "The scatter plot will use the lookback window selected in the sidebar."
            )

            multi_repos = list(data["repo"].unique())
            multi_repos.insert(0, "all")
            selected_repos = st.multiselect("Select Repositories", multi_repos, default=["all"])
            metric_x = st.selectbox("Select X-axis Metric", numeric_cols, index=0)
            metric_y = st.selectbox(
                "Select Y-axis Metric", numeric_cols, index=min(1, len(numeric_cols) - 1)
            )

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                color_dimension = (
                    "repo"
                    if st.toggle(
                        "Color by Repository",
                        value=True,
                        help="Color points by repository. When off, all points will be the same color.",
                    )
                    else None
                )
            with col2:
                use_log_x = st.toggle(
                    "Log Scale X", value=False, help="Use logarithmic scale for X-axis"
                )
            with col3:
                use_log_y = st.toggle(
                    "Log Scale Y", value=False, help="Use logarithmic scale for Y-axis"
                )
            with col4:
                show_reg = st.toggle(
                    "Show Regression Line",
                    value=False,
                    help="Display a linear regression line showing the relationship between the metrics",
                )

            if metric_x == metric_y:
                st.warning("Please select two different metrics.")
            else:
                color_by_arg = None if color_dimension == "none" else color_dimension
                fig_scatter = plot_scatter_two_metrics(
                    df=data,
                    repos=selected_repos,
                    window=lookback_window,
                    metric_x=metric_x,
                    metric_y=metric_y,
                    color_by=color_by_arg,
                    log_x=use_log_x,
                    log_y=use_log_y,
                    show_regression=show_reg,
                )
                st.plotly_chart(fig_scatter, use_container_width=True)

                if show_reg:
                    all_selected = any(r.lower() == "all" for r in selected_repos)
                    corr_df = data.copy()
                    if not all_selected:
                        corr_df = corr_df[corr_df["repo"].isin(selected_repos)]
                    corr_df = corr_df[corr_df["period_type"] == lookback_window]

                    corr_df = corr_df[[metric_x, metric_y]].dropna()
                    if not corr_df.empty:
                        correlation_matrix = corr_df.corr()
                        corr_val = correlation_matrix.loc[metric_x, metric_y]
                        st.write(f"**Correlation coefficient (Pearson r):** {corr_val:.3f}")
                    else:
                        st.info("No data available after applying those filters.")


if __name__ == "__main__":
    main()
