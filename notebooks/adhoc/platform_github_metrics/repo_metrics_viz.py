import streamlit as st
import polars as pl
import plotly.express as px
import io

# Metric descriptions mapping for the dashboard.
METRIC_DESCRIPTIONS = {
    "new_prs": "Count of new pull requests created.",
    "merged_prs": "Count of pull requests that were merged.",
    "closed_prs": "Count of pull requests that were closed without merging.",
    "active_prs": "Count of pull requests that are active or recently updated.",
    "median_time_to_first_review_hours": "Median hours taken to get the first review.",
    "median_time_to_first_non_bot_comment_hours": "Median hours taken for the first non-bot comment.",
    "median_time_to_merge_hours": "Median hours from PR creation to merge.",
    "total_comments": "Total number of comments on pull requests.",
    "total_reviews": "Total number of reviews performed on pull requests.",
    "unique_commenters": "Unique users who commented on pull requests.",
    "unique_reviewers": "Unique users who reviewed the pull requests.",
    "unique_contributors": "Unique contributors who created pull requests.",
    "approved_prs": "Count of pull requests that received approval.",
    "rejected_prs": "Count of pull requests that were rejected.",
    "review_requested_prs": "Count of pull requests with review requests.",
    "approval_ratio": "Approval ratio: approved PRs divided by new PRs.",
    "merge_ratio": "Merge ratio: merged PRs divided by new PRs.",
    "closed_ratio": "Closed ratio: closed PRs divided by new PRs.",
    "comment_intensity": "Comment intensity: average comments per PR.",
    "review_intensity": "Review intensity: average reviews per PR.",
    "active_ratio": "Active ratio: active PRs divided by new PRs.",
    "response_time_ratio": "Response time ratio: non-bot comment time relative to merge time.",
    "contributor_engagement": "Contributor engagement: metric indicating user interaction.",
    "repo": "Repository name.",
    "period_start": "Start of the time period.",
    "period_end": "End of the time period.",
    "period_type": "Type of period (rolling window interval).",
}


# Cache the data load to speed up reruns.
@st.cache_data
def load_data(file_bytes):
    # Read CSV data from the provided bytes wrapped in a BytesIO.
    return pl.read_csv(io.BytesIO(file_bytes))


def plot_repo_metric_over_time(
    df: pl.DataFrame, repo: str, metric: str, x_axis: str, markers: bool = True
):
    """
    Plot a single metric over time, split by the time window (period_type).
    If "all" is selected as the repo, the data is not filtered by repo.
    """
    # Filter the data if a specific repo is selected.
    if repo.lower() != "all":
        df = df.filter(pl.col("repo") == repo)

    # Sort and select only the necessary columns.
    df_pd = df.sort([x_axis, "period_type"]).select(["period_type", x_axis, metric]).to_pandas()

    title = (
        f"{metric.capitalize().replace('_', ' ')} over time for '{repo}'"
        if repo.lower() != "all"
        else f"{metric.capitalize().replace('_', ' ')} over time for all repositories"
    )

    fig = px.line(df_pd, x=x_axis, y=metric, color="period_type", title=title, markers=markers)
    fig.update_layout(xaxis_title=x_axis, yaxis_title=metric, legend_title="Timeframe")
    return fig


def plot_multi_repo_metric_over_time(
    df: pl.DataFrame, repos: list, period_type: str, metric: str, x_axis: str, chart_type: str
):
    """
    Plot a single metric over time for multiple repositories.
    :param repos: list of selected repos
    :param period_type: selected rolling window (e.g. '7d' or '30d'), if applicable
    :param metric: numeric metric to plot
    :param x_axis: 'period_end' or 'period_start'
    :param chart_type: "Line", "Stacked Area", or "Side-by-Side Bar"
    """
    # Filter by the selected repos (skip if empty or "all" in the list).
    # We'll treat "all" as meaning "include everything."
    if "all" not in [r.lower() for r in repos]:
        df = df.filter(pl.col("repo").is_in(repos))

    # Filter by period_type if needed (depends on your data).
    if period_type and period_type.lower() != "all":
        df = df.filter(pl.col("period_type") == period_type)

    # Sort and select relevant columns.
    df_pd = df.sort([x_axis, "repo"]).select(["repo", x_axis, metric, "period_type"]).to_pandas()

    # Build the figure.
    title = f"{metric.capitalize().replace('_',' ')} - Multi-Repo Comparison"

    if chart_type == "Line":
        fig = px.line(
            df_pd,
            x=x_axis,
            y=metric,
            color="repo",
            title=title,
            markers=True,
        )
    elif chart_type == "Stacked Area":
        fig = px.area(
            df_pd,
            x=x_axis,
            y=metric,
            color="repo",
            title=title,
        )
    else:  # Side-by-Side Bar
        fig = px.bar(
            df_pd,
            x=x_axis,
            y=metric,
            color="repo",
            barmode="group",
            title=title,
        )

    fig.update_layout(xaxis_title=x_axis, yaxis_title=metric, legend_title="Repository")
    return fig


def plot_scatter_two_metrics(
    df: pl.DataFrame, repos: list, period_type: str, metric_x: str, metric_y: str
):
    """
    Plot a scatter chart for two selected metrics, possibly filtered by repos and period_type.
    :param repos: list of selected repos
    :param period_type: rolling window filter
    :param metric_x: x-axis metric
    :param metric_y: y-axis metric
    """
    if "all" not in [r.lower() for r in repos]:
        df = df.filter(pl.col("repo").is_in(repos))

    if period_type and period_type.lower() != "all":
        df = df.filter(pl.col("period_type") == period_type)

    df_pd = df.select(["repo", metric_x, metric_y, "period_type"]).to_pandas()

    title = f"Scatter: {metric_x.capitalize().replace('_',' ')} vs. {metric_y.capitalize().replace('_',' ')}"

    fig = px.scatter(
        df_pd,
        x=metric_x,
        y=metric_y,
        color="repo",
        title=title,
        hover_data=["repo", "period_type"],
    )
    fig.update_layout(xaxis_title=metric_x, yaxis_title=metric_y, legend_title="Repository")
    return fig


def main():
    st.title("GitHub Metrics Explorer")

    # Sidebar: Upload CSV file.
    st.sidebar.header("Upload Data")
    uploaded_file = st.sidebar.file_uploader("Upload CSV file", type=["csv"])

    # Check if the file is uploaded.
    if uploaded_file is None:
        st.info("Please upload a CSV file to get started.")
        st.stop()

    # Load the CSV data.
    file_bytes = uploaded_file.getvalue()
    df = load_data(file_bytes)

    # Determine which column to use for the x-axis.
    x_axis = "period_end" if "period_end" in df.columns else "period_start"

    # Prepare for filters in the sidebar.
    st.sidebar.header("Filters")

    # Repository selector: We allow multiple repos AND an "all" option.
    unique_repos = sorted(df["repo"].unique().to_list())
    if "all" not in [r.lower() for r in unique_repos]:
        unique_repos.insert(0, "all")
    selected_repo = st.sidebar.selectbox(
        "Select Repository for Single-Metric Plot", unique_repos, index=len(unique_repos) - 1
    )

    # Identify numeric columns (metrics).
    exclude_cols = {"repo", "period_start", "period_end", "period_type"}
    numeric_types = {
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
        pl.Float32,
        pl.Float64,
    }
    metric_options = [
        col for col in df.columns if col not in exclude_cols and df.schema[col] in numeric_types
    ]

    if not metric_options:
        st.error("No numeric metric columns found in the data.")
        st.stop()

    # Single-metric selection for the single-metric time-series section.
    selected_metric = st.sidebar.selectbox(
        "Select Metric for Single-Metric Plot", metric_options, index=len(metric_options) - 1
    )

    # Display the metric description in the sidebar for single metric.
    st.sidebar.subheader("Metric Description")
    metric_description = METRIC_DESCRIPTIONS.get(selected_metric, "No description available.")
    st.sidebar.write(metric_description)

    # Option to show markers on the single-metric chart.
    markers = st.sidebar.checkbox("Show Markers", value=False)

    # === Single Metric Over Time ===
    st.header("Single Metric over Time")
    fig_single = plot_repo_metric_over_time(df, selected_repo, selected_metric, x_axis, markers)
    st.plotly_chart(fig_single, use_container_width=True)

    # === Multiple Repositories Over Time ===
    st.header("Multiple Repositories Over Time")
    st.write("Compare a single metric across multiple repositories in a chosen rolling window.")

    # Multi-select repos
    multi_repo_selection = st.multiselect(
        "Select Repositories (or 'all')", unique_repos, default=["all"]
    )

    # Select rolling window (period_type) if you have multiple
    # If you only have one type in your data, you can skip or hide it.
    period_types_available = sorted(df["period_type"].unique().to_list())
    if "all" not in [pt.lower() for pt in period_types_available]:
        period_types_available.insert(0, "all")
    selected_period_type = st.selectbox(
        "Select Rolling Window (Period Type)", period_types_available
    )

    # Metric to plot
    selected_metric_multi = st.selectbox("Select Metric for Multi-Repo Plot", metric_options)

    # Chart type selection
    chart_type_options = ["Line", "Stacked Area", "Side-by-Side Bar"]
    selected_chart_type = st.radio("Select Chart Type", chart_type_options, index=0)

    # Button to generate the multi-repo chart
    if st.button("Generate Multi-Repo Chart"):
        fig_multi = plot_multi_repo_metric_over_time(
            df,
            repos=multi_repo_selection,
            period_type=selected_period_type,
            metric=selected_metric_multi,
            x_axis=x_axis,
            chart_type=selected_chart_type,
        )
        st.plotly_chart(fig_multi, use_container_width=True)

    # === Scatter Plot of Two Metrics ===
    st.header("Scatter Plot of Two Metrics")
    st.write(
        "Plot any two metrics to see how they correlate across repositories in a selected window."
    )

    # Multi-select repos for scatter
    scatter_repos = st.multiselect(
        "Select Repositories for Scatter (or 'all')", unique_repos, default=["all"]
    )

    # Period type for scatter
    selected_period_type_scatter = st.selectbox(
        "Select Rolling Window (Period Type) for Scatter", period_types_available
    )

    # Select two metrics for scatter
    metric_x = st.selectbox("Select X-axis Metric", metric_options, index=0)
    metric_y = st.selectbox(
        "Select Y-axis Metric", metric_options, index=1 if len(metric_options) > 1 else 0
    )

    # Button to generate scatter
    if st.button("Generate Scatter"):
        if metric_x == metric_y:
            st.warning("Please select two different metrics for a meaningful scatter plot.")
        else:
            fig_scatter = plot_scatter_two_metrics(
                df, scatter_repos, selected_period_type_scatter, metric_x, metric_y
            )
            st.plotly_chart(fig_scatter, use_container_width=True)


if __name__ == "__main__":
    main()
