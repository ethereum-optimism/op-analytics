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

    # Sidebar: Filters.
    st.sidebar.header("Filters")

    # Repository selector: include an "all" option.
    repo_options = sorted(df["repo"].unique().to_list())
    repo_options.insert(0, "all")
    selected_repo = st.sidebar.selectbox(
        "Select Repository", repo_options, index=len(repo_options) - 1
    )

    # Metric selector: we exclude non-metric columns.
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

    selected_metric = st.sidebar.selectbox(
        "Select Metric", metric_options, index=len(metric_options) - 1
    )

    # Display the metric description in the sidebar.
    st.sidebar.subheader("Metric Description")
    metric_description = METRIC_DESCRIPTIONS.get(selected_metric, "No description available.")
    st.sidebar.write(metric_description)

    # Option to show markers on the chart.
    markers = st.sidebar.checkbox("Show Markers", value=False)

    # Plot the time series chart.
    st.header("Single Metric over Time")
    fig = plot_repo_metric_over_time(df, selected_repo, selected_metric, x_axis, markers)
    st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()
