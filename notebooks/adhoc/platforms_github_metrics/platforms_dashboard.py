"""
Streamlit Dashboard for GitHub Metrics
"""

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

# Set custom config at the top.
st.set_page_config(
    page_title="[Platforms] GH Metrics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get Help": "https://github.com/your-repo",
        "Report a bug": "https://github.com/your-repo/issues",
        "About": "# GitHub Metrics Dashboard\nAn interactive dashboard for GitHub repository metrics."
    }
)


@st.cache_data
def load_data() -> pd.DataFrame:
    """
    Load the dataset and parse dates.

    Returns:
        A pandas DataFrame containing all GitHub timeseries data.
    """
    df = pd.read_csv("data/github_timeseries_dataset.csv")
    df["date"] = pd.to_datetime(df["date"])
    return df


df = load_data()

# Identify numeric columns (excluding "repo" and "date").
numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
available_metrics = numeric_cols

# Capture unique repositories for later use in the "Repository Details" tab.
repos = df["repo"].unique()

# Color mapping for repos (used in Overview plots).
repo_color_map = px.colors.qualitative.Plotly
repo_colors = {repo: repo_color_map[i % len(repo_color_map)] for i, repo in enumerate(repos)}

# Color mapping for metrics (used in Repository Details plots).
metric_list = available_metrics
metric_color_map = px.colors.qualitative.Plotly
metric_colors = {
    metric: metric_color_map[i % len(metric_color_map)] for i, metric in enumerate(metric_list)
}

# Sidebar navigation
# st.sidebar.image("notebooks/platforms_github_metrics/test.png", width=150)  # Replace with your logo's path
st.sidebar.title("Navigation")
tabs = ["Overview", "Repository Details"]
selected_tab = st.sidebar.radio("Select a Tab", tabs)

# -----------------------------------------------------------------------------
# Logo at the top-left
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# OVERVIEW TAB
# -----------------------------------------------------------------------------
if selected_tab == "Overview":
    st.title("GitHub Metrics Dashboard - Overview")

    # -------------------------------
    # 1) Filtering and Rolling Setup
    # -------------------------------
    st.subheader("Filters")

    selected_repos = st.multiselect(
        "Select Repositories to Include",
        options=repos,
        default=list(repos),
        help="Choose which repositories to visualize. If none are selected, no data will be shown."
    )

    window_size = st.slider(
        "Rolling Window Size (Days)",
        min_value=1,
        max_value=30,
        value=1,
        help="Apply a rolling average to each repository's metric. Set to 1 for raw values."
    )

    show_distribution = st.checkbox(
        "Show Distribution Plots Instead of Time-Series Plots",
        value=False,
        help="When checked, displays distribution plots (box plots) instead of time-series plots."
    )

    show_aggregate = st.checkbox(
        "Show Aggregate Across All Repositories",
        value=False,
        help="When checked, plots the aggregate sum across all selected repositories instead of individual repos."
    )

    # Filter the DataFrame based on selected repositories
    if selected_repos:
        overview_df = df[df["repo"].isin(selected_repos)].copy()
    else:
        overview_df = pd.DataFrame(columns=df.columns)  # empty if none selected

    if not overview_df.empty and available_metrics:
        # 2) Display aggregated key metrics in columns (sums across selected repos)
        st.subheader("Aggregated Key Metrics (Selected Repos)")
        metric_cols = st.columns(3)
        for i, metric in enumerate(available_metrics):
            metric_sum = overview_df[metric].sum()
            with metric_cols[i % 3]:
                st.metric(
                    label=metric.replace("_", " ").title(),
                    value=f"{metric_sum:.2f}"
                )

        # 3) Plots: Time-Series, Distribution, or Aggregate
        st.subheader("Plots for Each Metric")
        for metric in available_metrics:
            # Make a copy and sort for rolling operations
            plot_df = overview_df[["date", "repo", metric]].copy()
            plot_df = plot_df.sort_values(["repo", "date"])

            # Apply rolling if window_size > 1
            if window_size > 1:
                rolled_list = []
                for repo_name in plot_df["repo"].unique():
                    subset = plot_df[plot_df["repo"] == repo_name].copy()
                    subset[metric] = subset[metric].rolling(window=window_size).mean()
                    # Drop rows with NaN from the rolling window
                    subset.dropna(subset=[metric], inplace=True)
                    rolled_list.append(subset)
                plot_df = pd.concat(rolled_list, ignore_index=True)
            # If window_size == 1, no rolling needed

            if show_aggregate:
                # Plot Aggregate (Sum Across All Repositories)
                agg_df = (
                    plot_df.groupby("date", as_index=False)[metric].sum().dropna(subset=[metric])
                )
                if show_distribution:
                    # Aggregate Distribution Plot (Box Plot)
                    fig_plot = px.box(
                        overview_df,
                        x="date",
                        y=metric,
                        title=f"Aggregate Distribution of {metric.replace('_', ' ').title()}",
                        labels={"date": "Date", metric: metric.replace("_", " ").title()},
                    )
                else:
                    # Aggregate Time-Series Plot
                    fig_plot = px.line(
                        agg_df,
                        x="date",
                        y=metric,
                        title=f"Aggregate {metric.replace('_', ' ').title()} Over Time",
                        color_discrete_sequence=["#636EFA"],
                        labels={
                            "date": "Date",
                            metric: metric.replace("_", " ").title()
                        },
                    )
            else:
                if show_distribution:
                    # Distribution Plot (Box Plot)
                    fig_plot = px.box(
                        overview_df,
                        x="repo",
                        y=metric,
                        color="repo",
                        title=f"Distribution of {metric.replace('_', ' ').title()}",
                        color_discrete_map=repo_colors,
                        labels={"repo": "Repository", metric: metric.replace("_", " ").title()},
                    )
                else:
                    # Time-Series Plot
                    fig_plot = px.line(
                        plot_df,
                        x="date",
                        y=metric,
                        color="repo",
                        title=f"{metric.replace('_', ' ').title()} Over Time",
                        color_discrete_map=repo_colors,
                        labels={
                            "date": "Date",
                            "repo": "Repository",
                            metric: metric.replace("_", " ").title()
                        },
                    )

            # Show Plot
            st.plotly_chart(fig_plot, use_container_width=True)

    else:
        st.warning("No data to display. Please select at least one repository.")

# -----------------------------------------------------------------------------
# REPOSITORY DETAILS TAB
# -----------------------------------------------------------------------------
elif selected_tab == "Repository Details":
    st.title("GitHub Metrics Dashboard - Repository Details")

    # Select a repository
    selected_repo = st.selectbox("Select a Repository", repos)
    repo_df = df[df["repo"] == selected_repo].copy()

    st.markdown(f"### Key Metrics for {selected_repo}")

    # Display key metrics in a row of columns (if they exist)
    col1, col2, col3 = st.columns(3)
    with col1:
        if "number_of_prs" in numeric_cols:
            total_prs = int(repo_df["number_of_prs"].sum())
            st.metric(label="Total PRs", value=total_prs)
        else:
            st.write("No 'number_of_prs' metric found.")

    with col2:
        if "avg_time_to_merge_days" in numeric_cols:
            avg_merge_time = repo_df["avg_time_to_merge_days"].mean()
            st.metric(label="Avg Time to Merge (Days)", value=f"{avg_merge_time:.2f}")
        else:
            st.write("No 'avg_time_to_merge_days' metric found.")

    with col3:
        if "approval_ratio" in numeric_cols:
            approval_ratio = repo_df["approval_ratio"].mean() * 100
            st.metric(label="Approval Ratio", value=f"{approval_ratio:.2f}%")
        else:
            st.write("No 'approval_ratio' metric found.")

    st.markdown(f"### Rolling Averages for {selected_repo}")
    details_window_size = st.slider(
        "Select Rolling Window Size (Days)",
        min_value=1,
        max_value=30,
        value=7,
        help="Apply a rolling average to each metric for the selected repository. Set to 1 for raw values."
    )

    if not repo_df.empty and available_metrics:
        # Two columns for rolling average plots
        col_c, col_d = st.columns(2)
        for i, metric in enumerate(available_metrics):
            subset = repo_df[["date", metric]].copy().sort_values("date")

            if details_window_size > 1:
                subset[metric] = subset[metric].rolling(window=details_window_size).mean()
                subset.dropna(subset=[metric], inplace=True)

            fig_rolling = px.line(
                subset,
                x="date",
                y=metric,
                title=f"{details_window_size}-Day Rolling Avg of {metric.replace('_', ' ').title()} - {selected_repo}",
                color_discrete_sequence=[metric_colors.get(metric, "#636EFA")],
                labels={"date": "Date", metric: "Rolling Average"}
            )

            if i % 2 == 0:
                with col_c:
                    st.plotly_chart(fig_rolling, use_container_width=True)
            else:
                with col_d:
                    st.plotly_chart(fig_rolling, use_container_width=True)
    else:
        st.warning("No numeric metrics available for rolling averages.")
