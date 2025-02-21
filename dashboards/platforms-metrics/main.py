import streamlit as st
import pandas as pd

# Import the local modules we just split out
import tabs.tab_main_metrics as tab1
import tabs.tab_timing as tab2
import tabs.tab_dynamic_analysis as tab3
from utils import (
    DATE_COLUMN_START,
    DATE_COLUMN_END,
    load_github_pr_data,
    get_prev_date_offset,
)


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
        index=repo_list.index("optimism") if "optimism" in repo_list else 0,
        help="Which repository's metrics?",
    )

    # Lookback Window
    period_type_list = sorted(list(data["period_type"].unique()), key=lambda x: len(x))
    lookback_window = st.sidebar.selectbox(
        "Lookback Window (rolling)",
        period_type_list,
        index=period_type_list.index("month") if "month" in period_type_list else 0,
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

    # Create tabs
    tab_main, tab_timing, tab_dynamic = st.tabs(["Main metrics", "Timing", "Dynamic Analysis"])

    with tab_main:
        tab1.render_tab1(
            filtered_data=filtered_data,
            numeric_cols=numeric_cols,
            latest_data=latest_data,
            previous_data=previous_data,
            repo=repo,
            lookback_window=lookback_window,
        )

    with tab_timing:
        tab2.render_tab2(
            filtered_data=filtered_data,
            numeric_cols=numeric_cols,
            latest_data=latest_data,
            previous_data=previous_data,
        )

    with tab_dynamic:
        tab3.render_tab3(data, lookback_window, numeric_cols)


if __name__ == "__main__":
    main()
