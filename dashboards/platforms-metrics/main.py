import streamlit as st
import pandas as pd
from utils import load_github_pr_data, DATE_COLUMN_END
from tabs import tab_main_metrics, tab_timing, tab_dynamic_analysis


def main():
    st.set_page_config(
        page_title="Platforms Metrics Dashboard", layout="wide", initial_sidebar_state="expanded"
    )
    st.title("Platforms Metrics Dashboard")
    with st.expander("About"):
        st.markdown(
            """
            * This dashboard shows metrics for GitHub PRs over time.
            * The lookback window is the rolling or fixed interval used to group these PR metrics.
            * Data is refreshed daily.
            """
        )

    # Sidebar Filters
    st.sidebar.header("Data Filters")
    with st.spinner("Loading GitHub PR metrics..."):
        data = load_github_pr_data()

    exclude_cols = {"repo", DATE_COLUMN_END, "period_type", "dt"}
    numeric_cols = [
        col
        for col in data.columns
        if col not in exclude_cols and pd.api.types.is_numeric_dtype(data[col])
    ]

    if not numeric_cols:
        st.warning("No numeric columns found in the dataset.")
        st.stop()

    # Date Filter
    min_date_default = data[DATE_COLUMN_END].min().date()
    min_date = st.sidebar.date_input("Filter by Minimum Date", value=min_date_default)

    # Repository and Lookback Window Filters
    st.sidebar.header("Repository and Lookback Window")
    repo_list = sorted(list(data["repo"].unique()))
    repo = st.sidebar.selectbox(
        "Repository",
        repo_list,
        index=repo_list.index("optimism") if "optimism" in repo_list else 0,
        help="Select a GitHub repository to filter metrics",
    )
    period_type_list = sorted(list(data["period_type"].unique()))
    lookback_window = st.sidebar.selectbox(
        "Lookback Window (rolling)",
        period_type_list,
        index=period_type_list.index("month") if "month" in period_type_list else 0,
        help="Choose the time window for aggregating metrics (e.g. week, month, quarter)",
    )

    # Additional sidebar info
    with st.sidebar.expander("What's a Lookback Window?"):
        st.markdown(
            "A **Lookback Window** is the rolling or fixed interval used to group PR metrics. It affects previous period comparisons."
        )

    # Filter the data
    filtered_data = data[data[DATE_COLUMN_END] >= pd.to_datetime(min_date)]
    if repo:
        filtered_data = filtered_data[filtered_data["repo"] == repo]
    if lookback_window:
        filtered_data = filtered_data[filtered_data["period_type"] == lookback_window]

    # Create Tabs and call each tab's render function
    tab1, tab2, tab3 = st.tabs(["Main metrics", "Timing", "Dynamic Analysis"])

    with tab1:
        tab_main_metrics.render_tab(data, filtered_data, numeric_cols, repo, lookback_window)

    with tab2:
        # For the timing tab, pass latest/previous data based on your own logic.
        # You might want to compute these once and pass them along.
        sorted_data = filtered_data.sort_values(by=DATE_COLUMN_END, ascending=False)
        latest_data = sorted_data.head(1)
        # Find previous data similarly as done in tab_main_metrics if needed:
        previous_data = sorted_data.tail(1)
        tab_timing.render_tab(
            filtered_data, numeric_cols, latest_data, previous_data, lookback_window
        )

    with tab3:
        tab_dynamic_analysis.render_tab(data, numeric_cols, lookback_window)


if __name__ == "__main__":
    main()
