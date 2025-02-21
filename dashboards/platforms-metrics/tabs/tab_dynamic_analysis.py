import pandas as pd
import streamlit as st
from utils import plot_scatter_two_metrics


def render_tab(data: pd.DataFrame, numeric_cols: list, lookback_window: str):
    st.subheader("Scatter Plot: Compare Any Two Metrics")
    st.markdown(
        "Select repositories, choose two numeric metrics, and optionally add a regression line. "
        "The scatter plot will use the lookback window selected in the sidebar."
    )

    multi_repos = list(data["repo"].unique())
    multi_repos.insert(0, "all")
    selected_repos = st.multiselect("Select Repositories", multi_repos, default=["optimism"])
    metric_x = st.selectbox("Select X-axis Metric", numeric_cols, index=0)
    metric_y = st.selectbox(
        "Select Y-axis Metric", numeric_cols, index=1 if len(numeric_cols) > 1 else 0
    )

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        color_dimension = (
            "repo"
            if st.toggle("Color by Repository", value=True, help="Color points by repository.")
            else None
        )
    with col2:
        use_log_x = st.toggle("Log Scale X", value=False, help="Use log scale for X-axis")
    with col3:
        use_log_y = st.toggle("Log Scale Y", value=False, help="Use log scale for Y-axis")
    with col4:
        show_reg = st.toggle("Show Regression Line", value=True, help="Display a regression line")

    if metric_x == metric_y:
        st.warning("Please select two different metrics.")
    else:
        fig_scatter = plot_scatter_two_metrics(
            df=data,
            repos=selected_repos,
            window=lookback_window,
            metric_x=metric_x,
            metric_y=metric_y,
            color_by=color_dimension,
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
