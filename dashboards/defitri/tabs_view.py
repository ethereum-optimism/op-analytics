# dashboards/defitri/tabs_view.py

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


@st.cache_data
def filter_df(df: pd.DataFrame, min_tvl_millions: float, selected_ranks: list[str]) -> pd.DataFrame:
    """
    Filter logic, cached by arguments. If min_tvl_millions or selected_ranks change,
    the result is recomputed. If not, we reuse the old result.
    """
    if df.empty:
        return df

    # Filter by minimum TVL
    min_tvl = min_tvl_millions * 1_000_000
    tvl_stats = df.groupby("display_name")["TVL"].mean()
    keepers = tvl_stats[tvl_stats >= min_tvl].index.tolist()

    if not keepers:
        # fallback to at least 5 highest if possible
        sorted_tvl = tvl_stats.sort_values(ascending=False)
        if len(sorted_tvl) >= 5:
            pivot_tvl = sorted_tvl.iloc[4] * 0.9
            keepers = sorted_tvl[sorted_tvl >= pivot_tvl].index.tolist()
        else:
            keepers = sorted_tvl.index.tolist()

    newdf = df[df["display_name"].isin(keepers)].copy()

    if "composite_rank" in newdf.columns:
        del newdf["composite_rank"]

    if selected_ranks:
        newdf["composite_rank"] = newdf[selected_ranks].mean(axis=1)

    return newdf


def render_main_tabs():
    """
    The main tab-based UI for DeFi Tri. We assume st.session_state["df"] is loaded and non-empty.
    """
    df = st.session_state["df"]
    if df.empty:
        st.warning("No data in session state. Please load data first.")
        return

    # Initialize defaults if not present
    st.session_state.setdefault("min_tvl_millions", 50.0)
    st.session_state.setdefault("top_n_chains", 10)
    if "selected_ranks" not in st.session_state:
        rank_cols = [c for c in df.columns if c.endswith("_Rank")]
        st.session_state["selected_ranks"] = rank_cols[:4]
    st.session_state.setdefault("filters_applied", False)
    st.session_state.setdefault("filtered_df", pd.DataFrame())
    st.session_state.setdefault("viz_rank_metric", None)
    st.session_state.setdefault("selected_chain", None)

    # Create tabs across the top
    tab1, tab2 = st.tabs(["Rankings Overview", "Chain Deep Dive"])

    with tab1:
        show_filter_and_overview(df)

    with tab2:
        show_chain_deep_dive()


def show_filter_and_overview(df: pd.DataFrame):
    st.subheader("Rankings Overview")

    col1, col2 = st.columns([2, 1])
    with col1:
        st.session_state.min_tvl_millions = st.number_input(
            "Minimum Average TVL (millions)",
            min_value=0.0,
            value=st.session_state.min_tvl_millions,
            step=10.0,
        )

        all_rank_cols = [c for c in df.columns if c.endswith("_Rank")]
        st.session_state.selected_ranks = st.multiselect(
            "Ranking Columns for Composite Score",
            all_rank_cols,
            default=st.session_state.selected_ranks,
        )

    with col2:
        st.session_state.top_n_chains = st.number_input(
            "Top N Chains",
            min_value=1,
            max_value=50,
            value=st.session_state.top_n_chains,
        )

        if st.button("Apply Filters"):
            st.session_state.filters_applied = True

    if st.session_state.filters_applied or st.session_state["filtered_df"].empty:
        filtered = filter_df(
            df=df,
            min_tvl_millions=st.session_state.min_tvl_millions,
            selected_ranks=st.session_state.selected_ranks,
        )
        st.session_state["filtered_df"] = filtered
        st.session_state.filters_applied = False

    filtered_df = st.session_state["filtered_df"]

    if filtered_df.empty:
        st.warning("Filtered data is empty. Try lowering min TVL or re-check your rank columns.")
        return

    with st.expander("Raw Data"):
        st.dataframe(filtered_df)

    st.subheader("Ranking Overview")

    rank_cols = [c for c in filtered_df.columns if c.endswith("_Rank")]
    has_composite = "composite_rank" in filtered_df.columns
    default_rank = "composite_rank" if has_composite else (rank_cols[0] if rank_cols else None)

    if default_rank is None:
        st.warning("No rank columns found in filtered data.")
        return

    # Build the final rank options
    final_rank_opts = list(st.session_state.selected_ranks)
    if has_composite and "composite_rank" not in final_rank_opts:
        final_rank_opts.insert(0, "composite_rank")

    if (
        not st.session_state.viz_rank_metric
        or st.session_state.viz_rank_metric not in final_rank_opts
    ):
        st.session_state.viz_rank_metric = default_rank

    chosen_rank = st.selectbox(
        "Rank Metric for Chart",
        final_rank_opts,
        index=final_rank_opts.index(st.session_state.viz_rank_metric),
    )
    st.session_state.viz_rank_metric = chosen_rank

    bump_chart(filtered_df, chosen_rank, st.session_state.top_n_chains)


def bump_chart(df: pd.DataFrame, rank_col: str, top_n: int) -> None:
    if df.empty:
        return

    # Bump chart logic
    df_dates = sorted(df["dt"].dropna().unique())

    # Filter out the last few dates which might have incomplete data
    days_to_exclude = 3  # Excluding last 3 days of data that might be incomplete
    if len(df_dates) > days_to_exclude:
        df_dates = df_dates[:-days_to_exclude]

    top_set = set()
    for d in df_dates:
        day_slice = df[df["dt"] == d].sort_values(rank_col).head(top_n)
        top_set.update(day_slice["display_name"].tolist())

    # Filter the data to exclude the last few days and only include top chains
    chart_df = df[(df["display_name"].isin(top_set)) & (df["dt"].isin(df_dates))].copy()

    # Get all rank columns for hover data
    rank_cols = [col for col in chart_df.columns if col.endswith("_Rank")]

    # Create hover_data dictionary to control what's shown in tooltips
    hover_data = {"display_name": True}

    # Add all rank columns to hover data
    for col in rank_cols:
        hover_data[col] = True

    # Create the plot with enhanced hover data
    fig = px.line(
        chart_df,
        x="dt",
        y=rank_col,
        color="display_name",
        markers=True,
        hover_data=hover_data,
        title=f"Top {top_n} by {rank_col}",
    )

    # Find the end points of each line to add labels
    end_points = {}
    max_date = max(df_dates)

    for chain in top_set:
        # Get the last data point for this chain
        chain_end = chart_df[(chart_df["display_name"] == chain) & (chart_df["dt"] == max_date)]
        if not chain_end.empty:
            end_points[chain] = {"x": max_date, "y": chain_end[rank_col].values[0]}

    # Add chain name annotations at the end of each line
    for chain, point in end_points.items():
        fig.add_annotation(
            x=point["x"],
            y=point["y"],
            text=f"  {chain}  ",
            showarrow=False,
            xanchor="left",
            yanchor="middle",
            xshift=5,  # Small shift to the right
            bgcolor="rgba(255, 255, 255, 0.7)",  # Semi-transparent white background
            bordercolor="rgba(0, 0, 0, 0.3)",
            borderwidth=1,
            borderpad=3,
            font=dict(size=11),
        )

    # Correct the y-axis to show exactly ranks 1 to top_n
    fig.update_layout(
        yaxis=dict(
            range=[top_n + 0.5, 0.5],  # Reversed range: high value to low value
            title="Rank",
            dtick=1,  # Show integer ticks
            autorange=False,  # Disable autorange to ensure our range is used
        ),
        height=800,
        hovermode="closest",
        # Add margin to the right to accommodate chain labels
        margin=dict(r=150),
    )

    st.plotly_chart(fig, use_container_width=True)


def show_chain_deep_dive():
    st.subheader("Chain Deep Dive")

    df = st.session_state["filtered_df"]
    if df.empty:
        st.warning("No filtered data to explore. Check the Filter & Overview tab first.")
        return

    chain_names = sorted(df["display_name"].dropna().unique())
    if not chain_names:
        st.warning("No chain data available after filtering.")
        return

    if st.session_state.selected_chain not in chain_names:
        st.session_state.selected_chain = chain_names[0]

    # Set default index for OP Mainnet if it exists in the list
    default_index = chain_names.index(st.session_state.selected_chain)
    if "OP Mainnet" in chain_names:
        default_index = chain_names.index("OP Mainnet")

    chosen_chain = st.selectbox(
        "Chain to Explore",
        chain_names,
        index=default_index,
        key="deep_dive_chain_select",
    )
    st.session_state.selected_chain = chosen_chain

    chain_df = df[df["display_name"] == chosen_chain]
    if chain_df.empty:
        st.info("No data for this chain under current filters.")
        return

    # Plot chain ranks vs. average
    rank_cols = [c for c in df.columns if c.endswith("_Rank")]
    if not rank_cols:
        st.warning("No rank columns found in filtered data.")
        return

    # 1. Rank history chart (without average lines)
    st.subheader(f"{chosen_chain}: Rank History")
    fig = go.Figure()

    for r in rank_cols:
        fig.add_trace(
            go.Scatter(
                x=chain_df["dt"],
                y=chain_df[r],
                mode="lines+markers",
                name=r.replace("_Rank", ""),
            )
        )

    fig.update_layout(
        title=f"Chain {chosen_chain}: Rank History",
        xaxis_title="Date",
        yaxis=dict(
            autorange="reversed",
            title="Rank",
        ),
        height=500,
    )
    st.plotly_chart(fig, use_container_width=True)

    # 2. Rank Distribution Chart
    st.subheader(f"{chosen_chain}: Rank Distribution")

    # Create a box plot to show the distribution of ranks
    fig_dist = px.box(
        chain_df,
        y=[col for col in rank_cols],
        labels={"value": "Rank", "variable": "Metric"},
        title=f"Distribution of Rankings for {chosen_chain}",
    )

    # Customize box plot
    fig_dist.update_layout(
        yaxis=dict(
            autorange="reversed",
            title="Rank",
        ),
        height=400,
    )
    # Update x-axis tick labels to remove "_Rank" suffix
    fig_dist.update_xaxes(
        ticktext=[col.replace("_Rank", "") for col in rank_cols],
        tickvals=rank_cols,
    )

    st.plotly_chart(fig_dist, use_container_width=True)

    # 3. Percentiles over time
    st.subheader(f"{chosen_chain}: Percentile Performance")

    # Find the value columns (those that don't end with _Rank)
    base_cols = []
    for col in df.columns:
        if col.endswith("_Rank"):
            base_name = col.replace("_Rank", "")
            if base_name in df.columns:
                base_cols.append(base_name)

    if base_cols:
        # Create percentile dataframe
        percentile_df = pd.DataFrame(index=chain_df["dt"].unique())

        for col in base_cols:
            # For each date, calculate the chain's percentile rank for this metric
            for date in percentile_df.index:
                date_df = df[df["dt"] == date]
                if not date_df.empty and col in date_df.columns:
                    chain_value = chain_df[chain_df["dt"] == date][col].values
                    if len(chain_value) > 0:
                        all_values = date_df[col].dropna()
                        if not all_values.empty:
                            # Get the chain value and ensure it's not pd.NA or None
                            chain_val = chain_value[0]
                            if pd.notna(chain_val):
                                percentile = (all_values < chain_val).mean() * 100
                                percentile_df.at[date, col] = percentile

        # Drop any NA values to prevent serialization errors
        percentile_df = percentile_df.dropna(how="all")

        # Only create the chart if we have valid data
        if not percentile_df.empty:
            fig_pct = go.Figure()

            for col in base_cols:
                if col in percentile_df.columns:
                    # Filter out any NA values in this column
                    valid_data = percentile_df[col].dropna()
                    if not valid_data.empty:
                        fig_pct.add_trace(
                            go.Scatter(
                                x=valid_data.index,
                                y=valid_data.values,
                                mode="lines+markers",
                                name=col,
                            )
                        )

            # Only proceed if we added at least one trace
            if len(fig_pct.data) > 0:
                fig_pct.update_layout(
                    title=f"Percentile Rankings Over Time for {chosen_chain}",
                    xaxis_title="Date",
                    yaxis=dict(
                        title="Percentile (%)",
                        range=[0, 100],
                    ),
                    height=500,
                )

                # Add reference lines for quartiles
                date_min = percentile_df.index.min()
                date_max = percentile_df.index.max()

                if pd.notna(date_min) and pd.notna(date_max):
                    fig_pct.add_shape(
                        type="line",
                        x0=date_min,
                        x1=date_max,
                        y0=75,
                        y1=75,
                        line=dict(color="green", width=1, dash="dash"),
                    )
                    fig_pct.add_shape(
                        type="line",
                        x0=date_min,
                        x1=date_max,
                        y0=50,
                        y1=50,
                        line=dict(color="orange", width=1, dash="dash"),
                    )
                    fig_pct.add_shape(
                        type="line",
                        x0=date_min,
                        x1=date_max,
                        y0=25,
                        y1=25,
                        line=dict(color="red", width=1, dash="dash"),
                    )

                st.plotly_chart(fig_pct, use_container_width=True)

                # Explanation text
                st.info(
                    "The percentile chart shows how this chain ranks compared to others. Higher percentiles (closer to 100%) are better, indicating the chain outperforms more of its peers."
                )
            else:
                st.warning("Insufficient data to generate percentile chart.")
        else:
            st.warning("No valid percentile data available for this chain.")
    else:
        st.warning("No base value columns found to calculate percentiles.")
