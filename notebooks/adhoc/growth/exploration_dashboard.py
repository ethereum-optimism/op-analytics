import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


# For broader Streamlit version support, we use @st.cache_data (works in 1.18+).
# If you have an older version of Streamlit, swap to @st.cache.
@st.cache_data
def load_csv_as_df(uploaded_file):
    """
    Load the uploaded CSV file into a Pandas DataFrame.
    """
    return pd.read_csv(uploaded_file, parse_dates=["dt"], infer_datetime_format=True, index_col=0)


def resample_df(df: pd.DataFrame, date_col: str, freq: str, agg_method: str) -> pd.DataFrame:
    """
    Resample the DataFrame by 'dt' using the given frequency and aggregation method.
    numeric_only=True ensures we ignore non-numeric columns (e.g., 'chain').
    """
    df = df.set_index(date_col)

    if agg_method == "sum":
        df_resampled = df.resample(freq).sum(numeric_only=True)
    elif agg_method == "max":
        df_resampled = df.resample(freq).max(numeric_only=True)
    elif agg_method == "min":
        df_resampled = df.resample(freq).min(numeric_only=True)
    else:  # mean
        df_resampled = df.resample(freq).mean(numeric_only=True)

    df_resampled.dropna(how="all", inplace=True)
    df_resampled.reset_index(inplace=True)
    return df_resampled


def compute_corr_matrix(df: pd.DataFrame, metrics: list) -> pd.DataFrame:
    """
    Compute a correlation matrix among the selected metrics.
    """
    return df[metrics].corr()


def plot_correlation_heatmap(corr_matrix: pd.DataFrame, title: str = "Correlation Heatmap"):
    """
    Create a Plotly heatmap for the given correlation matrix.
    """
    fig = go.Figure(
        data=go.Heatmap(
            z=corr_matrix.values,
            x=corr_matrix.columns,
            y=corr_matrix.index,
            colorscale="RdBu",
            zmin=-1,
            zmax=1,
        )
    )
    fig.update_layout(
        title=title,
        xaxis_nticks=len(corr_matrix.columns),
        yaxis_nticks=len(corr_matrix.index),
        width=700,
        height=700,
    )
    return fig


def main():
    st.title("Blockchain Metrics Correlation Explorer")

    # 1. Sidebar for CSV Upload
    st.sidebar.header("Data Upload")
    uploaded_file = st.sidebar.file_uploader("Upload CSV file", type=["csv"])

    if not uploaded_file:
        st.info("Please upload a CSV to begin.")
        st.stop()

    # 2. Load the CSV into a DataFrame
    df = load_csv_as_df(uploaded_file)

    if df.empty:
        st.warning("The uploaded CSV has no rows or failed to load properly.")
        st.stop()

    # 3. Confirm required columns exist
    if "dt" not in df.columns:
        st.error("No 'dt' column found in your data. Please ensure you have a column named 'dt'.")
        st.stop()

    st.subheader("Data Preview")
    st.write(df.head())

    # 4. Filter by chain if present
    if "chain" in df.columns:
        st.sidebar.subheader("Chain Filter")
        chains = df["chain"].dropna().unique().tolist()
        chains.sort()
        chain_options = ["All"] + chains
        selected_chain = st.sidebar.selectbox("Select Chain", chain_options)

        if selected_chain != "All":
            df = df[df["chain"] == selected_chain]

    # 5. Date Range Filter
    st.sidebar.subheader("Date Range Filter")
    df["dt"] = pd.to_datetime(df["dt"])
    min_date, max_date = df["dt"].min(), df["dt"].max()

    user_date_range = st.sidebar.date_input("Select Date Range", [min_date, max_date])
    if len(user_date_range) == 2:
        start_date, end_date = user_date_range
        df = df[(df["dt"] >= pd.to_datetime(start_date)) & (df["dt"] <= pd.to_datetime(end_date))]

    # 6. Resampling Options
    st.sidebar.subheader("Resampling")
    resample_options = {
        "No Resample (Daily)": None,
        "Weekly": "W",
        "Monthly": "M",
        "Quarterly": "Q",
    }
    selected_resample_label = st.sidebar.selectbox(
        "Frequency", list(resample_options.keys()), index=0
    )
    selected_freq = resample_options[selected_resample_label]

    agg_method = st.sidebar.selectbox("Aggregation Method", ["mean", "sum", "max", "min"], index=0)

    if selected_freq:
        df = resample_df(df, "dt", selected_freq, agg_method)

    # 7. Numeric Columns for Correlation
    #    We'll separate out to handle correlation and also for the scatter.
    numeric_cols = df.select_dtypes(include=["float", "int"]).columns.tolist()

    if not numeric_cols:
        st.warning("No numeric columns found in the data.")
        st.stop()

    # Let user pick columns to include in correlation
    st.subheader("Correlation Analysis")
    selected_metrics = st.multiselect(
        "Select numeric columns to analyze in the correlation matrix",
        numeric_cols,
        default=numeric_cols[:4] if len(numeric_cols) >= 4 else numeric_cols,
    )

    if len(selected_metrics) < 2:
        st.warning("Please select at least two metrics for correlation analysis.")
    else:
        # Compute correlation
        corr_matrix = compute_corr_matrix(df, selected_metrics)
        st.write("Correlation Matrix:")
        st.write(corr_matrix)

        # Plot heatmap
        heatmap_fig = plot_correlation_heatmap(corr_matrix, "Correlation Heatmap")
        st.plotly_chart(heatmap_fig, use_container_width=True)

    # 8. Scatter Plot with Lag & Smoothing
    st.subheader("Scatter Plot with Lag & Smoothing")
    st.write(
        "Visualize how two metrics relate to each other. Adjust lag and smoothing in real time."
    )

    scatter_col1, scatter_col2 = st.columns(2)
    with scatter_col1:
        x_metric = st.selectbox("X-Axis Metric", numeric_cols)
    with scatter_col2:
        y_metric = st.selectbox("Y-Axis Metric", numeric_cols, index=min(1, len(numeric_cols) - 1))

    # Sliders for lag (days) on X and Y
    # We'll allow negative to shift backward in time (metric leads),
    # and positive to shift forward in time (metric lags).
    st.write("### Lags (Days)")
    lag_col1, lag_col2 = st.columns(2)
    with lag_col1:
        x_lag = st.slider("Lag for X metric (days)", min_value=-30, max_value=30, value=0)
    with lag_col2:
        y_lag = st.slider("Lag for Y metric (days)", min_value=-30, max_value=30, value=0)

    # Sliders for smoothing windows on X and Y (1 = no smoothing)
    st.write("### Smoothing Window (Days)")
    smooth_col1, smooth_col2 = st.columns(2)
    with smooth_col1:
        x_smooth = st.slider("Smoothing window for X metric", 1, 30, value=1)
    with smooth_col2:
        y_smooth = st.slider("Smoothing window for Y metric", 1, 30, value=1)

    # Optionally color by chain if the column is present and user didn't filter to a single chain
    color_by_chain = False
    if "chain" in df.columns and selected_chain == "All":
        color_by_chain = st.checkbox("Color by chain?", value=False)

    # We'll create new columns for the shifted/smoothed data
    # so we don't change the original dataframe.
    # (We do it every run so the user sees immediate changes.)
    temp_df = df.copy()
    temp_df = temp_df.sort_values("dt")  # ensure it's sorted by time

    # Apply lag. If lag is positive, shift downward (value appears later).
    # If lag is negative, shift upward (value appears earlier).
    if x_lag != 0:
        temp_df[x_metric + "_x_lagged"] = temp_df[x_metric].shift(x_lag)
    else:
        temp_df[x_metric + "_x_lagged"] = temp_df[x_metric]

    if y_lag != 0:
        temp_df[y_metric + "_y_lagged"] = temp_df[y_metric].shift(y_lag)
    else:
        temp_df[y_metric + "_y_lagged"] = temp_df[y_metric]

    # Apply rolling mean smoothing. If window=1, it's effectively no smoothing.
    if x_smooth > 1:
        temp_df[x_metric + "_x_lagged_smooth"] = (
            temp_df[x_metric + "_x_lagged"].rolling(window=x_smooth, min_periods=1).mean()
        )
    else:
        temp_df[x_metric + "_x_lagged_smooth"] = temp_df[x_metric + "_x_lagged"]

    if y_smooth > 1:
        temp_df[y_metric + "_y_lagged_smooth"] = (
            temp_df[y_metric + "_y_lagged"].rolling(window=y_smooth, min_periods=1).mean()
        )
    else:
        temp_df[y_metric + "_y_lagged_smooth"] = temp_df[y_metric + "_y_lagged"]

    # Build the scatter
    # We only keep rows where both new columns are not NaN (since shifting can introduce NaNs).
    temp_df = temp_df.dropna(subset=[x_metric + "_x_lagged_smooth", y_metric + "_y_lagged_smooth"])

    fig = px.scatter(
        temp_df,
        x=x_metric + "_x_lagged_smooth",
        y=y_metric + "_y_lagged_smooth",
        color="chain" if color_by_chain else None,
        title=f"{x_metric} vs. {y_metric} (Lag & Smoothing Applied)",
        labels={
            x_metric + "_x_lagged_smooth": f"{x_metric} (Lag={x_lag}, Smooth={x_smooth})",
            y_metric + "_y_lagged_smooth": f"{y_metric} (Lag={y_lag}, Smooth={y_smooth})",
        },
    )
    st.plotly_chart(fig, use_container_width=True)

    st.success("Done! Adjust your filters, lags, and smoothing to explore further.")


if __name__ == "__main__":
    main()
