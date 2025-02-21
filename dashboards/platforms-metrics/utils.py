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
DATA_PATH = Path(__file__).parent / "data/github_pr_metrics.csv"
LOAD_CSV = False

# Mapping for friendlier labels
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
        # df.to_csv(Path(__file__).parent / "data/github_pr_metrics_v1.csv", index=False) # Debug
    df[DATE_COLUMN_START] = pd.to_datetime(df[DATE_COLUMN_START])
    df[DATE_COLUMN_END] = pd.to_datetime(df[DATE_COLUMN_END])
    df["period_type"] = df["period_type"].map(lambda x: PERIOD_TYPE_MAP[x])
    df.drop(columns=["review_requested_prs"], inplace=True)  # Todo: fix this metric
    df = df[df[DATE_COLUMN_START] >= pd.to_datetime("2024-01-01")]
    return df


def plot_line_chart(
    df: pd.DataFrame,
    x_col: str,
    y_cols: list,
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
    df: pd.DataFrame, x_col: str, y_cols: list, title: str = "Cumulative Metrics Over Time"
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
    df: pd.DataFrame, metric_cols: list, title: str = "Box Plot of Metrics", ylabel: str = ""
) -> None:
    melted_df = df[metric_cols].melt(var_name="Metric", value_name="Value")
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
    repos: list,
    window: str,
    metric_x: str,
    metric_y: str,
    color_by: str = "repo",
    log_x: bool = False,
    log_y: bool = False,
    show_regression: bool = False,
) -> go.Figure:
    all_selected = any(r.lower() == "all" for r in repos)
    if not all_selected:
        df = df[df["repo"].isin(repos)]
    if window and window.lower() != "all":
        df = df[df["period_type"] == window]
    plot_df = df[[metric_x, metric_y, "repo", "period_type"]].dropna().copy()
    x_label = FRIENDLY_LABELS.get(metric_x, metric_x)
    y_label = FRIENDLY_LABELS.get(metric_y, metric_y)
    title = f"Scatter: {x_label} vs. {y_label}"
    color_col = (
        "repo" if color_by == "repo" else ("period_type" if color_by == "period_type" else None)
    )
    fig = px.scatter(
        plot_df,
        x=metric_x,
        y=metric_y,
        color=color_col,
        title=title,
        labels={metric_x: x_label, metric_y: y_label},
        log_x=log_x,
        log_y=log_y,
        hover_data=["repo", "period_type"],
    )
    if show_regression and not plot_df.empty:
        x_vals = plot_df[metric_x].values
        y_vals = plot_df[metric_y].values
        slope, intercept = np.polyfit(x_vals, y_vals, 1)
        x_line = np.linspace(x_vals.min(), x_vals.max(), 50)
        y_line = slope * x_line + intercept
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
