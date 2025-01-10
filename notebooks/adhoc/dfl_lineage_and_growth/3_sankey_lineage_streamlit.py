import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import random

# ----------------------------------------------------------------------------
# 1) Set page config FIRST to avoid "set_page_config() can only be called once..."
# ----------------------------------------------------------------------------
st.set_page_config(layout="wide")

# ----------------------------------------------------------------------------
# 2) Apply custom CSS for multi-select so large selections are scrollable
#    both in the dropdown list and in the "selected items" control itself.
# ----------------------------------------------------------------------------
st.markdown(
    """
    <style>
    /* Limit height of the dropdown list */
    .stMultiSelect [role='listbox'] {
        max-height: 200px;
        overflow-y: auto;
    }
    /* Limit height of the selected-items box (so that selected items scroll if they exceed this height) */
    .stMultiSelect .css-1u3bzj6-control {
        max-height: 100px;
        overflow-y: auto;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ----------------------------------------------------------------------------
# DATA LOADING AND HELPER FUNCTIONS
# ----------------------------------------------------------------------------
@st.cache_data
def load_data(csv_path: str) -> pd.DataFrame:
    """Load a large CSV and cache it to avoid repeated reads."""
    return pd.read_csv(csv_path)


def filter_by_date(df_all: pd.DataFrame, snapshot_date: str) -> pd.DataFrame:
    """Return a subset of df_all that matches the given snapshot_date."""
    return df_all[df_all["dt"] == snapshot_date].copy()


def get_unique_sorted(df: pd.DataFrame, col: str) -> list[str]:
    """Retrieve sorted, unique values (dropping NaN) for a given column."""
    return sorted(df[col].dropna().unique().tolist())


def group_small_values(df: pd.DataFrame, col: str, threshold_val: float) -> pd.DataFrame:
    """Group values in 'col' whose total TVL < threshold_val into 'Others'."""
    totals = df.groupby(col)["app_token_tvl_usd"].sum()
    small_values = totals[totals < threshold_val].index
    df[col] = df[col].apply(lambda x: x if x not in small_values else "Others")
    return df


def build_sankey_df(
    df: pd.DataFrame, columns_order: list[str], threshold_val: float = 0.03
) -> pd.DataFrame:
    """
    Summarize TVL for a 3-level Sankey chart based on columns in columns_order.
    """
    left_col, mid_col, right_col = columns_order

    df_sankey = df.groupby([left_col, mid_col, right_col], as_index=False).agg(
        {"app_token_tvl_usd": "sum"}
    )

    total_val = df_sankey["app_token_tvl_usd"].sum()
    cutoff = threshold_val * total_val

    # Consolidate small entries
    df_sankey = group_small_values(df_sankey, left_col, cutoff)
    df_sankey = group_small_values(df_sankey, mid_col, cutoff)
    df_sankey = group_small_values(df_sankey, right_col, cutoff)

    # Re-aggregate after grouping to merge the newly labeled 'Others'
    df_sankey = df_sankey.groupby([left_col, mid_col, right_col], as_index=False).agg(
        {"app_token_tvl_usd": "sum"}
    )
    return df_sankey


def build_sankey_links_and_nodes(df_sankey: pd.DataFrame, columns_order: list[str]):
    """
    Convert the 3-col DataFrame into arrays suitable for Plotly Sankey links.
    """
    import pandas as pd

    left_col, mid_col, right_col = columns_order
    left_vals = df_sankey[left_col].unique().tolist()
    mid_vals = df_sankey[mid_col].unique().tolist()
    right_vals = df_sankey[right_col].unique().tolist()

    idx_left = {v: i for i, v in enumerate(left_vals)}
    idx_mid = {v: i + len(left_vals) for i, v in enumerate(mid_vals)}
    idx_right = {v: i + len(left_vals) + len(mid_vals) for i, v in enumerate(right_vals)}

    # Left -> Mid
    lm_df = df_sankey.groupby([left_col, mid_col], as_index=False).agg({"app_token_tvl_usd": "sum"})
    lm_source = lm_df[left_col].map(idx_left)
    lm_target = lm_df[mid_col].map(idx_mid)
    lm_value = lm_df["app_token_tvl_usd"]

    # Mid -> Right
    mr_df = df_sankey.groupby([mid_col, right_col], as_index=False).agg(
        {"app_token_tvl_usd": "sum"}
    )
    mr_source = mr_df[mid_col].map(idx_mid)
    mr_target = mr_df[right_col].map(idx_right)
    mr_value = mr_df["app_token_tvl_usd"]

    link_source = pd.concat([lm_source, mr_source], ignore_index=True)
    link_target = pd.concat([lm_target, mr_target], ignore_index=True)
    link_value = pd.concat([lm_value, mr_value], ignore_index=True)

    node_labels = left_vals + mid_vals + right_vals
    return node_labels, link_source, link_target, link_value


def random_color() -> str:
    """Generate a random hex color code."""
    r = lambda: random.randint(0, 255)
    return f"#{r():02X}{r():02X}{r():02X}"


def assign_node_colors(node_labels: list[str]) -> list[str]:
    """
    Assign random, but reproducible, colors to each node label.
    Make 'Others' gray if present.
    """
    random.seed(42)
    color_map = {lbl: random_color() for lbl in node_labels}
    if "Others" in color_map:
        color_map["Others"] = "#999999"
    return [color_map[lbl] for lbl in node_labels]


def plot_sankey(
    df_sankey: pd.DataFrame,
    column_labels_map: dict,
    columns_order: list[str],
    snapshot_date: str,
    threshold_val: float,
    note: str = None,
) -> go.Figure:
    """
    Build a Plotly Sankey diagram given a 3-col df_sankey and columns_order.

    This version includes:
    - Higher resolution/size for improved readability.
    - Larger base font size for clarity.
    """
    # Raise an error if the resulting DataFrame is empty
    if df_sankey.empty:
        raise ValueError("No data available after filters. Sankey would be empty.")

    # Build Sankey links and nodes
    node_labels, link_source, link_target, link_value = build_sankey_links_and_nodes(
        df_sankey, columns_order
    )
    node_colors = assign_node_colors(node_labels)

    # Calculate total TVL for annotation
    total_tvl = df_sankey["app_token_tvl_usd"].sum()

    # Retrieve friendly labels for each column
    left_col, mid_col, right_col = columns_order
    left_lbl = column_labels_map.get(left_col, left_col)
    mid_lbl = column_labels_map.get(mid_col, mid_col)
    right_lbl = column_labels_map.get(right_col, right_col)

    # Create the Sankey figure with larger dimensions and increased font size
    fig = go.Figure(
        data=[
            go.Sankey(
                arrangement="snap",
                node=dict(
                    label=node_labels,
                    pad=15,
                    thickness=20,
                    line=dict(color="black", width=0.5),
                    color=node_colors,
                ),
                link=dict(
                    source=link_source,
                    target=link_target,
                    value=link_value,
                    color="#cccccc",  # uniform gray for all links
                    customdata=(link_value / 1e6),
                    hovertemplate="TVL: %{customdata:.2f}M USD<extra></extra>",
                ),
            )
        ]
    )

    # Updated layout for better readability
    fig.update_layout(
        title={"text": f"Protocol Token Lineage - {snapshot_date}", "font": {"size": 16}},
        annotations=[
            dict(
                x=0.0,
                y=-0.25,
                xref="paper",
                yref="paper",
                text=note if note else "",
                showarrow=False,
                font=dict(size=10),
                align="left",
            ),
            dict(
                x=0,
                y=1.05,
                xref="paper",
                yref="paper",
                text=f"<b>{left_lbl}</b>",
                showarrow=False,
                font=dict(size=14),
                align="center",
            ),
            dict(
                x=0.5,
                y=1.05,
                xref="paper",
                yref="paper",
                text=f"<b>{mid_lbl}</b>",
                showarrow=False,
                font=dict(size=14),
                align="center",
            ),
            dict(
                x=1,
                y=1.05,
                xref="paper",
                yref="paper",
                text=f"<b>{right_lbl}</b>",
                showarrow=False,
                font=dict(size=14),
                align="center",
            ),
            dict(
                x=0,
                y=-0.08,
                xref="paper",
                yref="paper",
                text=f"<b>Total TVL Shown: ${total_tvl/1e9:,.2f}B</b>",
                showarrow=False,
                font=dict(size=14),
            ),
        ],
        font_size=12,
        margin=dict(l=50, r=50, b=150, t=100),
        autosize=True,
        width=1200,  # Increased width
        height=800,  # Increased height
    )
    return fig


def truncate_list(values: list[str], max_count: int = 10) -> str:
    """Utility to convert a list to a truncated string if it exceeds max_count items."""
    if len(values) <= max_count:
        return ", ".join(values)
    else:
        head_items = ", ".join(values[:max_count])
        return f"{head_items}, ... ({len(values)} total)"


# ----------------------------------------------------------------------------
# FILTER MENU WITH TABS
# ----------------------------------------------------------------------------
def render_filter_menu(df_date: pd.DataFrame):
    """
    Render a multi-tab filter menu, each with a toggleable "Select All / Deselect All" button.
    Returns the user's selections along with threshold and "Apply" status.
    """

    # Initialize session_state defaults if missing
    if "selected_chains" not in st.session_state:
        st.session_state["selected_chains"] = []
    if "selected_protocol_categories" not in st.session_state:
        st.session_state["selected_protocol_categories"] = []
    if "selected_token_categories" not in st.session_state:
        st.session_state["selected_token_categories"] = []
    if "selected_protocols" not in st.session_state:
        st.session_state["selected_protocols"] = []

    chain_options = get_unique_sorted(df_date, "chain")
    proto_cat_options = get_unique_sorted(df_date, "protocol_category")
    token_cat_options = get_unique_sorted(df_date, "token_category")
    protocol_slug_options = get_unique_sorted(df_date, "protocol_slug")

    with st.form("filters_form"):
        filter_tabs = st.tabs(
            ["Chains", "Protocol Categories", "Token Categories", "Protocol Slugs", "Threshold"]
        )

        # --------------- TAB 1: CHAINS ---------------
        # --------------- TAB 1: CHAINS ---------------
    with filter_tabs[0]:
        st.markdown("### Chain Filter")

        # Initialize session state for selected chains if not already set
        if "selected_chains" not in st.session_state:
            st.session_state["selected_chains"] = []

        # Add custom CSS to limit the height of the dropdown
        st.markdown(
            """
            <style>
            div[data-baseweb="select"] {
                max-height: 200px;
                overflow: auto;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        # Toggling "Select All / Deselect All" button
        toggle_all_chains_btn = st.form_submit_button("Toggle All Chains")
        if toggle_all_chains_btn:
            if len(st.session_state["selected_chains"]) == len(chain_options):
                # Deselect all
                st.session_state["selected_chains"] = []
            else:
                # Select all
                st.session_state["selected_chains"] = chain_options

        # Multi-select dropdown
        selected_chains = st.multiselect(
            "Select Chains",
            options=chain_options,
            default=st.session_state["selected_chains"],
        )

        # Only update session state if the selection changes
        if set(selected_chains) != set(st.session_state["selected_chains"]):
            st.session_state["selected_chains"] = selected_chains

        # --------------- TAB 2: PROTOCOL CATEGORIES ---------------
        # --------------- TAB 2: PROTOCOL CATEGORIES ---------------
    with filter_tabs[1]:
        st.markdown("### Protocol Categories")

        # Initialize session state for selected protocol categories if not already set
        if "selected_protocol_categories" not in st.session_state:
            st.session_state["selected_protocol_categories"] = []

        # Add custom CSS to limit the height of the dropdown
        st.markdown(
            """
            <style>
            div[data-baseweb="select"] {
                max-height: 200px;
                overflow: auto;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        # Toggling "Select All / Deselect All" button
        toggle_all_proto_cats_btn = st.form_submit_button("Toggle All Protocol Cats")
        if toggle_all_proto_cats_btn:
            if len(st.session_state["selected_protocol_categories"]) == len(proto_cat_options):
                st.session_state["selected_protocol_categories"] = []
            else:
                st.session_state["selected_protocol_categories"] = proto_cat_options

        # Multi-select dropdown
        selected_protocol_categories = st.multiselect(
            "Select Protocol Categories",
            options=proto_cat_options,
            default=st.session_state["selected_protocol_categories"],
        )

        # Only update session state if the selection changes
        if set(selected_protocol_categories) != set(
            st.session_state["selected_protocol_categories"]
        ):
            st.session_state["selected_protocol_categories"] = selected_protocol_categories

    # --------------- TAB 3: TOKEN CATEGORIES ---------------
    with filter_tabs[2]:
        st.markdown("### Token Categories")

        # Initialize session state for selected token categories if not already set
        if "selected_token_categories" not in st.session_state:
            st.session_state["selected_token_categories"] = []

        # Add custom CSS to limit the height of the dropdown
        st.markdown(
            """
            <style>
            div[data-baseweb="select"] {
                max-height: 200px;
                overflow: auto;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        # Toggling "Select All / Deselect All" button
        toggle_all_token_cats_btn = st.form_submit_button("Toggle All Token Cats")
        if toggle_all_token_cats_btn:
            if len(st.session_state["selected_token_categories"]) == len(token_cat_options):
                st.session_state["selected_token_categories"] = []
            else:
                st.session_state["selected_token_categories"] = token_cat_options

        # Multi-select dropdown
        selected_token_categories = st.multiselect(
            "Select Token Categories",
            options=token_cat_options,
            default=st.session_state["selected_token_categories"],
        )

        # Only update session state if the selection changes
        if set(selected_token_categories) != set(st.session_state["selected_token_categories"]):
            st.session_state["selected_token_categories"] = selected_token_categories

    # --------------- TAB 4: PROTOCOL SLUGS ---------------
    with filter_tabs[3]:
        st.markdown("### Protocol Slugs")

        # Initialize session state for selected protocols if not already set
        if "selected_protocols" not in st.session_state:
            st.session_state["selected_protocols"] = []

        # Add custom CSS to limit the height of the dropdown
        st.markdown(
            """
            <style>
            div[data-baseweb="select"] {
                max-height: 200px;
                overflow: auto;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        # Toggling "Select All / Deselect All" button
        toggle_all_protocols_btn = st.form_submit_button("Toggle All Protocols")
        if toggle_all_protocols_btn:
            if len(st.session_state["selected_protocols"]) == len(protocol_slug_options):
                st.session_state["selected_protocols"] = []
            else:
                st.session_state["selected_protocols"] = protocol_slug_options

        # Multi-select dropdown
        selected_protocols = st.multiselect(
            "Select Protocol Slugs",
            options=protocol_slug_options,
            default=st.session_state["selected_protocols"],
        )

        # Only update session state if the selection changes
        if set(selected_protocols) != set(st.session_state["selected_protocols"]):
            st.session_state["selected_protocols"] = selected_protocols

        # --------------- TAB 5: THRESHOLD ---------------
        with filter_tabs[4]:
            st.markdown("### Threshold Settings")
            threshold_slider = st.slider(
                "Grouping Threshold (%) of total TVL",
                min_value=0.0,
                max_value=10.0,
                value=3.0,
                step=0.5,
            )
            threshold_val = threshold_slider / 100.0

    apply_filters = st.button("Apply Filters", type="primary")

    return (
        st.session_state["selected_chains"],
        st.session_state["selected_protocol_categories"],
        st.session_state["selected_token_categories"],
        st.session_state["selected_protocols"],
        threshold_val,
        apply_filters,
    )


# ----------------------------------------------------------------------------
# MAIN APP
# ----------------------------------------------------------------------------
def main():
    """
    Main function to run the Streamlit app, now including an Export to PNG button
    for the generated Sankey plot.
    """
    st.title("Lineage Dashboard")

    # 1) Load Data (cached)
    data_file = "./protocol_data_cleaned_filtered.csv"  # Adjust path if needed
    df_all = load_data(data_file)
    st.write(f"Full Data Loaded. Shape: {df_all.shape}")

    # 2) Date selection form
    with st.form(key="date_form"):
        st.subheader("Date Selection")
        snapshot_dates = sorted(list(df_all["dt"].dropna().unique()))

        # Determine default index
        if "cached_date" not in st.session_state:
            default_date_index = 0
        else:
            cached_date = st.session_state["cached_date"]
            default_date_index = (
                snapshot_dates.index(cached_date) if cached_date in snapshot_dates else 0
            )

        selected_date = st.selectbox(
            "Snapshot Date", options=snapshot_dates, index=default_date_index
        )
        date_submitted = st.form_submit_button("Confirm Date")

    # If date changed, store new date subset
    if date_submitted:
        st.session_state["cached_date"] = selected_date
        df_date = filter_by_date(df_all, selected_date)
        st.session_state["df_date"] = df_date
    else:
        df_date = st.session_state.get("df_date", pd.DataFrame([]))

    if df_date.empty:
        st.warning("No data available for the selected date.")
        return

    st.write(f"Filtered by date. Shape: {df_date.shape}")

    # 3) Render filter menu
    (
        selected_chains,
        selected_protocol_categories,
        selected_token_categories,
        selected_protocols,
        threshold_val,
        apply_filters,
    ) = render_filter_menu(df_date)

    # 4) Build df_focus if user clicks "Apply Filters"
    if apply_filters:
        st.session_state["cached_filters"] = {
            "selected_chains": selected_chains,
            "selected_protocol_categories": selected_protocol_categories,
            "selected_token_categories": selected_token_categories,
            "selected_protocols": selected_protocols,
            "threshold_val": threshold_val,
        }

        df_focus = df_date[
            df_date["chain"].isin(selected_chains)
            & df_date["protocol_category"].isin(selected_protocol_categories)
            & df_date["token_category"].isin(selected_token_categories)
            & df_date["protocol_slug"].isin(selected_protocols)
        ].copy()
        st.session_state["df_focus"] = df_focus

    # 5) If we have a subset and filters, build Sankey
    if "df_focus" in st.session_state and "cached_filters" in st.session_state:
        df_focus = st.session_state["df_focus"]
        used_filters = st.session_state["cached_filters"]

        st.subheader("Filtered Sankey Plot")
        st.write(f"Data shape after additional filters: {df_focus.shape}")

        columns_order = ["source_protocol", "token", "parent_protocol"]
        df_sankey = build_sankey_df(
            df_focus, columns_order, threshold_val=used_filters["threshold_val"]
        )
        st.write(f"Data shape after grouping: {df_sankey.shape}")

        note = (
            f"<b>Filters:</b><br>"
            f"• <b>Chains:</b> {truncate_list(used_filters['selected_chains'])}<br>"
            f"• <b>Protocol Categories:</b> {truncate_list(used_filters['selected_protocol_categories'])}<br>"
            f"• <b>Token Categories:</b> {truncate_list(used_filters['selected_token_categories'])}<br>"
            f"• <b>Protocols:</b> {truncate_list(used_filters['selected_protocols'])}<br><br>"
            f"Note: Smaller protocols than {used_filters['threshold_val']*100:.1f}% of total TVL "
            f"are grouped under 'Others'."
        )

        column_labels_map = {
            "source_protocol": "Token Issuer",
            "token": "Token",
            "parent_protocol": "Protocol Destination",
        }

        try:
            fig = plot_sankey(
                df_sankey=df_sankey,
                column_labels_map=column_labels_map,
                columns_order=columns_order,
                snapshot_date=str(st.session_state["cached_date"]),
                threshold_val=used_filters["threshold_val"],
                note=note,
            )

            # Display the figure
            st.plotly_chart(fig, use_container_width=True, height=600)

            # Button for exporting the figure to JPG
            import io

            # Convert Plotly figure to a BytesIO buffer in JPG format
            jpg_buffer = io.BytesIO()
            fig.write_image(jpg_buffer, format="jpg", scale=2)
            jpg_buffer.seek(0)

            # Provide a download button
            st.download_button(
                label="Download Plot as JPG",
                data=jpg_buffer,
                file_name="sankey_plot.jpg",
                mime="image/jpeg",
            )

        except ValueError as e:
            st.warning(str(e))
    else:
        st.info("No filters have been applied yet. Select your filters and click 'Apply Filters'.")


# ----------------------------------------------------------------------------
# 7) Run the App
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
