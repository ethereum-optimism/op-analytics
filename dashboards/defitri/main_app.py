# dashboards/defitri/main_app.py

import streamlit as st
from sidebar_utils import setup_sidebar
from tabs_view import render_main_tabs  # We'll create "tabs_view.py" for the tab-based UI


def main() -> None:
    """
    Main entry for the DeFi Tri Rank Tracker.
    We separate the tab UI logic into a separate file (tabs_view.py) to avoid re-initializing
    everything every time a button is pressed. Data loading is handled here and cached in session_state.
    """

    st.set_page_config(page_title="DeFi Tri Rank Tracker", layout="wide")

    # 1) If we haven't loaded data yet, try to load from the sidebar (CSV or BigQuery).
    #    The 'setup_sidebar' function sets st.session_state["df"] under the hood if data is loaded.
    setup_sidebar()

    # 2) If there's no data after the sidebar step, show a message. Otherwise, go to the main UI tabs.
    if "df" not in st.session_state or st.session_state["df"].empty:
        st.info("No data loaded yet. Please upload a CSV or run a BigQuery query.")
        return

    # 3) We have data => render the main tab view, which includes filters and plots
    render_main_tabs()


if __name__ == "__main__":
    main()
