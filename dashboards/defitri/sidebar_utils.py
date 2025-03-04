"""
Refactored sidebar_utils.py with caching to reduce redundant data loads/queries.
"""

# ===== Standard Library Imports =====
import json
import tempfile

# ===== Third-Party Imports =====
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

# ===== Local/Project Imports (if any) =====
# from op_analytics.coreutils.logger import structlog  # Example: if you need your logger

# ================ Auth Helpers =========================


def authenticate_bigquery(key_path: str = None, json_key: dict = None) -> bigquery.Client:
    """
    Authenticate to BigQuery using either a local JSON key file path or
    an in-memory JSON key dict. If neither is provided, attempt application-default credentials.
    """
    if json_key:
        credentials = service_account.Credentials.from_service_account_info(json_key)
        return bigquery.Client(credentials=credentials)
    elif key_path:
        credentials = service_account.Credentials.from_service_account_file(key_path)
        return bigquery.Client(credentials=credentials)
    else:
        return bigquery.Client()  # Use default credentials (e.g., gcloud auth)


# ================ BQ Query Helpers =====================


@st.cache_data
def run_bigquery_cached_query(
    query: str,
    project_id: str,
    json_key: dict | None,
    key_path: str | None,
) -> pd.DataFrame:
    """
    Cached function to run a BigQuery query and return a DataFrame.
    The cache is invalidated if any of the function arguments change (query, project_id, etc.)
    or if the code in this function changes.
    """
    client = authenticate_bigquery(key_path=key_path, json_key=json_key)
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as exc:
        st.error(f"BigQuery query failed: {str(exc)}")
        return pd.DataFrame()


# ================ CSV Upload Helpers ====================


@st.cache_data
def load_csv_file(file_bytes: bytes) -> pd.DataFrame:
    """
    Cached CSV loader. The cache is invalidated when the file content changes.
    """
    df = pd.read_csv(file_bytes)
    return df


# ================ Main Sidebar Routines =================


def upload_csv_data() -> pd.DataFrame:
    """
    Provides a file uploader UI for CSV. If a file is uploaded,
    loads it via a cached function. Returns the resulting DataFrame.
    """
    uploaded = st.file_uploader(
        "Upload CSV file (must contain date, display_name, and ranking metrics)",
        type=["csv"],
        key="file_upload",
    )

    if uploaded is None:
        return pd.DataFrame()

    # Check if we should replace existing data
    if "df" in st.session_state and not st.session_state["df"].empty:
        if not st.button("Replace current data with uploaded CSV?"):
            return st.session_state["df"]

    # The file is not None, so load via cached function
    try:
        df = load_csv_file(uploaded)
        st.success(f"Loaded data with {len(df)} rows from '{uploaded.name}'")
    except Exception as e:
        st.error(f"CSV load failed: {str(e)}")
        return pd.DataFrame()

    # Convert date-like columns to datetime if we can guess them
    date_cols = [c for c in df.columns if "dt" in c.lower() or "date" in c.lower()]
    if date_cols:
        df[date_cols[0]] = pd.to_datetime(df[date_cols[0]], errors="coerce")
        if date_cols[0] != "dt":
            df["dt"] = df[date_cols[0]]

    # Ensure we have a 'display_name'
    if "display_name" not in df.columns:
        # Try to guess a chain or name column
        object_cols = df.select_dtypes(include=["object"]).columns.tolist()
        name_cols = [col for col in object_cols if "name" in col.lower() or "chain" in col.lower()]
        if name_cols:
            df["display_name"] = df[name_cols[0]]
        else:
            st.warning("No suitable column found for 'display_name' (chain/network name).")

    # Store in session state for persistence
    st.session_state["df"] = df
    return df


def fetch_bigquery_data() -> pd.DataFrame:
    """
    Interactive BigQuery data fetch UI. We can store service account info
    via st.secrets or prompt user for manual input. The final data is
    returned and cached if the query + credentials remain unchanged.
    """
    # Try retrieving credentials from st.secrets first
    config_auth = False
    project_id = "oplabs-tools-data"
    json_key = None
    key_path = None
    client = None

    st.markdown("### BigQuery Data Fetch")

    try:
        if "gcp_service_account" in st.secrets:
            # Attempt to use service account from st.secrets
            sa_info = dict(st.secrets["gcp_service_account"])
            json_key = sa_info
            project_id = sa_info.get("project_id", "oplabs-tools-data")
            config_auth = True
            st.info("Using GCP Service Account from st.secrets")
        elif "connections" in st.secrets and "bigquery" in st.secrets["connections"]:
            bq_config = st.secrets["connections"]["bigquery"]
            project_id = bq_config.get("project_id", project_id)
            dataset = bq_config.get("dataset", "")
            table = bq_config.get("table", "")
            credentials_json = bq_config.get("credentials_json", "")
            credentials_path = bq_config.get("credentials_path", "")

            if credentials_json:
                json_key = json.loads(credentials_json)
                config_auth = True
                st.info("Using GCP Service Account from connections.bigquery in st.secrets")
            elif credentials_path:
                key_path = credentials_path
                config_auth = True
                st.info("Using local key path from connections.bigquery in st.secrets")
    except Exception as e:
        st.warning(f"Could not parse st.secrets: {str(e)}")

    if not config_auth:
        # Let user pick an auth method
        auth_method = st.radio(
            "BigQuery Auth Method",
            ["Default Credentials", "Service Account Key File", "Service Account JSON"],
            key="bq_auth_method",
        )
        if auth_method == "Service Account Key File":
            key_file = st.file_uploader("Upload your service account key JSON", type=["json"])
            if key_file is not None:
                # Save to a temp file for usage
                with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
                    tmp_path = tmp.name
                    tmp.write(key_file.read())
                key_path = tmp_path
                st.success("Service account key file uploaded.")
        elif auth_method == "Service Account JSON":
            raw_json = st.text_area("Paste your service account JSON:")
            if raw_json.strip():
                try:
                    json_key = json.loads(raw_json)
                    st.success("Service account JSON loaded.")
                except Exception as e:
                    st.error(f"Invalid JSON: {str(e)}")
        else:
            # Default credentials
            st.info("Will try using default GCP credentials (no JSON key).")

    user_project = st.text_input("BigQuery Project ID", value=project_id)
    user_dataset = st.text_input("BigQuery Dataset", value="materialized_tables")
    user_table = st.text_input("BigQuery Table", value="daily_superchain_health_mv")
    days_back = st.number_input("Days of Data", min_value=1, max_value=365, value=90)

    query = f"""
    SELECT
      dt,
      display_name,
      app_tvl_usd AS TVL,
      app_tvl_usd_stablecoins AS TVL_Stables,
      app_tvl_usd_eth_lst_lrt AS TVL_ETH_LST_LRT,
      app_tvl_usd_dex AS TVL_DEX,
      total_dex_volume_usd AS DEX_Volume,
      lend_total_supply_usd AS Supply_TVL,
      lend_total_borrow_usd AS Borrow_TVL,
      lend_wt_avg_apy_base_borrow AS Borrow_Rate,
      (total_dex_volume_usd / NULLIF(app_tvl_usd_dex, 0)) AS DEX_Volume_TVL,
      (lend_total_borrow_usd / NULLIF(lend_total_supply_usd, 0)) AS Borrow_Utilization,
      RANK() OVER (PARTITION BY dt ORDER BY app_tvl_usd DESC) AS TVL_Rank,
      RANK() OVER (PARTITION BY dt ORDER BY app_tvl_usd_stablecoins DESC) AS TVL_Stables_Rank,
      RANK() OVER (PARTITION BY dt ORDER BY app_tvl_usd_eth_lst_lrt DESC) AS TVL_ETH_LST_LRT_Rank,
      RANK() OVER (PARTITION BY dt ORDER BY app_tvl_usd_dex DESC) AS TVL_DEX_Rank,
      RANK() OVER (PARTITION BY dt ORDER BY total_dex_volume_usd DESC) AS DEX_Volume_Rank,
      RANK() OVER (PARTITION BY dt ORDER BY lend_total_supply_usd DESC) AS Supply_TVL_Rank,
      RANK() OVER (PARTITION BY dt ORDER BY lend_total_borrow_usd DESC) AS Borrow_TVL_Rank,
      RANK() OVER (PARTITION BY dt ORDER BY lend_wt_avg_apy_base_borrow DESC) AS Borrow_Rate_Rank
    FROM `{user_project}.{user_dataset}.{user_table}`
    WHERE 
      DATE(dt) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
      AND app_tvl_usd IS NOT NULL
    ORDER BY dt, display_name
    """

    st.markdown("#### Generated Query:")
    st.code(query, language="sql")

    # Check if we should replace existing data
    if "df" in st.session_state and not st.session_state["df"].empty:
        if not st.button("Replace current data with BigQuery results?"):
            return st.session_state["df"]

    if st.button("Execute Query"):
        with st.spinner("Querying BigQuery..."):
            df = run_bigquery_cached_query(
                query=query,
                project_id=user_project,
                json_key=json_key,
                key_path=key_path,
            )
            if df.empty:
                st.warning("No data returned or query failed.")
                return pd.DataFrame()

            # Attempt to parse dt as datetime
            if "dt" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["dt"]):
                df["dt"] = pd.to_datetime(df["dt"], errors="coerce")

            st.success(f"Loaded {len(df)} rows from BigQuery.")

            # Store in session state for persistence
            st.session_state["df"] = df
            return df

    # If we haven't executed the query, check if we have data in session state
    if "df" in st.session_state and not st.session_state["df"].empty:
        return st.session_state["df"]

    # Otherwise return empty
    return pd.DataFrame()


def setup_sidebar() -> pd.DataFrame:
    """
    Main function that sets up the sidebar UI for data loading (CSV or BigQuery).
    Returns the loaded DataFrame. If neither source is chosen or loaded, returns empty df.
    """
    st.sidebar.title("DeFi Tri Rank Tracker")

    # Initialize session state if needed
    if "df" not in st.session_state:
        st.session_state["df"] = pd.DataFrame()

    # Let user pick data source
    with st.sidebar.expander("Data Source", expanded=True):
        data_source = st.radio("Select Data Source:", ["CSV Upload", "BigQuery"], key="ds_select")

        if data_source == "CSV Upload":
            df = upload_csv_data()
        else:
            df = fetch_bigquery_data()

    # Show available numeric columns in a separate expander
    if not df.empty:
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            with st.sidebar.expander("Available Metrics", expanded=False):
                st.write(", ".join(numeric_cols))

    return df
