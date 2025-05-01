import streamlit as st
import pandas as pd
import numpy as np
import requests
from io import StringIO
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse, parse_qs

# --- Configuration ---
st.set_page_config(page_title="KYC Lookup Tool", page_icon="🗝️", layout="wide")

# --- Constants ---
# APIs
PERSONA_BASE_URL = "https://app.withpersona.com/api/v1"
PERSONA_SCHEME_DOMAIN = "https://app.withpersona.com"
TYPEFORM_BASE_URL = "https://api.typeform.com"
TYPEFORM_FORM_ID = "KoPTjofd"  # Specific form ID from original code
GITHUB_OWNER = "dioptx"
GITHUB_REPO = "the-trojans"
GITHUB_BASE_API_URL = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents"

# GitHub File Paths
CONTRIBUTORS_PATH = "grants.contributors.csv"
PROJECTS_PATH = "grants.projects.csv"
PERSONS_LEGACY_PATH = "legacy.persons.csv"
BUSINESSES_LEGACY_PATH = "legacy.businesses.csv"

# Data Settings
CACHE_TTL_SECONDS = 600  # 10 minutes
EXPIRATION_DAYS = 365  # KYC/KYB expiration period
PERSONA_MAX_PAGES_NORMAL = 200  # Max pages to fetch in normal mode
PERSONA_MAX_PAGES_DEBUG = 1  # Max pages to fetch in debug mode

# Status Constants
STATUS_CLEARED = "🟢 cleared"
STATUS_REJECTED = "🛑 rejected"
STATUS_IN_REVIEW = "🟠 in review"
STATUS_RETRY = "🌕 retry"
STATUS_INCOMPLETE = "🔵 incomplete"
STATUS_EXPIRED = "⚫ expired"
STATUS_NOT_STARTED = "⚪ not started"
STATUS_UNKNOWN = "❓ unknown"
STATUS_NO_FORM = "📄 no form"

# Typeform Field ID - REMOVED Constant, will be loaded from secrets
# TYPEFORM_NUM_CONTROLLERS_FIELD_ID = "v8dfrNJiIQaZ" # Example ID


# --- Authentication ---
def check_credentials() -> bool:
    """Returns `True` if the user is authenticated."""
    # (Authentication code remains the same)

    def login_form():
        st.warning("Please log in to access the KYC Lookup Tool.")
        with st.form("credentials"):
            st.text_input("Email", key="email")
            st.text_input("Password", type="password", key="password")
            submitted = st.form_submit_button("Login")
            if submitted:
                if "email" in st.session_state and "password" in st.session_state:
                    email = st.session_state["email"].lower().strip()
                    password = st.session_state["password"]
                    allowed_emails = st.secrets.get("allowed_emails", [])
                    correct_password = st.secrets.get("password", "")

                    if not allowed_emails or not correct_password:
                        st.error("Authentication configuration missing.")
                        return False

                    if (
                        email in [e.lower() for e in allowed_emails]
                        and password == correct_password
                    ):
                        st.session_state["authenticated"] = True
                        st.rerun()
                    else:
                        st.error("Invalid email or password")
                        st.session_state["authenticated"] = False
                        return False
                else:
                    st.error("Please enter both email and password.")
                    return False
        return False

    if st.session_state.get("authenticated", False):
        return True

    return login_form()


# --- Data Fetching Utilities ---
# (fetch_persona_data, fetch_typeform_data, fetch_github_csv remain the same as previous step)
@st.cache_data(ttl=CACHE_TTL_SECONDS)
def fetch_persona_data(
    api_key: str, endpoint: str, debug_mode: bool = False
) -> List[Dict[str, Any]]:
    """
    Fetches paginated data from a Persona API endpoint with progress updates.
    Limits pages fetched if debug_mode is True.
    """
    results = []
    headers = {"Authorization": f"Bearer {api_key}", "Persona-Version": "2023-01-05"}
    params: Dict[str, Any] = {"page[size]": 100}
    request_url = f"{PERSONA_BASE_URL}/{endpoint}"
    page_count = 0
    max_pages = PERSONA_MAX_PAGES_DEBUG if debug_mode else float("inf")
    debug_msg = " (Debug Mode)" if debug_mode else ""

    status_indicator = st.session_state.get("status_indicator")
    initial_message = f"Fetching Persona {endpoint}{debug_msg}..."
    if status_indicator:
        status_indicator.info(initial_message)

    while True:
        if debug_mode and page_count >= max_pages:
            break

        response = None
        page_num_display = page_count + 1
        progress_message = f"Fetching Persona {endpoint} (Page {page_num_display}){debug_msg}... Fetched {len(results)} records."
        if status_indicator:
            status_indicator.info(progress_message)

        try:
            response = requests.get(request_url, headers=headers, params=params)
            response.raise_for_status()
            response_data = response.json()
            current_page_data = response_data.get("data", [])
            if not current_page_data and page_count > 0:
                if status_indicator:
                    status_indicator.info(
                        f"Finished fetching Persona {endpoint}{debug_msg}. Received empty page {page_num_display}."
                    )
                break
            results.extend(current_page_data)
            next_link = response_data.get("links", {}).get("next")
            if next_link:
                try:
                    parsed_url = urlparse(next_link)
                    query_params = parse_qs(parsed_url.query)
                    next_cursor = query_params.get("page[after]", [None])[0]
                    if next_cursor:
                        params = {"page[size]": 100, "page[after]": next_cursor}
                    else:
                        if status_indicator:
                            status_indicator.info(
                                f"Finished fetching Persona {endpoint}{debug_msg}. No 'next' cursor found."
                            )
                        break
                except Exception as e:
                    st.error(f"Error parsing Persona 'next' link or cursor for {endpoint}: {e}")
                    if status_indicator:
                        status_indicator.warning(
                            f"Error parsing 'next' link for Persona {endpoint}{debug_msg}. Stopping fetch."
                        )
                    break
            else:
                if status_indicator:
                    status_indicator.info(
                        f"Finished fetching Persona {endpoint}{debug_msg}. No 'next' link."
                    )
                break
            page_count += 1
        except requests.exceptions.RequestException as e:
            error_url = request_url if response is None else response.url
            st.error(
                f"Error fetching data from Persona {endpoint} (Page {page_num_display}, URL: {error_url}): {e}"
            )
            if response is not None:
                st.error(f"Response status: {response.status_code}, Content: {response.text[:500]}")
            if status_indicator:
                status_indicator.error(f"Error fetching Persona {endpoint}{debug_msg}. Check logs.")
            return results
        except Exception as e:
            st.error(
                f"An unexpected error occurred during Persona fetch for {endpoint} (Page {page_num_display}): {e}"
            )
            if status_indicator:
                status_indicator.error(
                    f"Unexpected error fetching Persona {endpoint}{debug_msg}. Check logs."
                )
            return results

    if debug_mode and page_count >= max_pages:
        st.warning(
            f"Reached maximum page limit ({max_pages}) for Persona endpoint {endpoint}{debug_msg}. Data might be incomplete."
        )
        if status_indicator:
            status_indicator.warning(f"Reached max pages for Persona {endpoint}{debug_msg}.")

    final_msg = (
        f"✅ Finished fetching {len(results)} records from Persona {endpoint}{debug_msg} (limited to {max_pages} page(s))."
        if debug_mode
        else f"✅ Finished fetching {len(results)} records from Persona {endpoint}."
    )
    if status_indicator:
        status_indicator.info(final_msg)
    return results


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def fetch_typeform_data(api_key: str, form_id: str) -> List[Dict[str, Any]]:
    """Fetches paginated data for a Typeform form."""
    all_items = []
    headers = {"Authorization": f"Bearer {api_key}"}
    url = f"{TYPEFORM_BASE_URL}/forms/{form_id}/responses"
    params: Dict[str, Any] = {"page_size": 1000}
    page_count = 0
    max_pages = 10
    total_items_expected = None

    status_indicator = st.session_state.get("status_indicator")
    if status_indicator:
        status_indicator.info(f"Fetching Typeform {form_id}...")

    while page_count < max_pages:
        response = None
        page_num_display = page_count + 1
        progress_message = f"Fetching Typeform {form_id} (Page {page_num_display})... Fetched {len(all_items)} records"
        if total_items_expected:
            progress_message += f" of ~{total_items_expected}"
        progress_message += "."
        if status_indicator:
            status_indicator.info(progress_message)

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            items = data.get("items", [])
            all_items.extend(items)
            if page_count == 0 and "total_items" in data:
                total_items_expected = data["total_items"]
            retrieved_items = len(all_items)
            current_page_size = len(items)
            if (
                total_items_expected is not None and retrieved_items >= total_items_expected
            ) or current_page_size < params.get("page_size", 1000):
                break
            if items:
                last_token = items[-1].get("token")
                if last_token:
                    params["after"] = last_token
                else:
                    st.warning(
                        "Could not find 'token' in last Typeform item for pagination. Stopping."
                    )
                    break
            else:
                break
        except requests.exceptions.RequestException as e:
            error_url = url if response is None else response.url
            st.error(
                f"Error fetching data from Typeform {form_id} (Page {page_num_display}, URL: {error_url}): {e}"
            )
            if response is not None:
                st.error(f"Response status: {response.status_code}, Content: {response.text[:500]}")
            if status_indicator:
                status_indicator.error(f"Error fetching Typeform {form_id}.")
            return all_items
        except Exception as e:
            st.error(
                f"An unexpected error occurred during Typeform fetch (Page {page_num_display}): {e}"
            )
            if status_indicator:
                status_indicator.error(f"Unexpected error fetching Typeform {form_id}.")
            return all_items
        page_count += 1

    if page_count >= max_pages:
        st.warning(
            f"Reached maximum page limit ({max_pages}) for Typeform {form_id}. Data might be incomplete."
        )
        if status_indicator:
            status_indicator.warning(f"Reached max pages for Typeform {form_id}.")

    if status_indicator:
        status_indicator.info(
            f"✅ Finished fetching {len(all_items)} records from Typeform {form_id}."
        )
    return all_items


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def fetch_github_csv(access_token: str, owner: str, repo: str, path: str) -> Optional[pd.DataFrame]:
    """Fetches a CSV file from a GitHub repository and returns a DataFrame."""
    url = f"{GITHUB_BASE_API_URL}/{path}"
    headers = {"Authorization": f"token {access_token}", "Accept": "application/vnd.github.v3.raw"}

    status_indicator = st.session_state.get("status_indicator")
    if status_indicator:
        status_indicator.info(f"Fetching GitHub {path}...")

    response = None
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        csv_content = response.content.decode("utf-8")
        df = pd.read_csv(StringIO(csv_content))
        if status_indicator:
            status_indicator.info(f"✅ Fetched GitHub {path} ({len(df)} rows).")
        return df
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch GitHub file {path}: {e}")
        if response is not None:
            st.error(f"Response status: {response.status_code}")
            if response.status_code == 401:
                st.error("GitHub token invalid/missing permissions ('repo' scope?).")
            elif response.status_code == 404:
                st.error(f"File not found at path: {path}")
        if status_indicator:
            status_indicator.error(f"Error fetching GitHub {path}.")
        return None
    except pd.errors.EmptyDataError:
        st.warning(f"GitHub CSV file {path} is empty.")
        if status_indicator:
            status_indicator.warning(f"GitHub file {path} is empty.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An unexpected error occurred fetching or parsing GitHub CSV {path}: {e}")
        if status_indicator:
            status_indicator.error(f"Error parsing GitHub {path}.")
        return None


# --- Data Processing Functions ---
# (process_inquiries, process_cases remain the same)
def process_inquiries(raw_inquiries_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Processes raw Persona inquiry data into a structured DataFrame."""
    records = []
    for item in raw_inquiries_data:
        inquiry_id = item.get("id")
        attributes = item.get("attributes", {})
        status_raw = attributes.get("status")
        name_first = attributes.get("name-first", "") or ""
        name_middle = attributes.get("name-middle", "") or ""
        name_last = attributes.get("name-last", "") or ""
        full_name = f"{name_first} {name_middle} {name_last}".strip().replace("  ", " ")
        email_raw = attributes.get("email-address", "") or ""
        email = email_raw.lower().strip() if "@" in email_raw else ""
        updated_at_raw = attributes.get("updated-at")
        updated_at = pd.to_datetime(updated_at_raw, errors="coerce", utc=True)
        l2_address_raw = attributes.get("fields", {}).get("l-2-address", {}).get("value")
        l2_address = np.nan
        if isinstance(l2_address_raw, str) and l2_address_raw.lower().strip().startswith("0x"):
            l2_address = l2_address_raw.lower().strip()
        status = STATUS_UNKNOWN
        if status_raw == "approved":
            status = STATUS_CLEARED
        elif status_raw in ["expired", "pending", "created"]:
            status = STATUS_RETRY
        elif status_raw == "declined":
            status = STATUS_REJECTED
        elif status_raw == "needs_review":
            status = STATUS_IN_REVIEW
        if inquiry_id and email:
            records.append(
                {
                    "inquiry_id": inquiry_id,
                    "name": full_name,
                    "email": email,
                    "l2_address": l2_address,
                    "updated_at": updated_at,
                    "status": status,
                    "source": "persona_inquiry",
                }
            )
    df = pd.DataFrame(records)
    if not df.empty:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True)
        df["l2_address"] = df["l2_address"].astype(str).replace("nan", "")
    return df


def process_cases(raw_cases_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Processes raw Persona case data into a structured DataFrame."""
    records = []
    for item in raw_cases_data:
        case_id = item.get("id")
        attributes = item.get("attributes", {})
        status_raw = attributes.get("status")
        fields = attributes.get("fields", {})
        business_name_raw = fields.get("business-name", {}).get("value")
        business_name = str(business_name_raw).strip() if pd.notna(business_name_raw) else ""
        email_raw = fields.get("form-filler-email-address", {}).get("value")
        email = (
            str(email_raw).lower().strip() if pd.notna(email_raw) and "@" in str(email_raw) else ""
        )
        updated_at_raw = attributes.get("updated-at")
        updated_at = pd.to_datetime(updated_at_raw, errors="coerce", utc=True)
        l2_address_raw = fields.get("l-2-address", {}).get("value")
        l2_address = np.nan
        if isinstance(l2_address_raw, str) and l2_address_raw.lower().strip().startswith("0x"):
            l2_address = l2_address_raw.lower().strip()
        status = STATUS_UNKNOWN
        if status_raw == "Approved":
            status = STATUS_CLEARED
        elif status_raw in ["expired", "pending", "created", "Waiting on UBOs"]:
            status = STATUS_INCOMPLETE
        elif status_raw == "Declined":
            status = STATUS_REJECTED
        elif status_raw in ["Ready for Review", "needs_review"]:
            status = STATUS_IN_REVIEW
        if case_id and email and business_name:
            records.append(
                {
                    "case_id": case_id,
                    "business_name": business_name,
                    "email": email,
                    "l2_address": l2_address,
                    "updated_at": updated_at,
                    "status": status,
                    "source": "persona_case",
                }
            )
    df = pd.DataFrame(records)
    if not df.empty:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True)
        df["l2_address"] = df["l2_address"].astype(str).replace("nan", "")
    return df


# Update process_typeform to accept the field ID from secrets
def process_typeform(
    raw_typeform_data: List[Dict[str, Any]], num_controllers_field_id: Optional[str]
) -> Optional[pd.DataFrame]:
    """
    Processes raw Typeform response data into a structured DataFrame.
    Uses the provided field ID to distinguish KYC/KYB emails.
    """
    if not raw_typeform_data:
        return None
    if not num_controllers_field_id:
        st.error(
            "Typeform Number of Controllers Field ID not configured in secrets. Cannot process emails correctly."
        )
        num_controllers_field_id = (
            "FIELD_ID_MISSING"  # Prevent errors below, but logic will be wrong
        )

    form_entries = []
    for item in raw_typeform_data:
        response_id = item.get("response_id")
        submitted_at_raw = item.get("submitted_at")
        submitted_at = pd.to_datetime(submitted_at_raw, errors="coerce", utc=True)
        hidden = item.get("hidden", {})
        grant_id = hidden.get("grant_id")
        project_id = hidden.get("project_id")
        l2_address_hidden = hidden.get("l2_address")
        if pd.isna(grant_id):
            continue

        l2_address = np.nan
        if isinstance(l2_address_hidden, str) and l2_address_hidden.lower().strip().startswith(
            "0x"
        ):
            l2_address = l2_address_hidden.lower().strip()

        kyc_emails = []
        kyb_emails = []
        number_of_kyb_controllers = 0
        found_controller_field = False
        answers = item.get("answers", [])
        if not isinstance(answers, list):
            answers = []

        for answer in answers:
            field = answer.get("field", {})
            field_id = field.get("id")
            field_type = field.get("type")

            # Use the field ID passed from secrets
            if field_id == num_controllers_field_id and field_type == "number":
                number_of_kyb_controllers = answer.get("number", 0)
                found_controller_field = True
                continue

            if field_type == "email":
                email_raw = answer.get("email")
                if isinstance(email_raw, str) and "@" in email_raw:
                    email_clean = email_raw.lower().strip()
                    if found_controller_field:
                        kyb_emails.append(email_clean)
                    else:
                        kyc_emails.append(email_clean)

        kyb_emails = kyb_emails[:number_of_kyb_controllers]

        form_entries.append(
            {
                "form_id": response_id,
                "project_id": project_id,
                "grant_id": str(grant_id).strip(),
                "l2_address": l2_address,
                "submitted_at": submitted_at,
                "kyc_emails": kyc_emails,
                "kyb_emails": kyb_emails,
            }
        )

    if not form_entries:
        return None
    df = pd.DataFrame(form_entries)
    if not df.empty:
        df["submitted_at"] = pd.to_datetime(df["submitted_at"], errors="coerce", utc=True)
        df["l2_address"] = df["l2_address"].astype(str).replace("nan", "")
        df["grant_id"] = df["grant_id"].astype(str)
    return df


# --- Data Combining & Consolidation ---
# (combine_legacy_live remains the same)
def combine_legacy_live(
    legacy_df: Optional[pd.DataFrame],
    live_df: Optional[pd.DataFrame],
    group_by_cols: List[str],
    id_col: str,
    date_col: str = "updated_at",
) -> pd.DataFrame:
    """
    Combines legacy and live data, determines the latest record based on date,
    and applies expiration logic.
    """
    if legacy_df is None:
        legacy_df = pd.DataFrame()
    if live_df is None:
        live_df = pd.DataFrame()

    if legacy_df.empty:
        combined_df = live_df.copy() if not live_df.empty else pd.DataFrame()
    elif live_df.empty:
        combined_df = legacy_df.copy()
    else:
        if date_col in legacy_df.columns:
            legacy_df[date_col] = pd.to_datetime(legacy_df[date_col], errors="coerce", utc=True)
        if date_col in live_df.columns:
            live_df[date_col] = pd.to_datetime(live_df[date_col], errors="coerce", utc=True)

        required_cols = group_by_cols + [id_col, date_col, "status", "l2_address"]
        optional_cols = ["name", "business_name"]
        all_potential_cols = required_cols + optional_cols + ["source"]

        for df in [legacy_df, live_df]:
            for col in all_potential_cols:
                if col not in df.columns:
                    df[col] = np.nan

        if "source" not in legacy_df.columns or legacy_df["source"].isnull().all():
            legacy_df["source"] = "legacy"
        if "source" not in live_df.columns or live_df["source"].isnull().all():
            live_df["source"] = "live"

        all_cols = list(set(legacy_df.columns) | set(live_df.columns))
        legacy_df = legacy_df.reindex(columns=all_cols)
        live_df = live_df.reindex(columns=all_cols)

        combined_df = pd.concat([legacy_df, live_df], ignore_index=True)

    if combined_df.empty or date_col not in combined_df.columns:
        return pd.DataFrame()

    epoch_start = pd.Timestamp("1970-01-01", tz="UTC")
    combined_df[date_col] = pd.to_datetime(combined_df[date_col], errors="coerce", utc=True).fillna(
        epoch_start
    )

    valid_group_by_cols = [col for col in group_by_cols if col in combined_df.columns]
    if not valid_group_by_cols:
        st.error(f"Group_by columns missing: {group_by_cols}")
        return pd.DataFrame()

    fill_cols = ["status", "l2_address", "name", "business_name", "source", id_col]
    for col in fill_cols:
        if col in combined_df.columns:
            combined_df[col] = combined_df.groupby(valid_group_by_cols)[col].ffill().bfill()

    combined_df = combined_df.sort_values(
        by=valid_group_by_cols + [date_col], ascending=[True] * len(valid_group_by_cols) + [False]
    )
    combined_df = combined_df.drop_duplicates(subset=valid_group_by_cols, keep="first")

    if "status" in combined_df.columns:
        current_date_utc = datetime.now(timezone.utc)
        expiration_threshold = current_date_utc - timedelta(days=EXPIRATION_DAYS)
        combined_df[date_col] = pd.to_datetime(combined_df[date_col], errors="coerce", utc=True)

        expired_mask = (combined_df["status"] == STATUS_CLEARED) & (
            combined_df[date_col] < expiration_threshold
        )
        combined_df.loc[expired_mask, "status"] = STATUS_EXPIRED
        combined_df["status"] = combined_df["status"].fillna(STATUS_NOT_STARTED)
    else:
        st.warning("Column 'status' not found for expiration logic.")

    combined_df[date_col] = combined_df[date_col].replace(epoch_start, pd.NaT)
    combined_df.replace({np.nan: None}, inplace=True)
    if "l2_address" in combined_df.columns:
        combined_df["l2_address"] = combined_df["l2_address"].fillna("")

    return combined_df


# --- UI Interaction Functions ---
# (perform_search remains the same)
def perform_search(df: pd.DataFrame, search_term: str, search_fields: List[str]) -> pd.DataFrame:
    """Performs a case-insensitive search across specified fields in a DataFrame."""
    if not search_term or df.empty:
        return pd.DataFrame(columns=df.columns)  # Return empty df structure

    search_term_lower = search_term.lower()
    mask = pd.Series(False, index=df.index)

    for field in search_fields:
        if field in df.columns:
            # Fill NA with empty string for safe searching
            mask |= (
                df[field]
                .fillna("")
                .astype(str)
                .str.lower()
                .str.contains(search_term_lower, na=False)
            )
        else:
            pass

    return df[mask]


# Update display_search_results to use column_config
def display_search_results(
    results_df: pd.DataFrame,
    search_type: str,
    all_persons_df: pd.DataFrame,
    all_businesses_df: pd.DataFrame,
):
    """Displays search results in a structured format using column_config."""

    # Add search context header
    # st.subheader(f"Search Results: {search_type}") # Already added in main

    if results_df.empty:
        st.info("No matching results found.")
        return

    # --- Define Column Configs ---
    column_config_base = {
        "updated_at": st.column_config.DatetimeColumn("Last Updated", format="YYYY-MM-DD"),
        "submitted_at": st.column_config.DatetimeColumn("Form Submitted", format="YYYY-MM-DD"),
        "inquiry_id": st.column_config.TextColumn("Inquiry ID", help="Internal Persona Inquiry ID"),
        "case_id": st.column_config.TextColumn("Case ID", help="Internal Persona Case ID"),
        "form_id": st.column_config.TextColumn("Form ID", help="Internal Typeform Response ID"),
        "project_id": st.column_config.TextColumn(
            "Project ID", help="Internal Project ID (from Typeform/Legacy)"
        ),
        "l2_address": st.column_config.TextColumn(
            "L2 Address"
        ),  # Could potentially be LinkColumn if base URL known
        "op_amt": st.column_config.NumberColumn("OP Amount", format="%.2f"),
        "email": st.column_config.TextColumn("Email"),
        "name": st.column_config.TextColumn("Individual Name"),
        "business_name": st.column_config.TextColumn("Business Name"),
        "project_name": st.column_config.TextColumn("Project Name"),
        "round_id": st.column_config.TextColumn("Round ID"),
        "status": st.column_config.TextColumn("KYC Status"),
        "overall_status": st.column_config.TextColumn("Grant Status"),
        "source": st.column_config.TextColumn("Latest Source"),
        # Hide email lists by default in Grant search results table
        "kyc_emails": None,
        "kyb_emails": None,
    }

    # --- Display Logic ---
    if search_type == "Individual (KYC)":
        cols_to_display = [
            "name",
            "email",
            "l2_address",
            "status",
            "updated_at",
            "project_name",
            "round_id",
            "op_amt",
            "source",
        ]
        display_df = results_df[
            [col for col in cols_to_display if col in results_df.columns]
        ].copy()
        st.dataframe(
            display_df, use_container_width=True, column_config=column_config_base, hide_index=True
        )
        # Display summary for the first result
        if not results_df.empty:
            first_result = results_df.iloc[0]
            st.markdown(
                f"#### Latest Status for {first_result.get('name', first_result.get('email', 'Result'))}: **{first_result.get('status', STATUS_UNKNOWN)}**"
            )

    elif search_type == "Business (KYB)":
        cols_to_display = ["business_name", "email", "l2_address", "status", "updated_at", "source"]
        display_df = results_df[
            [col for col in cols_to_display if col in results_df.columns]
        ].copy()
        st.dataframe(
            display_df, use_container_width=True, column_config=column_config_base, hide_index=True
        )
        if not results_df.empty:
            first_result = results_df.iloc[0]
            st.markdown(
                f"#### Latest Status for {first_result.get('business_name', first_result.get('email', 'Result'))}: **{first_result.get('status', STATUS_UNKNOWN)}**"
            )

    elif search_type == "Grant ID":
        cols_to_display = [
            "grant_id",
            "project_name",
            "round_id",
            "overall_status",
            "l2_address",
            "submitted_at",
            "kyc_emails",
            "kyb_emails",
        ]
        display_df = results_df[
            [col for col in cols_to_display if col in results_df.columns]
        ].copy()
        # Display main grant info table (hide email lists here)
        st.dataframe(
            display_df, use_container_width=True, column_config=column_config_base, hide_index=True
        )

        # Display individual statuses separately below the main table
        for index, grant_row in results_df.iterrows():
            st.markdown("---")
            st.markdown(
                f"#### Details for Grant: {grant_row['grant_id']} ({grant_row.get('project_name', 'N/A')})"
            )
            st.markdown(
                f"**Overall Status:** **{grant_row.get('overall_status', STATUS_UNKNOWN)}**"
            )

            kyc_emails = grant_row.get("kyc_emails", [])
            kyb_emails = grant_row.get("kyb_emails", [])

            if kyc_emails:
                st.markdown("**Individual KYC Statuses:**")
                kyc_status_data = []
                for email in kyc_emails:
                    person_row = all_persons_df[all_persons_df["email"] == email]
                    status = (
                        person_row["status"].iloc[0] if not person_row.empty else STATUS_NOT_STARTED
                    )
                    name = (
                        person_row["name"].iloc[0]
                        if not person_row.empty and pd.notna(person_row["name"].iloc[0])
                        else "N/A"
                    )
                    last_update = (
                        pd.to_datetime(person_row["updated_at"].iloc[0]).strftime("%Y-%m-%d")
                        if not person_row.empty and pd.notna(person_row["updated_at"].iloc[0])
                        else "N/A"
                    )
                    kyc_status_data.append(
                        {"Email": email, "Name": name, "Status": status, "Last Update": last_update}
                    )
                st.table(pd.DataFrame(kyc_status_data))  # Use st.table for simple display
            else:
                st.markdown("*No individual KYC emails linked.*")

            if kyb_emails:
                st.markdown("**Business KYB Statuses:**")
                kyb_status_data = []
                for email in kyb_emails:
                    business_rows = all_businesses_df[all_businesses_df["email"] == email]
                    if not business_rows.empty:
                        for _, biz_row in business_rows.iterrows():
                            last_update = (
                                pd.to_datetime(biz_row.get("updated_at")).strftime("%Y-%m-%d")
                                if pd.notna(biz_row.get("updated_at"))
                                else "N/A"
                            )
                            kyb_status_data.append(
                                {
                                    "Email": email,
                                    "Business Name": biz_row.get("business_name", "N/A"),
                                    "Status": biz_row.get("status", STATUS_NOT_STARTED),
                                    "Last Update": last_update,
                                }
                            )
                    else:
                        kyb_status_data.append(
                            {
                                "Email": email,
                                "Business Name": "N/A",
                                "Status": STATUS_NOT_STARTED,
                                "Last Update": "N/A",
                            }
                        )
                st.table(pd.DataFrame(kyb_status_data))  # Use st.table for simple display
            else:
                st.markdown("*No business KYB emails linked.*")
            st.markdown("---")


# --- Main Application Logic ---
def main():
    """Main function to run the Streamlit application."""
    st.title("🗝️ KYC Lookup Tool")

    if not check_credentials():
        st.stop()

    st.success(f"Welcome {st.session_state.get('email', 'User')}!")

    # --- Sidebar Setup ---
    st.sidebar.header("⚙️ Settings & Status")
    debug_mode_enabled = st.sidebar.checkbox(
        "Enable Debug Mode (Limit Persona Data)", key="debug_mode", value=False
    )
    if "status_indicator" not in st.session_state:
        st.session_state.status_indicator = st.sidebar.empty()
    if debug_mode_enabled:
        st.warning("🐞 Debug Mode Active: Persona data is limited.", icon="⚠️")

    # --- Load Secrets ---
    st.session_state.status_indicator.info("Loading secrets...")
    try:
        persona_api_key = st.secrets["persona"]["api_key"]
        typeform_key = st.secrets["typeform"]["typeform_key"]
        github_token = st.secrets["github"]["access_token"]
        # Load Typeform Field ID from secrets (Improvement 3)
        typeform_num_controllers_field_id = st.secrets.get("typeform", {}).get(
            "num_controllers_field_id"
        )

        if not persona_api_key or not typeform_key or not github_token:
            raise KeyError("API keys (Persona, Typeform, GitHub) are missing.")
        if not typeform_num_controllers_field_id:
            st.warning(
                "Typeform 'num_controllers_field_id' not found in secrets. Email parsing may be incorrect."
            )
            # Provide a default or handle error later in processing
            # typeform_num_controllers_field_id = None # Let processing function handle None

    except KeyError as e:
        st.session_state.status_indicator.error(f"Missing secret config: {e}")
        st.error(f"Fatal Error: Missing secret configuration: {e}. App cannot proceed.")
        st.stop()
    except Exception as e:  # Catch other potential secret loading errors
        st.session_state.status_indicator.error(f"Error loading secrets: {e}")
        st.error(f"Fatal Error loading secrets: {e}. App cannot proceed.")
        st.stop()

    # --- Load Data ---
    def load_all_data(persona_key, tf_key, gh_token, debug_enabled):
        data = {}
        status_indicator = st.session_state.get("status_indicator")
        if status_indicator:
            status_indicator.info("Fetching data... See sidebar for details.")

        data["inquiries_raw"] = fetch_persona_data(
            persona_key, "inquiries", debug_mode=debug_enabled
        )
        data["cases_raw"] = fetch_persona_data(persona_key, "cases", debug_mode=debug_enabled)
        data["typeform_raw"] = fetch_typeform_data(tf_key, TYPEFORM_FORM_ID)
        data["contributors_legacy"] = fetch_github_csv(
            gh_token, GITHUB_OWNER, GITHUB_REPO, CONTRIBUTORS_PATH
        )
        data["projects_legacy"] = fetch_github_csv(
            gh_token, GITHUB_OWNER, GITHUB_REPO, PROJECTS_PATH
        )
        data["persons_legacy"] = fetch_github_csv(
            gh_token, GITHUB_OWNER, GITHUB_REPO, PERSONS_LEGACY_PATH
        )
        data["businesses_legacy"] = fetch_github_csv(
            gh_token, GITHUB_OWNER, GITHUB_REPO, BUSINESSES_LEGACY_PATH
        )

        if (
            data.get("inquiries_raw") is None
            or data.get("cases_raw") is None
            or data.get("typeform_raw") is None
        ):
            st.warning("Failed to load some live data. Results may be incomplete.")
        if (
            data.get("contributors_legacy") is None
            or data.get("projects_legacy") is None
            or data.get("persons_legacy") is None
            or data.get("businesses_legacy") is None
        ):
            st.warning("Failed to load some legacy data. Results may be incomplete.")

        if status_indicator:
            status_indicator.info("Data fetching complete. Processing...")
        return data

    loaded_data = load_all_data(
        persona_api_key, typeform_key, github_token, debug_enabled=debug_mode_enabled
    )

    # --- Process Data ---
    @st.cache_data(max_entries=5)
    def process_and_consolidate_data(data_dict, tf_controller_field_id):
        with st.spinner("Processing and consolidating data..."):
            processed = {}
            processed["persons_live_df"] = process_inquiries(data_dict.get("inquiries_raw", []))
            processed["businesses_live_df"] = process_cases(data_dict.get("cases_raw", []))
            processed["typeform_df"] = process_typeform(
                data_dict.get("typeform_raw", []), tf_controller_field_id
            )
            processed["persons_legacy_df"] = data_dict.get("persons_legacy", pd.DataFrame())
            processed["businesses_legacy_df"] = data_dict.get("businesses_legacy", pd.DataFrame())
            processed["contributors_legacy_df"] = data_dict.get(
                "contributors_legacy", pd.DataFrame()
            )
            processed["projects_legacy_df"] = data_dict.get("projects_legacy", pd.DataFrame())

            processed["all_persons_df"] = combine_legacy_live(
                processed["persons_legacy_df"],
                processed["persons_live_df"],
                group_by_cols=["email"],
                id_col="inquiry_id",
                date_col="updated_at",
            )
            processed["all_businesses_df"] = combine_legacy_live(
                processed["businesses_legacy_df"],
                processed["businesses_live_df"],
                group_by_cols=["email", "business_name"],
                id_col="case_id",
                date_col="updated_at",
            )

            processed["all_contributors_view"] = processed["contributors_legacy_df"]
            if (
                processed["contributors_legacy_df"] is not None
                and not processed["contributors_legacy_df"].empty
                and "email" in processed["contributors_legacy_df"].columns
                and processed["all_persons_df"] is not None
                and not processed["all_persons_df"].empty
            ):
                person_status_cols = ["email", "status", "l2_address", "updated_at", "name"]
                cols_to_merge = [
                    col for col in person_status_cols if col in processed["all_persons_df"].columns
                ]
                merged_contributors = processed["contributors_legacy_df"].merge(
                    processed["all_persons_df"][cols_to_merge],
                    on="email",
                    how="left",
                    suffixes=("_contrib", "_person"),
                )
                for col in ["status", "l2_address", "updated_at", "name"]:
                    p_col, c_col = f"{col}_person", f"{col}_contrib"
                    if p_col in merged_contributors.columns:
                        merged_contributors[col] = merged_contributors[p_col].fillna(
                            merged_contributors.get(c_col)
                        )
                        merged_contributors = merged_contributors.drop(
                            columns=[p_col, c_col], errors="ignore"
                        )
                    elif c_col in merged_contributors.columns:
                        merged_contributors[col] = merged_contributors[c_col]
                        merged_contributors = merged_contributors.drop(
                            columns=[c_col], errors="ignore"
                        )
                if "status" in merged_contributors.columns:
                    merged_contributors["status"] = merged_contributors["status"].fillna(
                        STATUS_NOT_STARTED
                    )
                else:
                    merged_contributors["status"] = STATUS_NOT_STARTED
                merged_contributors.replace({np.nan: None}, inplace=True)
                if "l2_address" in merged_contributors.columns:
                    merged_contributors["l2_address"] = merged_contributors["l2_address"].fillna("")
                dedup_cols = ["email", "round_id", "op_amt"]
                valid_dedup_cols = [col for col in dedup_cols if col in merged_contributors.columns]
                if len(valid_dedup_cols) == len(dedup_cols):
                    merged_contributors.drop_duplicates(
                        subset=valid_dedup_cols, inplace=True, keep="last"
                    )
                processed["all_contributors_view"] = merged_contributors
            else:
                processed["all_contributors_view"] = pd.DataFrame()

            processed["all_projects_view"] = processed["typeform_df"]
            if processed["typeform_df"] is not None and not processed["typeform_df"].empty:
                if (
                    processed["projects_legacy_df"] is not None
                    and not processed["projects_legacy_df"].empty
                    and "grant_id" in processed["projects_legacy_df"].columns
                ):
                    processed["typeform_df"]["grant_id"] = processed["typeform_df"][
                        "grant_id"
                    ].astype(str)
                    processed["projects_legacy_df"]["grant_id"] = processed["projects_legacy_df"][
                        "grant_id"
                    ].astype(str)
                    processed["projects_legacy_df"] = (
                        processed["projects_legacy_df"]
                        .sort_values("grant_id")
                        .drop_duplicates("grant_id", keep="last")
                    )
                    processed["all_projects_view"] = pd.merge(
                        processed["typeform_df"],
                        processed["projects_legacy_df"][["grant_id", "project_name", "round_id"]],
                        on="grant_id",
                        how="left",
                    )
                else:
                    processed["all_projects_view"] = processed["typeform_df"].copy()
                    if "project_name" not in processed["all_projects_view"].columns:
                        processed["all_projects_view"]["project_name"] = "Unknown"
                    if "round_id" not in processed["all_projects_view"].columns:
                        processed["all_projects_view"]["round_id"] = "Unknown"

                grant_statuses = []
                for _, row in processed["all_projects_view"].iterrows():
                    kyc_emails = row.get("kyc_emails", [])
                    kyb_emails = row.get("kyb_emails", [])
                    individual_statuses = []
                    if (
                        not processed["all_persons_df"].empty
                        and "email" in processed["all_persons_df"].columns
                    ):
                        statuses = processed["all_persons_df"][
                            processed["all_persons_df"]["email"].isin(kyc_emails)
                        ]["status"].tolist()
                        individual_statuses.extend(
                            statuses + [STATUS_NOT_STARTED] * (len(kyc_emails) - len(statuses))
                        )
                    else:
                        individual_statuses.extend([STATUS_NOT_STARTED] * len(kyc_emails))
                    if (
                        not processed["all_businesses_df"].empty
                        and "email" in processed["all_businesses_df"].columns
                    ):
                        statuses = processed["all_businesses_df"][
                            processed["all_businesses_df"]["email"].isin(kyb_emails)
                        ]["status"].tolist()
                        individual_statuses.extend(
                            statuses + [STATUS_NOT_STARTED] * (len(kyb_emails) - len(statuses))
                        )
                    else:
                        individual_statuses.extend([STATUS_NOT_STARTED] * len(kyb_emails))

                    if not kyc_emails and not kyb_emails:
                        overall_status = STATUS_NO_FORM
                    elif any(s == STATUS_REJECTED for s in individual_statuses):
                        overall_status = STATUS_REJECTED
                    elif all(
                        s == STATUS_CLEARED for s in individual_statuses if s != STATUS_NOT_STARTED
                    ):
                        if any(s != STATUS_NOT_STARTED for s in individual_statuses):
                            overall_status = STATUS_CLEARED
                        else:
                            overall_status = STATUS_NOT_STARTED
                    elif any(s == STATUS_IN_REVIEW for s in individual_statuses):
                        overall_status = STATUS_IN_REVIEW
                    elif any(s == STATUS_INCOMPLETE for s in individual_statuses):
                        overall_status = STATUS_INCOMPLETE
                    elif any(s == STATUS_RETRY for s in individual_statuses):
                        overall_status = STATUS_RETRY
                    elif any(s == STATUS_EXPIRED for s in individual_statuses):
                        overall_status = STATUS_EXPIRED
                    else:
                        overall_status = STATUS_NOT_STARTED
                    grant_statuses.append(overall_status)

                processed["all_projects_view"]["overall_status"] = grant_statuses
            else:
                processed["all_projects_view"] = pd.DataFrame()
        return processed

    processed_data = process_and_consolidate_data(loaded_data, typeform_num_controllers_field_id)
    st.session_state.status_indicator.success("Data loaded and processed. Ready.")

    all_persons_df = processed_data.get("all_persons_df", pd.DataFrame())
    all_businesses_df = processed_data.get("all_businesses_df", pd.DataFrame())
    all_contributors_view = processed_data.get("all_contributors_view", pd.DataFrame())
    all_projects_view = processed_data.get("all_projects_view", pd.DataFrame())

    # --- Display Information ---
    st.subheader("Project Status Information")
    with st.expander("About the Results & Status Codes", expanded=False):
        st.markdown(
            "**Every project must complete KYC (or KYB for businesses) in order to receive tokens or join the Superchain.**"
        )
        st.info(
            "This tool looks up the **latest known status** by combining live data (Persona) and historical records. Expiration (1 year) is applied to 'cleared' statuses."
        )
        st.markdown("**Status Code Meanings:**")
        st.markdown(f"- **{STATUS_CLEARED}**: Approved and current.")
        st.markdown(f"- **{STATUS_REJECTED}**: Declined. Cannot proceed.")
        st.markdown(f"- **{STATUS_IN_REVIEW}**: Pending compliance review (allow up to 72 hours).")
        st.markdown(
            f"- **{STATUS_RETRY}**: Incomplete submission (Persona: pending/created). Needs re-attempt at kyc.optimism.io."
        )
        st.markdown(
            f"- **{STATUS_INCOMPLETE}**: Business verification waiting on controller(s) (Persona Case: Waiting on UBOs). Check emails."
        )
        st.markdown(
            f"- **{STATUS_EXPIRED}**: Previously cleared, but older than {EXPIRATION_DAYS} days. Needs renewal."
        )
        st.markdown(f"- **{STATUS_NOT_STARTED}**: No KYC/KYB record found for this email/entity.")
        st.markdown(
            f"- **{STATUS_NO_FORM}**: Grant ID found, but no associated Typeform submission linking emails."
        )
        st.markdown(f"- **{STATUS_UNKNOWN}**: An unexpected status was encountered from the API.")

    # --- UI Interaction ---
    st.sidebar.header("🔍 Database Lookup")
    option = st.sidebar.selectbox(
        "Search By", ["Individual (KYC)", "Business (KYB)", "Grant ID"], key="search_option"
    )
    search_term_input = st.sidebar.text_input(
        "Enter Search Term (Email, Name, L2 Address, Grant ID)", key="search_term"
    )

    if st.sidebar.button("Clear Cache & Refresh Data"):
        st.cache_data.clear()
        st.session_state.clear()
        st.success("Cache cleared. Rerunning...")
        st.rerun()

    # --- Display Search Results ---
    st.divider()
    st.header("Search Results")
    results_df = pd.DataFrame()

    search_term_to_use = search_term_input.strip()
    search_triggered = bool(search_term_to_use)

    if search_triggered:
        # Add search context (Improvement 2)
        st.markdown(f"Searching **{option}** for: `{search_term_to_use}`")
        with st.spinner("Searching..."):
            if option == "Individual (KYC)":
                search_fields = ["name", "email", "l2_address"]
                results_df = perform_search(
                    all_contributors_view, search_term_to_use, search_fields
                )
                display_search_results(results_df, option, all_persons_df, all_businesses_df)
            elif option == "Business (KYB)":
                search_fields = ["business_name", "email", "l2_address"]
                results_df = perform_search(all_businesses_df, search_term_to_use, search_fields)
                display_search_results(results_df, option, all_persons_df, all_businesses_df)
            elif option == "Grant ID":
                search_fields = [
                    "grant_id",
                    "project_name",
                    "l2_address",
                    "kyc_emails",
                    "kyb_emails",
                ]
                term_lower = search_term_to_use.lower()
                if all_projects_view is not None and not all_projects_view.empty:
                    mask = pd.Series(False, index=all_projects_view.index)
                    for field in ["grant_id", "project_name", "l2_address"]:
                        if field in all_projects_view.columns:
                            mask |= (
                                all_projects_view[field]
                                .fillna("")
                                .astype(str)
                                .str.lower()
                                .str.contains(term_lower, na=False)
                            )
                    for email_list_col in ["kyc_emails", "kyb_emails"]:
                        if email_list_col in all_projects_view.columns:
                            mask |= all_projects_view[email_list_col].apply(
                                lambda emails: any(term_lower in email.lower() for email in emails)
                                if isinstance(emails, list)
                                else False
                            )
                    results_df = all_projects_view[mask]
                else:
                    results_df = pd.DataFrame()
                display_search_results(results_df, option, all_persons_df, all_businesses_df)
    else:
        st.info("Use the sidebar to search for an individual, business, or grant.")

    # --- Display Filterable Tables ---
    st.divider()
    st.header("Consolidated Views")

    # --- Filtering Widgets ---
    st.sidebar.header("📊 Filter Views")
    statuses_contributors = (
        all_contributors_view["status"]
        if all_contributors_view is not None and "status" in all_contributors_view
        else pd.Series()
    )
    statuses_businesses = (
        all_businesses_df["status"]
        if all_businesses_df is not None and "status" in all_businesses_df
        else pd.Series()
    )
    statuses_projects = (
        all_projects_view["overall_status"]
        if all_projects_view is not None and "overall_status" in all_projects_view
        else pd.Series()
    )
    available_statuses = list(
        pd.concat([statuses_contributors, statuses_businesses, statuses_projects]).dropna().unique()
    )
    available_statuses.sort()
    available_rounds = (
        list(all_projects_view["round_id"].dropna().unique())
        if all_projects_view is not None and "round_id" in all_projects_view
        else []
    )
    available_rounds.sort()
    default_statuses = available_statuses
    default_rounds = available_rounds
    selected_statuses = st.sidebar.multiselect(
        "Filter by Status", available_statuses, default=default_statuses
    )
    selected_rounds = st.sidebar.multiselect(
        "Filter by Round ID (Projects)", available_rounds, default=default_rounds
    )

    # --- Apply Filters and Display Tabs ---
    # Define base column config for tables (Improvement 1)
    table_column_config = {
        "updated_at": st.column_config.DatetimeColumn("Last Updated", format="YYYY-MM-DD"),
        "submitted_at": st.column_config.DatetimeColumn("Form Submitted", format="YYYY-MM-DD"),
        "l2_address": st.column_config.TextColumn("L2 Address", width="medium"),
        "op_amt": st.column_config.NumberColumn("OP Amount", format="%.2f"),
        # Hide internal IDs by default
        "inquiry_id": None,
        "case_id": None,
        "form_id": None,
        "project_id": None,
        "source": None,  # Hide source column in tables
        "kyc_emails": None,  # Hide email lists in project table
        "kyb_emails": None,
    }

    tab1, tab2, tab3 = st.tabs(["Contributors (KYC)", "Businesses (KYB)", "Projects (Grants)"])

    with tab1:
        st.subheader("All Contributors (Latest KYC Status)")
        if all_contributors_view is not None and not all_contributors_view.empty:
            if "status" in all_contributors_view.columns:
                filtered_contributors = all_contributors_view[
                    all_contributors_view["status"].isin(selected_statuses)
                ]
                st.dataframe(
                    filtered_contributors,
                    use_container_width=True,
                    column_config=table_column_config,
                    hide_index=True,
                )  # Apply config
                st.caption(
                    f"Displaying {len(filtered_contributors)} of {len(all_contributors_view)} contributors."
                )
            else:
                st.warning("Missing 'status' column in contributor data.")
                st.dataframe(
                    all_contributors_view,
                    use_container_width=True,
                    column_config=table_column_config,
                    hide_index=True,
                )  # Apply config
        else:
            st.warning("No contributor data available.")

    with tab2:
        st.subheader("All Businesses (Latest KYB Status)")
        if all_businesses_df is not None and not all_businesses_df.empty:
            if "status" in all_businesses_df.columns:
                filtered_businesses = all_businesses_df[
                    all_businesses_df["status"].isin(selected_statuses)
                ]
                st.dataframe(
                    filtered_businesses,
                    use_container_width=True,
                    column_config=table_column_config,
                    hide_index=True,
                )  # Apply config
                st.caption(
                    f"Displaying {len(filtered_businesses)} of {len(all_businesses_df)} businesses."
                )
            else:
                st.warning("Missing 'status' column in business data.")
                st.dataframe(
                    all_businesses_df,
                    use_container_width=True,
                    column_config=table_column_config,
                    hide_index=True,
                )  # Apply config
        else:
            st.warning("No business data available.")

    with tab3:
        st.subheader("All Projects (Overall Grant Status)")
        if all_projects_view is not None and not all_projects_view.empty:
            status_col_exists = "overall_status" in all_projects_view.columns
            round_col_exists = "round_id" in all_projects_view.columns
            if status_col_exists and round_col_exists:
                filtered_projects = all_projects_view[
                    all_projects_view["overall_status"].isin(selected_statuses)
                    & all_projects_view["round_id"].isin(selected_rounds)
                ]
            elif status_col_exists:
                filtered_projects = all_projects_view[
                    all_projects_view["overall_status"].isin(selected_statuses)
                ]
                st.warning("Missing 'round_id' column for filtering projects.")
            elif round_col_exists:
                filtered_projects = all_projects_view[
                    all_projects_view["round_id"].isin(selected_rounds)
                ]
                st.warning("Missing 'overall_status' column for filtering projects.")
            else:
                filtered_projects = all_projects_view
                st.warning(
                    "Missing 'overall_status' and 'round_id' columns for filtering projects."
                )

            cols_to_show = [
                "grant_id",
                "project_name",
                "round_id",
                "overall_status",
                "l2_address",
                "submitted_at",
            ]
            st.dataframe(
                filtered_projects[
                    [col for col in cols_to_show if col in filtered_projects.columns]
                ],
                use_container_width=True,
                column_config=table_column_config,
                hide_index=True,
            )  # Apply config
            st.caption(f"Displaying {len(filtered_projects)} of {len(all_projects_view)} projects.")
        else:
            st.warning("No project data available.")


# --- Entry Point ---
if __name__ == "__main__":
    main()
