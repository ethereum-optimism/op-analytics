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
CACHE_TTL_SECONDS = 600  # 10 minutes (Applies to Typeform/GitHub now)
EXPIRATION_DAYS = 365  # KYC/KYB expiration period
FILTER_LAST_YEAR_DAYS = 365  # Only keep records within the last year
PERSONA_MAX_PAGES_DEBUG = 1  # Max pages to fetch in debug mode

# Status Constants
STATUS_CLEARED = "🟢 Cleared"
STATUS_REJECTED = "🛑 Rejected"
STATUS_IN_REVIEW = "🟠 In Review"
STATUS_RETRY = "🌕 Retry (Incomplete KYC)"  # Persona Inquiry status: pending/created/expired
STATUS_INCOMPLETE = (
    "🔵 Incomplete (KYB)"  # Persona Case status: Waiting on UBOs/pending/created/expired
)
STATUS_EXPIRED = "⚫ Expired"
STATUS_NOT_STARTED = "⚪ Not Started"
STATUS_UNKNOWN = "❓ Unknown"
STATUS_NO_FORM = "📄 No Form Data"

# --- Session State Keys ---
SESSION_KEY_PERSONA_INQUIRIES = "raw_persona_inquiries"
SESSION_KEY_PERSONA_CASES = "raw_persona_cases"
SESSION_KEY_DATA_LOADED = "initial_data_loaded"
SESSION_KEY_APPLIED_FILTERS = "applied_filters"


# --- Authentication ---
def check_credentials() -> bool:
    """Returns `True` if the user is authenticated."""

    # (Authentication logic remains the same)
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
                        st.error("Authentication configuration missing in secrets.")
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


def get_date_filter_threshold() -> datetime:
    """Returns the datetime threshold for filtering records within the last year."""
    return datetime.now(timezone.utc) - timedelta(days=FILTER_LAST_YEAR_DAYS)


def format_date_for_persona_api(dt: datetime) -> str:
    """Formats a datetime for Persona API date filtering."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# Persona fetching is NOT cached to allow UI updates during initial load
def fetch_persona_data(
    api_key: str,
    endpoint: str,
    status_indicator_ref,  # Pass the UI element reference
    debug_mode: bool = False,
) -> Optional[List[Dict[str, Any]]]:
    """
    Fetches paginated data from a Persona API endpoint with page-level UI updates.
    Limits pages fetched if debug_mode is True.
    Filters records to only include those within the last year.
    Returns None on critical fetch error, empty list on success with no data.
    NOTE: Caching is disabled for this function to allow UI updates.
    """
    results = []
    headers = {"Authorization": f"Bearer {api_key}", "Persona-Version": "2023-01-05"}

    # Add date filtering for the last year
    date_threshold = get_date_filter_threshold()
    date_filter = format_date_for_persona_api(date_threshold)

    params: Dict[str, Any] = {
        "page[size]": 100,
        "filter[updated-at-gte]": date_filter,  # Filter records updated in the last year
    }

    request_url = f"{PERSONA_BASE_URL}/{endpoint}"
    page_count = 0
    max_pages = PERSONA_MAX_PAGES_DEBUG if debug_mode else 10000
    debug_msg = " (Debug Mode)" if debug_mode else ""
    filter_msg = f" (Last {FILTER_LAST_YEAR_DAYS} days)"

    # Initial message
    status_indicator_ref.info(f"⏳ Fetching Persona {endpoint}{debug_msg}{filter_msg}...")

    while True:
        if page_count >= max_pages:
            status_indicator_ref.warning(
                f"⚠️ Reached max page limit ({max_pages}) for Persona {endpoint}{debug_msg}."
            )
            break

        response = None
        page_num_display = page_count + 1

        # Update status before fetching the page
        progress_message = f"⏳ Fetching Persona {endpoint} (Page {page_num_display}){debug_msg}{filter_msg}... Fetched {len(results)} records."
        status_indicator_ref.info(progress_message)

        try:
            response = requests.get(request_url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            response_data = response.json()
            current_page_data = response_data.get("data", [])

            if not current_page_data and page_count == 0 and response.status_code == 200:
                status_indicator_ref.info(
                    f"✅ Finished Persona {endpoint}{debug_msg}. No records found."
                )
                break  # Valid empty response
            elif not current_page_data and page_count > 0:
                status_indicator_ref.info(
                    f"✅ Finished Persona {endpoint}{debug_msg}. Reached end (Page {page_num_display})."
                )
                break  # End of data

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
                        status_indicator_ref.warning(
                            f"⚠️ Persona {endpoint}: 'next' link found but no cursor. Stopping."
                        )
                        break
                except Exception as e:
                    status_indicator_ref.error(
                        f"❌ Error parsing Persona 'next' link for {endpoint}: {e}"
                    )
                    break
            else:
                status_indicator_ref.info(
                    f"✅ Finished Persona {endpoint}{debug_msg}{filter_msg}. No 'next' link found (Page {page_num_display})."
                )
                break  # No more pages
            page_count += 1

        except requests.exceptions.Timeout:
            error_msg = f"❌ Timeout error fetching Persona {endpoint} (Page {page_num_display}, URL: {request_url})"
            st.error(error_msg)
            status_indicator_ref.error(error_msg)
            return None  # Indicate critical fetch failure
        except requests.exceptions.RequestException as e:
            error_url = request_url if response is None else response.url
            error_msg = f"❌ Error fetching Persona {endpoint} (Page {page_num_display}, URL: {error_url}): {e}"
            st.error(error_msg)
            if response is not None:
                st.error(f"Response status: {response.status_code}, Content: {response.text[:500]}")
            status_indicator_ref.error(error_msg)
            return None  # Indicate fetch failure
        except Exception as e:
            error_msg = f"❌ Unexpected error during Persona fetch {endpoint} (Page {page_num_display}): {e}"
            st.error(error_msg)
            status_indicator_ref.error(error_msg)
            return None  # Indicate unexpected error

    # Final confirmation message
    final_status = f"✅ Finished fetching {len(results)} records from Persona {endpoint}{debug_msg}{filter_msg}."
    status_indicator_ref.info(final_status)

    return results


# Typeform and GitHub fetching remain cached
@st.cache_data(ttl=CACHE_TTL_SECONDS)
def fetch_typeform_data(api_key: str, form_id: str) -> Optional[List[Dict[str, Any]]]:
    """Fetches paginated data for a Typeform form, filtering to last year only. (Cached)"""
    # Calculate date filter for the last year
    date_threshold = get_date_filter_threshold()
    since_date = date_threshold.strftime("%Y-%m-%dT%H:%M:%SZ")

    all_items = []
    headers = {"Authorization": f"Bearer {api_key}"}
    url = f"{TYPEFORM_BASE_URL}/forms/{form_id}/responses"
    params: Dict[str, Any] = {
        "page_size": 1000,
        "since": since_date,  # Filter responses submitted in the last year
    }
    page_count = 0
    max_pages = 20
    total_items_expected = None

    while page_count < max_pages:
        response = None
        page_num_display = page_count + 1

        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            items = data.get("items", [])
            all_items.extend(items)

            if page_count == 0 and "total_items" in data:
                total_items_expected = data["total_items"]

            retrieved_items = len(all_items)
            current_page_size = len(items)

            if (
                (total_items_expected is not None and retrieved_items >= total_items_expected)
                or current_page_size < params.get("page_size", 1000)
                or current_page_size == 0
            ):
                break

            if items:
                last_token = items[-1].get("token")
                if last_token:
                    params["after"] = last_token
                else:
                    st.warning(
                        f"Could not find 'token' in last Typeform item (page {page_num_display}) for pagination. Stopping."
                    )
                    break
            else:
                break
        except requests.exceptions.Timeout:
            st.error(
                f"Timeout error fetching data from Typeform {form_id} (Page {page_num_display}, URL: {url})"
            )
            return None
        except requests.exceptions.RequestException as e:
            error_url = url if response is None else response.url
            st.error(
                f"Error fetching data from Typeform {form_id} (Page {page_num_display}, URL: {error_url}): {e}"
            )
            if response is not None:
                st.error(f"Response status: {response.status_code}, Content: {response.text[:500]}")
            return None
        except Exception as e:
            st.error(
                f"An unexpected error occurred during Typeform fetch (Page {page_num_display}): {e}"
            )
            return None
        page_count += 1

    if page_count >= max_pages:
        st.warning(
            f"Reached maximum page limit ({max_pages}) for Typeform {form_id}. Data might be incomplete."
        )
    return all_items


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def fetch_github_csv(access_token: str, owner: str, repo: str, path: str) -> Optional[pd.DataFrame]:
    """Fetches a CSV file from a GitHub repository and returns a DataFrame, filtered to last year. (Cached)"""
    url = f"{GITHUB_BASE_API_URL}/{path}"
    headers = {"Authorization": f"token {access_token}", "Accept": "application/vnd.github.v3.raw"}

    response = None
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        csv_content = response.content.decode("utf-8")
        df = pd.read_csv(StringIO(csv_content))

        # Apply date filtering to the CSV data
        df = filter_csv_by_date(df, path)

        return df
    except requests.exceptions.Timeout:
        st.error(f"Timeout error fetching GitHub file {path} from {url}")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch GitHub file {path}: {e}")
        if response is not None:
            st.error(f"Response status: {response.status_code}")
            if response.status_code == 401:
                st.error("GitHub token seems invalid or lacks 'repo' scope permissions.")
            elif response.status_code == 404:
                st.error(f"File not found at path: {path}. Check owner, repo, and path.")
            elif response.status_code == 403:
                st.error(
                    f"Access forbidden for GitHub file: {path}. Check token permissions or rate limits."
                )
        return None
    except pd.errors.EmptyDataError:
        st.warning(f"GitHub CSV file {path} is empty.")
        return pd.DataFrame()
    except UnicodeDecodeError:
        st.error(f"Failed to decode GitHub file {path} as UTF-8. Is it a valid CSV?")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred fetching or parsing GitHub CSV {path}: {e}")
        return None


def filter_csv_by_date(df: pd.DataFrame, file_path: str) -> pd.DataFrame:
    """Filters CSV data to only include records within the last year based on available date columns."""
    if df is None or df.empty:
        return df

    date_threshold = get_date_filter_threshold()
    original_count = len(df)

    # Define potential date columns for different CSV files
    date_columns_map = {
        CONTRIBUTORS_PATH: ["updated_at", "created_at", "date"],
        PROJECTS_PATH: ["updated_at", "created_at", "date", "submitted_at"],
        PERSONS_LEGACY_PATH: ["updated_at", "created_at", "date"],
        BUSINESSES_LEGACY_PATH: ["updated_at", "created_at", "date"],
    }

    potential_date_cols = date_columns_map.get(
        file_path, ["updated_at", "created_at", "date", "submitted_at"]
    )

    # Find the first available date column
    date_col_to_use = None
    for col in potential_date_cols:
        if col in df.columns:
            date_col_to_use = col
            break

    if date_col_to_use is None:
        st.warning(f"No date column found in {file_path} for filtering. Including all records.")
        return df

    # Convert date column and filter
    try:
        df[date_col_to_use] = pd.to_datetime(df[date_col_to_use], errors="coerce", utc=True)
        # Filter to records within the last year
        filtered_df = df[df[date_col_to_use] >= date_threshold].copy()
        filtered_count = len(filtered_df)

        if filtered_count < original_count:
            st.info(
                f"Filtered {file_path}: {filtered_count}/{original_count} records within last {FILTER_LAST_YEAR_DAYS} days (by {date_col_to_use})"
            )

        return filtered_df
    except Exception as e:
        st.warning(
            f"Error filtering {file_path} by date column {date_col_to_use}: {e}. Including all records."
        )
        return df


# --- Data Processing Functions ---
# (process_inquiries, process_cases, process_typeform, combine_legacy_live remain the same)
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
        full_name = " ".join(filter(None, [name_first, name_middle, name_last])).strip()
        email_raw = attributes.get("email-address", "") or ""
        email = email_raw.lower().strip() if isinstance(email_raw, str) and "@" in email_raw else ""
        updated_at_raw = attributes.get("updated-at")
        updated_at = pd.to_datetime(updated_at_raw, errors="coerce", utc=True)
        l2_address_raw = attributes.get("fields", {}).get("l-2-address", {}).get("value")
        l2_address = ""
        if isinstance(l2_address_raw, str) and l2_address_raw.lower().strip().startswith("0x"):
            l2_address = l2_address_raw.lower().strip()

        status = STATUS_UNKNOWN
        if status_raw == "approved":
            status = STATUS_CLEARED
        elif status_raw in ["pending", "created", "expired"]:
            status = STATUS_RETRY
        elif status_raw == "declined":
            status = STATUS_REJECTED
        elif status_raw == "needs_review":
            status = STATUS_IN_REVIEW

        if inquiry_id and email:
            records.append(
                {
                    "inquiry_id": inquiry_id,
                    "name": full_name if full_name else None,
                    "email": email,
                    "l2_address": l2_address if l2_address else None,
                    "updated_at": updated_at,
                    "status": status,
                    "source": "persona_inquiry",
                }
            )
    df = pd.DataFrame(records)
    if not df.empty:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True)
        df["l2_address"] = df["l2_address"].fillna("").astype(str)
        df["name"] = df["name"].fillna("").astype(str)
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
        l2_address = ""
        if isinstance(l2_address_raw, str) and l2_address_raw.lower().strip().startswith("0x"):
            l2_address = l2_address_raw.lower().strip()

        status = STATUS_UNKNOWN
        if status_raw == "Approved":
            status = STATUS_CLEARED
        elif status_raw in ["pending", "created", "Waiting on UBOs", "expired"]:
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
                    "l2_address": l2_address if l2_address else None,
                    "updated_at": updated_at,
                    "status": status,
                    "source": "persona_case",
                }
            )
    df = pd.DataFrame(records)
    if not df.empty:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True)
        df["l2_address"] = df["l2_address"].fillna("").astype(str)
        df["business_name"] = df["business_name"].fillna("").astype(str)
    return df


def process_typeform(
    raw_typeform_data: List[Dict[str, Any]], num_controllers_field_id: Optional[str]
) -> pd.DataFrame:
    """Processes raw Typeform response data into a structured DataFrame."""
    if not raw_typeform_data:
        return pd.DataFrame()
    if not num_controllers_field_id:
        st.error(
            "Typeform Number of Controllers Field ID not configured. Cannot reliably distinguish KYC/KYB emails."
        )

    form_entries = []
    for item in raw_typeform_data:
        response_id = item.get("response_id")
        submitted_at_raw = item.get("submitted_at")
        submitted_at = pd.to_datetime(submitted_at_raw, errors="coerce", utc=True)
        hidden = item.get("hidden", {}) or {}
        grant_id = hidden.get("grant_id")
        project_id = hidden.get("project_id")
        l2_address_hidden = hidden.get("l2_address")

        if pd.isna(grant_id):
            continue

        l2_address = ""
        if isinstance(l2_address_hidden, str) and l2_address_hidden.lower().strip().startswith(
            "0x"
        ):
            l2_address = l2_address_hidden.lower().strip()

        kyc_emails, kyb_emails = [], []
        number_of_kyb_controllers = 0
        controller_field_encountered = False
        answers = item.get("answers", [])
        if not isinstance(answers, list):
            answers = []

        if num_controllers_field_id:
            for answer in answers:
                field = answer.get("field", {})
                if field.get("id") == num_controllers_field_id and field.get("type") == "number":
                    number_of_kyb_controllers = answer.get("number", 0)
                    if (
                        not isinstance(number_of_kyb_controllers, int)
                        or number_of_kyb_controllers < 0
                    ):
                        number_of_kyb_controllers = 0
                    break

        controller_field_encountered = False
        potential_kyb_emails = []
        for answer in answers:
            field = answer.get("field", {})
            field_id = field.get("id")
            field_type = field.get("type")

            if num_controllers_field_id and field_id == num_controllers_field_id:
                controller_field_encountered = True
                continue

            if field_type == "email":
                email_raw = answer.get("email")
                if isinstance(email_raw, str) and "@" in email_raw:
                    email_clean = email_raw.lower().strip()
                    if num_controllers_field_id and controller_field_encountered:
                        potential_kyb_emails.append(email_clean)
                    else:
                        kyc_emails.append(email_clean)

        if num_controllers_field_id and number_of_kyb_controllers > 0:
            kyb_emails = potential_kyb_emails[:number_of_kyb_controllers]

        kyc_emails = sorted(list(set(kyc_emails)))
        kyb_emails = sorted(list(set(kyb_emails)))

        form_entries.append(
            {
                "form_id": response_id,
                "project_id": project_id,
                "grant_id": str(grant_id).strip(),
                "l2_address": l2_address if l2_address else None,
                "submitted_at": submitted_at,
                "kyc_emails": kyc_emails,
                "kyb_emails": kyb_emails,
            }
        )

    if not form_entries:
        return pd.DataFrame()
    df = pd.DataFrame(form_entries)
    df["submitted_at"] = pd.to_datetime(df["submitted_at"], errors="coerce", utc=True)
    df["l2_address"] = df["l2_address"].fillna("").astype(str)
    df["grant_id"] = df["grant_id"].astype(str)
    df["kyc_emails"] = df["kyc_emails"].apply(lambda x: x if isinstance(x, list) else [])
    df["kyb_emails"] = df["kyb_emails"].apply(lambda x: x if isinstance(x, list) else [])
    return df


def combine_legacy_live(
    legacy_df: Optional[pd.DataFrame],
    live_df: Optional[pd.DataFrame],
    group_by_cols: List[str],
    id_col: str,
    date_col: str = "updated_at",
) -> pd.DataFrame:
    """Combines legacy and live data, determines latest record, applies expiration."""
    if legacy_df is None or legacy_df.empty:
        legacy_df = pd.DataFrame()
    if live_df is None or live_df.empty:
        live_df = pd.DataFrame()

    if legacy_df.empty and live_df.empty:
        expected_cols = group_by_cols + [
            id_col,
            date_col,
            "status",
            "l2_address",
            "name",
            "business_name",
            "source",
        ]
        return pd.DataFrame(columns=expected_cols)

    if not legacy_df.empty and "source" not in legacy_df.columns:
        legacy_df["source"] = "legacy"
    if not live_df.empty and "source" not in live_df.columns:
        live_df["source"] = "live"

    epoch_start = pd.Timestamp("1970-01-01", tz="UTC")
    for df in [legacy_df, live_df]:
        if not df.empty and date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce", utc=True)

    potential_cols = set(group_by_cols) | {
        id_col,
        date_col,
        "status",
        "l2_address",
        "name",
        "business_name",
        "source",
    }
    if not legacy_df.empty:
        potential_cols.update(legacy_df.columns)
    if not live_df.empty:
        potential_cols.update(live_df.columns)
    all_cols = list(potential_cols)

    if not legacy_df.empty:
        legacy_df = legacy_df.reindex(columns=all_cols, fill_value=np.nan)
    if not live_df.empty:
        live_df = live_df.reindex(columns=all_cols, fill_value=np.nan)

    combined_df = pd.concat([legacy_df, live_df], ignore_index=True)

    if date_col in combined_df.columns:
        combined_df[date_col] = pd.to_datetime(
            combined_df[date_col], errors="coerce", utc=True
        ).fillna(epoch_start)
    for col in group_by_cols:
        if col in combined_df.columns:
            combined_df[col] = (
                combined_df[col].astype(str).str.lower().str.strip().replace("nan", "", regex=False)
            )

    valid_group_by_cols = [col for col in group_by_cols if col in combined_df.columns]
    if not valid_group_by_cols:
        st.error(f"Critical Error: Group-by columns {group_by_cols} not found.")
        return pd.DataFrame(columns=all_cols)

    combined_df = combined_df.sort_values(
        by=valid_group_by_cols + [date_col],
        ascending=[True] * len(valid_group_by_cols) + [False],
        na_position="last",
    )
    latest_df = combined_df.drop_duplicates(subset=valid_group_by_cols, keep="first").copy()

    if "status" in latest_df.columns and date_col in latest_df.columns:
        current_date_utc = datetime.now(timezone.utc)
        expiration_threshold = current_date_utc - timedelta(days=EXPIRATION_DAYS)
        latest_df[date_col] = pd.to_datetime(latest_df[date_col], errors="coerce", utc=True)
        expired_mask = (
            (latest_df["status"] == STATUS_CLEARED)
            & (latest_df[date_col].notna())
            & (latest_df[date_col] < expiration_threshold)
        )
        latest_df.loc[expired_mask, "status"] = STATUS_EXPIRED
        latest_df["status"] = latest_df["status"].fillna(STATUS_NOT_STARTED)
    else:
        st.warning(f"Columns 'status' or '{date_col}' not found. Cannot apply expiration logic.")
        if "status" not in latest_df.columns:
            latest_df["status"] = STATUS_NOT_STARTED
        else:
            latest_df["status"] = latest_df["status"].fillna(STATUS_NOT_STARTED)

    if date_col in latest_df.columns:
        latest_df[date_col] = latest_df[date_col].replace(epoch_start, pd.NaT)

    for col in ["l2_address", "name", "business_name", "source", id_col]:
        if col in latest_df.columns:
            latest_df[col] = latest_df[col].fillna("").astype(str)
    for col in valid_group_by_cols:
        latest_df[col] = latest_df[col].astype(str).str.lower().str.strip()

    return latest_df


def detect_data_mismatches(processed_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    """
    Detects and returns mismatches between different data providers.
    Returns a dictionary with different types of mismatches.
    """
    mismatches = {}

    # Extract data sources
    typeform_df = processed_data.get("typeform_df", pd.DataFrame())
    persons_live_df = processed_data.get("persons_live_df", pd.DataFrame())
    businesses_live_df = processed_data.get("businesses_live_df", pd.DataFrame())
    persons_legacy_df = processed_data.get("persons_legacy_df", pd.DataFrame())
    businesses_legacy_df = processed_data.get("businesses_legacy_df", pd.DataFrame())
    contributors_legacy_df = processed_data.get("contributors_legacy_df", pd.DataFrame())
    projects_legacy_df = processed_data.get("projects_legacy_df", pd.DataFrame())

    # 1. Typeform submissions without corresponding Persona records
    if not typeform_df.empty and (not persons_live_df.empty or not businesses_live_df.empty):
        typeform_emails = set()
        for _, row in typeform_df.iterrows():
            kyc_emails = row.get("kyc_emails", [])
            kyb_emails = row.get("kyb_emails", [])
            if isinstance(kyc_emails, list):
                typeform_emails.update([email.lower().strip() for email in kyc_emails])
            if isinstance(kyb_emails, list):
                typeform_emails.update([email.lower().strip() for email in kyb_emails])

        persona_emails = set()
        if not persons_live_df.empty and "email" in persons_live_df.columns:
            persona_emails.update(
                persons_live_df["email"].astype(str).str.lower().str.strip().tolist()
            )
        if not businesses_live_df.empty and "email" in businesses_live_df.columns:
            persona_emails.update(
                businesses_live_df["email"].astype(str).str.lower().str.strip().tolist()
            )

        # Emails in Typeform but not in Persona
        typeform_only_emails = typeform_emails - persona_emails
        if typeform_only_emails:
            typeform_mismatch_data = []
            for _, row in typeform_df.iterrows():
                kyc_emails = row.get("kyc_emails", [])
                kyb_emails = row.get("kyb_emails", [])
                all_emails = []
                if isinstance(kyc_emails, list):
                    all_emails.extend(kyc_emails)
                if isinstance(kyb_emails, list):
                    all_emails.extend(kyb_emails)

                mismatched_emails = [
                    email for email in all_emails if email.lower().strip() in typeform_only_emails
                ]
                if mismatched_emails:
                    typeform_mismatch_data.append(
                        {
                            "grant_id": row.get("grant_id", ""),
                            "project_id": row.get("project_id", ""),
                            "submitted_at": row.get("submitted_at"),
                            "l2_address": row.get("l2_address", ""),
                            "missing_emails": ", ".join(mismatched_emails),
                            "email_type": "KYC"
                            if any(email in kyc_emails for email in mismatched_emails)
                            else "KYB",
                            "mismatch_type": "Typeform submission without Persona record",
                        }
                    )

            if typeform_mismatch_data:
                mismatches["typeform_without_persona"] = pd.DataFrame(typeform_mismatch_data)

    # 2. Persona records without corresponding Typeform submissions
    if (not persons_live_df.empty or not businesses_live_df.empty) and not typeform_df.empty:
        persona_only_emails = persona_emails - typeform_emails
        if persona_only_emails:
            persona_mismatch_data = []

            # Check persons
            if not persons_live_df.empty:
                for _, row in persons_live_df.iterrows():
                    email = str(row.get("email", "")).lower().strip()
                    if email in persona_only_emails:
                        persona_mismatch_data.append(
                            {
                                "email": row.get("email", ""),
                                "name": row.get("name", ""),
                                "inquiry_id": row.get("inquiry_id", ""),
                                "status": row.get("status", ""),
                                "updated_at": row.get("updated_at"),
                                "l2_address": row.get("l2_address", ""),
                                "record_type": "Individual KYC",
                                "mismatch_type": "Persona record without Typeform submission",
                            }
                        )

            # Check businesses
            if not businesses_live_df.empty:
                for _, row in businesses_live_df.iterrows():
                    email = str(row.get("email", "")).lower().strip()
                    if email in persona_only_emails:
                        persona_mismatch_data.append(
                            {
                                "email": row.get("email", ""),
                                "business_name": row.get("business_name", ""),
                                "case_id": row.get("case_id", ""),
                                "status": row.get("status", ""),
                                "updated_at": row.get("updated_at"),
                                "l2_address": row.get("l2_address", ""),
                                "record_type": "Business KYB",
                                "mismatch_type": "Persona record without Typeform submission",
                            }
                        )

            if persona_mismatch_data:
                mismatches["persona_without_typeform"] = pd.DataFrame(persona_mismatch_data)

    # 3. Legacy records without live counterparts
    if not contributors_legacy_df.empty and not persons_live_df.empty:
        legacy_emails = set()
        if "email" in contributors_legacy_df.columns:
            legacy_emails.update(
                contributors_legacy_df["email"].astype(str).str.lower().str.strip().tolist()
            )

        live_emails = set()
        if "email" in persons_live_df.columns:
            live_emails.update(
                persons_live_df["email"].astype(str).str.lower().str.strip().tolist()
            )

        legacy_only_emails = legacy_emails - live_emails
        if legacy_only_emails:
            legacy_mismatch_data = []
            for _, row in contributors_legacy_df.iterrows():
                email = str(row.get("email", "")).lower().strip()
                if email in legacy_only_emails:
                    legacy_mismatch_data.append(
                        {
                            "email": row.get("email", ""),
                            "name": row.get("name", ""),
                            "project_name": row.get("project_name", ""),
                            "round_id": row.get("round_id", ""),
                            "op_amt": row.get("op_amt", ""),
                            "l2_address": row.get("l2_address", ""),
                            "updated_at": row.get("updated_at"),
                            "mismatch_type": "Legacy contributor without live Persona record",
                        }
                    )

            if legacy_mismatch_data:
                mismatches["legacy_without_live"] = pd.DataFrame(legacy_mismatch_data)

    # 4. Projects without corresponding Typeform submissions
    if not projects_legacy_df.empty and not typeform_df.empty:
        legacy_grant_ids = set()
        if "grant_id" in projects_legacy_df.columns:
            legacy_grant_ids.update(projects_legacy_df["grant_id"].astype(str).str.strip().tolist())

        typeform_grant_ids = set()
        if "grant_id" in typeform_df.columns:
            typeform_grant_ids.update(typeform_df["grant_id"].astype(str).str.strip().tolist())

        projects_only_grant_ids = legacy_grant_ids - typeform_grant_ids
        if projects_only_grant_ids:
            projects_mismatch_data = []
            for _, row in projects_legacy_df.iterrows():
                grant_id = str(row.get("grant_id", "")).strip()
                if grant_id in projects_only_grant_ids:
                    projects_mismatch_data.append(
                        {
                            "grant_id": row.get("grant_id", ""),
                            "project_name": row.get("project_name", ""),
                            "round_id": row.get("round_id", ""),
                            "updated_at": row.get("updated_at"),
                            "mismatch_type": "Legacy project without Typeform submission",
                        }
                    )

            if projects_mismatch_data:
                mismatches["projects_without_typeform"] = pd.DataFrame(projects_mismatch_data)

    return mismatches


# --- UI Interaction Functions ---
def perform_search(df: pd.DataFrame, search_term: str, search_fields: List[str]) -> pd.DataFrame:
    """Performs a case-insensitive search across specified fields in a DataFrame."""
    if not search_term or df is None or df.empty:
        return pd.DataFrame(columns=df.columns if df is not None else [])
    search_term_lower = search_term.lower().strip()
    if not search_term_lower:
        return pd.DataFrame(columns=df.columns)

    mask = pd.Series(False, index=df.index)
    for field in search_fields:
        if field in df.columns:
            mask |= (
                df[field]
                .fillna("")
                .astype(str)
                .str.lower()
                .str.contains(search_term_lower, na=False, regex=False)
            )
    return df[mask]


def display_search_results(
    results_df: pd.DataFrame,
    search_type: str,
    all_persons_df: pd.DataFrame,
    all_businesses_df: pd.DataFrame,
):
    """Displays search results in a structured format."""
    if results_df is None or results_df.empty:
        st.info("No matching results found.")
        return

    column_config_base = {
        "updated_at": st.column_config.DatetimeColumn("Last Updated", format="YYYY-MM-DD HH:mm"),
        "submitted_at": st.column_config.DatetimeColumn(
            "Form Submitted", format="YYYY-MM-DD HH:mm"
        ),
        "inquiry_id": st.column_config.TextColumn(
            "Persona Inquiry ID", help="Internal Persona Inquiry ID", width="medium"
        ),
        "case_id": st.column_config.TextColumn(
            "Persona Case ID", help="Internal Persona Case ID", width="medium"
        ),
        "form_id": st.column_config.TextColumn(
            "Typeform Resp ID", help="Internal Typeform Response ID", width="medium"
        ),
        "project_id": st.column_config.TextColumn(
            "Project ID", help="Internal Project ID (from Typeform/Legacy)", width="small"
        ),
        "l2_address": st.column_config.TextColumn("L2 Address", width="large"),
        "op_amt": st.column_config.NumberColumn(
            "OP Amount", format="%.2f", help="OP amount from legacy contributors data"
        ),
        "email": st.column_config.TextColumn("Email", width="medium"),
        "name": st.column_config.TextColumn("Individual Name", width="medium"),
        "business_name": st.column_config.TextColumn("Business Name", width="medium"),
        "project_name": st.column_config.TextColumn("Project Name", width="medium"),
        "round_id": st.column_config.TextColumn("Round ID", width="small"),
        "status": st.column_config.TextColumn("KYC/KYB Status", width="medium"),
        "overall_status": st.column_config.TextColumn("Grant Status", width="medium"),
        "source": st.column_config.TextColumn(
            "Latest Source", help="Origin of the latest status (live/legacy)", width="small"
        ),
        "kyc_emails": None,
        "kyb_emails": None,
    }
    st.markdown("---")

    if search_type == "Individual (KYC)":
        st.subheader("👤 Individual Search Results")
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
            "inquiry_id",
        ]
        display_df = results_df[
            [col for col in cols_to_display if col in results_df.columns]
        ].copy()
        st.dataframe(
            display_df, use_container_width=True, column_config=column_config_base, hide_index=True
        )
        if not display_df.empty:
            first_result = display_df.iloc[0]
            name_display = first_result.get("name") or first_result.get("email", "N/A")
            status_display = first_result.get("status", STATUS_UNKNOWN)
            st.markdown(f"**Latest Status for {name_display}:** {status_display}")

    elif search_type == "Business (KYB)":
        st.subheader("🏢 Business Search Results")
        cols_to_display = [
            "business_name",
            "email",
            "l2_address",
            "status",
            "updated_at",
            "source",
            "case_id",
        ]
        display_df = results_df[
            [col for col in cols_to_display if col in results_df.columns]
        ].copy()
        st.dataframe(
            display_df, use_container_width=True, column_config=column_config_base, hide_index=True
        )
        if not display_df.empty:
            first_result = display_df.iloc[0]
            name_display = first_result.get("business_name") or first_result.get("email", "N/A")
            status_display = first_result.get("status", STATUS_UNKNOWN)
            st.markdown(f"**Latest Status for {name_display}:** {status_display}")

    elif search_type == "Grant ID":
        st.subheader("📄 Grant Search Results")
        cols_to_display_main = [
            "grant_id",
            "project_name",
            "round_id",
            "overall_status",
            "l2_address",
            "submitted_at",
            "form_id",
        ]
        display_df_main = results_df[
            [col for col in cols_to_display_main if col in results_df.columns]
        ].copy()
        st.dataframe(
            display_df_main,
            use_container_width=True,
            column_config=column_config_base,
            hide_index=True,
        )

        st.markdown("#### Linked Individuals & Businesses:")
        for index, grant_row in results_df.iterrows():
            st.markdown(
                f"**Details for Grant:** `{grant_row['grant_id']}` (Project: {grant_row.get('project_name', 'N/A')})"
            )
            kyc_emails = grant_row.get("kyc_emails", [])
            kyb_emails = grant_row.get("kyb_emails", [])

            if kyc_emails:
                st.markdown("**Individual KYC Statuses:**")
                kyc_status_data = []
                person_lookup = {}
                if (
                    all_persons_df is not None
                    and not all_persons_df.empty
                    and "email" in all_persons_df.columns
                ):
                    all_persons_df["email_lookup"] = (
                        all_persons_df["email"].astype(str).str.lower().str.strip()
                    )
                    person_lookup = all_persons_df.set_index("email_lookup").to_dict("index")
                for email in kyc_emails:
                    email_lower = email.lower().strip()
                    person_data = person_lookup.get(email_lower)
                    status = (
                        person_data.get("status", STATUS_NOT_STARTED)
                        if person_data
                        else STATUS_NOT_STARTED
                    )
                    name = person_data.get("name", "") if person_data else ""
                    name_display = name if name else "N/A"
                    last_update_raw = person_data.get("updated_at") if person_data else None
                    last_update = (
                        pd.to_datetime(last_update_raw).strftime("%Y-%m-%d")
                        if pd.notna(last_update_raw)
                        else "N/A"
                    )
                    kyc_status_data.append(
                        {
                            "Email": email,
                            "Name": name_display,
                            "Status": status,
                            "Last Update": last_update,
                        }
                    )
                st.dataframe(
                    pd.DataFrame(kyc_status_data), use_container_width=True, hide_index=True
                )
            else:
                st.markdown("*No individual KYC emails linked via Typeform.*")

            if kyb_emails:
                st.markdown("**Business KYB Statuses:**")
                kyb_status_data = []
                business_lookup = {}
                if (
                    all_businesses_df is not None
                    and not all_businesses_df.empty
                    and "email" in all_businesses_df.columns
                ):
                    all_businesses_df["email_lookup"] = (
                        all_businesses_df["email"].astype(str).str.lower().str.strip()
                    )
                    business_lookup = (
                        all_businesses_df.groupby("email_lookup")
                        .apply(lambda x: x.to_dict("records"))
                        .to_dict()
                    )
                for email in kyb_emails:
                    email_lower = email.lower().strip()
                    business_records = business_lookup.get(email_lower, [])
                    if business_records:
                        for biz_row in business_records:
                            last_update_raw = biz_row.get("updated_at")
                            last_update = (
                                pd.to_datetime(last_update_raw).strftime("%Y-%m-%d")
                                if pd.notna(last_update_raw)
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
                                "Business Name": "N/A (Not Found)",
                                "Status": STATUS_NOT_STARTED,
                                "Last Update": "N/A",
                            }
                        )
                st.dataframe(
                    pd.DataFrame(kyb_status_data), use_container_width=True, hide_index=True
                )
            else:
                st.markdown("*No business KYB emails linked via Typeform.*")
            st.markdown("---")


# --- Main Application Logic ---
def main():
    """Main function to run the Streamlit application."""
    st.title("🗝️ Optimism KYC/KYB Lookup Tool")

    if not check_credentials():
        st.stop()

    st.success(f"Welcome {st.session_state.get('email', 'User')}!")

    # --- Sidebar Setup ---
    st.sidebar.header("⚙️ Settings & Status")
    debug_mode_enabled = st.sidebar.checkbox(
        "Enable Debug Mode (Limit Persona Data)",
        key="debug_mode",
        value=False,
        help="Fetches only the first page of data from Persona API endpoints.",
        disabled=True,
    )
    if "status_indicator" not in st.session_state:
        st.session_state.status_indicator = st.sidebar.empty()
    status_indicator = st.session_state.status_indicator

    if debug_mode_enabled:
        st.warning("🐞 Debug Mode Active: Persona data fetching is limited.", icon="⚠️")

    # --- Load Secrets ---
    # (Secrets loading remains the same)
    status_indicator.info("⏳ Loading secrets...")
    try:
        persona_api_key = st.secrets["persona"]["api_key"]
        typeform_key = st.secrets["typeform"]["typeform_key"]
        github_token = st.secrets["github"]["access_token"]
        typeform_num_controllers_field_id = st.secrets.get("typeform", {}).get(
            "num_controllers_field_id"
        )

        if not persona_api_key or not typeform_key or not github_token:
            raise KeyError("Essential API keys missing.")
        if not typeform_num_controllers_field_id:
            status_indicator.warning("⚠️ Typeform 'num_controllers_field_id' missing.")

    except KeyError as e:
        status_indicator.error(f"❌ Missing Secret Config: {e}")
        st.error(f"Fatal Error: Missing required secret configuration: {e}.")
        st.stop()
    except Exception as e:
        status_indicator.error(f"❌ Error Loading Secrets: {e}")
        st.error(f"Fatal Error loading secrets: {e}.")
        st.stop()
    status_indicator.info("✅ Secrets loaded.")

    # --- Load Data (MODIFIED: Uses Session State for Persona) ---
    @st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
    def load_cached_data(persona_key, tf_key, gh_token, debug_enabled):
        """
        Cached wrapper for loading external data sources (Typeform, GitHub).
        Persona data is handled separately via session state.
        """
        data = {}

        # Typeform data
        typeform_result = fetch_typeform_data(tf_key, TYPEFORM_FORM_ID)
        data["typeform_raw"] = typeform_result if typeform_result is not None else []

        # GitHub CSV data - explicitly check for None to avoid DataFrame boolean ambiguity
        contributors_result = fetch_github_csv(
            gh_token, GITHUB_OWNER, GITHUB_REPO, CONTRIBUTORS_PATH
        )
        data["contributors_legacy"] = (
            contributors_result if contributors_result is not None else pd.DataFrame()
        )

        projects_result = fetch_github_csv(gh_token, GITHUB_OWNER, GITHUB_REPO, PROJECTS_PATH)
        data["projects_legacy"] = projects_result if projects_result is not None else pd.DataFrame()

        persons_result = fetch_github_csv(gh_token, GITHUB_OWNER, GITHUB_REPO, PERSONS_LEGACY_PATH)
        data["persons_legacy"] = persons_result if persons_result is not None else pd.DataFrame()

        businesses_result = fetch_github_csv(
            gh_token, GITHUB_OWNER, GITHUB_REPO, BUSINESSES_LEGACY_PATH
        )
        data["businesses_legacy"] = (
            businesses_result if businesses_result is not None else pd.DataFrame()
        )

        return data

    def load_all_data(persona_key, tf_key, gh_token, debug_enabled):
        """
        Loads all data sources. Fetches Persona data only if not already in session state.
        Uses cached function for external data to avoid repeated API calls during searches.
        """
        data = {}
        fetch_summary = []
        overall_success = True

        # Use the status indicator from session state
        status_ref = st.session_state.status_indicator

        # Check if Persona data is already loaded in this session
        if st.session_state.get(SESSION_KEY_DATA_LOADED, False):
            # Load from session state silently (no status updates to avoid triggering during searches)
            data[SESSION_KEY_PERSONA_INQUIRIES] = st.session_state.get(
                SESSION_KEY_PERSONA_INQUIRIES, []
            )
            data[SESSION_KEY_PERSONA_CASES] = st.session_state.get(SESSION_KEY_PERSONA_CASES, [])
        else:
            # --- Initial Persona Fetching (Not Cached, with UI updates inside) ---
            status_ref.info("⏳ Performing initial Persona data fetch...")
            persona_actions = [
                {
                    "key": SESSION_KEY_PERSONA_INQUIRIES,
                    "endpoint": "inquiries",
                    "label": "Persona Inquiries",
                },
                {"key": SESSION_KEY_PERSONA_CASES, "endpoint": "cases", "label": "Persona Cases"},
            ]
            persona_load_success = True
            for action in persona_actions:
                result = fetch_persona_data(
                    persona_key, action["endpoint"], status_ref, debug_enabled
                )
                if result is None:
                    fetch_summary.append(f"❌ {action['label']}: Failed")
                    data[action["key"]] = []
                    overall_success = False
                    persona_load_success = False  # Mark Persona load failed
                else:
                    count = len(result)
                    fetch_summary.append(f"✅ {action['label']}: {count} records")
                    data[action["key"]] = result
                    # Store successful result in session state
                    st.session_state[action["key"]] = result

            # Set the loaded flag only if both Persona fetches were successful
            if persona_load_success:
                st.session_state[SESSION_KEY_DATA_LOADED] = True
                status_ref.info("✅ Initial Persona fetch complete and stored in session.")
            else:
                status_ref.error("❌ Initial Persona fetch failed. Data may be incomplete.")

                # --- Load cached external data ---
        is_initial_load = not st.session_state.get(SESSION_KEY_DATA_LOADED, False)

        if is_initial_load:
            status_ref.info("⏳ Loading external data sources (cached)...")

        # Load all external data in one cached call
        cached_data = load_cached_data(persona_key, tf_key, gh_token, debug_enabled)
        data.update(cached_data)

        # Display summary only on initial load
        if is_initial_load:
            status_ref.success("✅ All data sources loaded successfully.")

        # Rename keys for consistency before returning (map session keys back)
        data_processed_keys = data.copy()
        data_processed_keys["inquiries_raw"] = data_processed_keys.pop(
            SESSION_KEY_PERSONA_INQUIRIES, []
        )
        data_processed_keys["cases_raw"] = data_processed_keys.pop(SESSION_KEY_PERSONA_CASES, [])

        return data_processed_keys  # Return dict with original expected keys

    # Call the data loading function
    loaded_data = load_all_data(
        persona_api_key,
        typeform_key,
        github_token,
        debug_enabled=debug_mode_enabled,
    )

    # --- Process Data ---
    # Processing function remains cached
    @st.cache_data(max_entries=5)
    def process_and_consolidate_data(data_dict, tf_controller_field_id):
        """Processes raw data and consolidates legacy/live information."""
        # (Implementation remains the same as previous version)
        with st.spinner("⚙️ Processing and consolidating data..."):
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
                legacy_df=processed["persons_legacy_df"],
                live_df=processed["persons_live_df"],
                group_by_cols=["email"],
                id_col="inquiry_id",
                date_col="updated_at",
            )
            processed["all_businesses_df"] = combine_legacy_live(
                legacy_df=processed["businesses_legacy_df"],
                live_df=processed["businesses_live_df"],
                group_by_cols=["email", "business_name"],
                id_col="case_id",
                date_col="updated_at",
            )

            processed["all_contributors_view"] = pd.DataFrame()
            contributors_legacy_df = processed.get("contributors_legacy_df")
            all_persons_df = processed.get("all_persons_df")
            if (
                contributors_legacy_df is not None
                and not contributors_legacy_df.empty
                and "email" in contributors_legacy_df.columns
                and all_persons_df is not None
                and not all_persons_df.empty
                and "email" in all_persons_df.columns
            ):
                person_status_cols = [
                    "email",
                    "status",
                    "l2_address",
                    "updated_at",
                    "name",
                    "source",
                    "inquiry_id",
                ]
                cols_to_merge = [col for col in person_status_cols if col in all_persons_df.columns]
                contrib_merge = contributors_legacy_df.copy()
                contrib_merge["email"] = contrib_merge["email"].astype(str).str.lower().str.strip()
                persons_merge = all_persons_df[cols_to_merge].copy()
                persons_merge["email"] = persons_merge["email"].astype(str).str.lower().str.strip()
                persons_merge = persons_merge.drop_duplicates(subset=["email"], keep="first")
                merged_contributors = pd.merge(
                    contrib_merge,
                    persons_merge,
                    on="email",
                    how="left",
                    suffixes=("_contrib", "_person"),
                )

                for col in ["status", "l2_address", "updated_at", "name", "source", "inquiry_id"]:
                    p_col, c_col = f"{col}_person", f"{col}_contrib"
                    if p_col in merged_contributors.columns:
                        merged_contributors[col] = merged_contributors[p_col].fillna(
                            merged_contributors.get(c_col)
                        )
                        merged_contributors = merged_contributors.drop(
                            columns=[p_col], errors="ignore"
                        )
                        if c_col in merged_contributors.columns:
                            merged_contributors = merged_contributors.drop(
                                columns=[c_col], errors="ignore"
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

                merged_contributors.replace({np.nan: None, pd.NaT: None}, inplace=True)
                for col in [
                    "l2_address",
                    "name",
                    "project_name",
                    "round_id",
                    "source",
                    "inquiry_id",
                ]:
                    if col in merged_contributors.columns:
                        merged_contributors[col] = merged_contributors[col].fillna("").astype(str)
                if "updated_at" in merged_contributors.columns:
                    merged_contributors["updated_at"] = pd.to_datetime(
                        merged_contributors["updated_at"], errors="coerce", utc=True
                    )
                if "op_amt" in merged_contributors.columns:
                    merged_contributors["op_amt"] = pd.to_numeric(
                        merged_contributors["op_amt"], errors="coerce"
                    )

                dedup_cols = ["email", "round_id", "project_name"]
                valid_dedup_cols = [col for col in dedup_cols if col in merged_contributors.columns]
                if len(valid_dedup_cols) > 1:
                    sort_cols = valid_dedup_cols + ["updated_at"]
                    if "updated_at" in merged_contributors.columns:
                        merged_contributors.sort_values(
                            by=sort_cols,
                            ascending=[True] * len(valid_dedup_cols) + [False],
                            na_position="last",
                            inplace=True,
                        )
                        merged_contributors.drop_duplicates(
                            subset=valid_dedup_cols, keep="first", inplace=True
                        )
                    else:
                        merged_contributors.drop_duplicates(
                            subset=valid_dedup_cols, keep="first", inplace=True
                        )
                processed["all_contributors_view"] = merged_contributors

            processed["all_projects_view"] = pd.DataFrame()
            typeform_df = processed.get("typeform_df")
            projects_legacy_df = processed.get("projects_legacy_df")
            all_persons_df = processed.get("all_persons_df", pd.DataFrame())
            all_businesses_df = processed.get("all_businesses_df", pd.DataFrame())
            if typeform_df is not None and not typeform_df.empty:
                projects_view_base = typeform_df.copy()
                projects_view_base["grant_id"] = (
                    projects_view_base["grant_id"].astype(str).str.strip()
                )
                if (
                    projects_legacy_df is not None
                    and not projects_legacy_df.empty
                    and "grant_id" in projects_legacy_df.columns
                    and "project_name" in projects_legacy_df.columns
                ):
                    proj_legacy_merge = projects_legacy_df[
                        ["grant_id", "project_name", "round_id"]
                    ].copy()
                    proj_legacy_merge["grant_id"] = (
                        proj_legacy_merge["grant_id"].astype(str).str.strip()
                    )
                    proj_legacy_merge = proj_legacy_merge.drop_duplicates("grant_id", keep="last")
                    projects_view_base = pd.merge(
                        projects_view_base, proj_legacy_merge, on="grant_id", how="left"
                    )
                else:
                    if "project_name" not in projects_view_base.columns:
                        projects_view_base["project_name"] = "Unknown (Legacy Missing)"
                    if "round_id" not in projects_view_base.columns:
                        projects_view_base["round_id"] = "Unknown (Legacy Missing)"

                projects_view_base["project_name"] = projects_view_base["project_name"].fillna(
                    "Unknown (Legacy Missing)"
                )
                projects_view_base["round_id"] = projects_view_base["round_id"].fillna(
                    "Unknown (Legacy Missing)"
                )

                grant_statuses = []
                persons_status_map = {}
                if (
                    not all_persons_df.empty
                    and "email" in all_persons_df.columns
                    and "status" in all_persons_df.columns
                ):
                    persons_status_map = (
                        all_persons_df.drop_duplicates(subset=["email"], keep="first")
                        .set_index(all_persons_df["email"].astype(str).str.lower().str.strip())[
                            "status"
                        ]
                        .to_dict()
                    )
                businesses_status_map = {}
                if (
                    not all_businesses_df.empty
                    and "email" in all_businesses_df.columns
                    and "status" in all_businesses_df.columns
                ):
                    businesses_status_map = (
                        all_businesses_df.dropna(subset=["email"])
                        .groupby(all_businesses_df["email"].astype(str).str.lower().str.strip())[
                            "status"
                        ]
                        .apply(list)
                        .to_dict()
                    )

                for index, row in projects_view_base.iterrows():
                    kyc_emails = row.get("kyc_emails", [])
                    kyb_emails = row.get("kyb_emails", [])
                    individual_statuses = []
                    for email in kyc_emails:
                        if isinstance(email, str):
                            individual_statuses.append(
                                persons_status_map.get(email.lower().strip(), STATUS_NOT_STARTED)
                            )
                    for email in kyb_emails:
                        best_status_for_email = STATUS_NOT_STARTED
                        if isinstance(email, str):
                            statuses_for_email = businesses_status_map.get(
                                email.lower().strip(), []
                            )
                            if statuses_for_email:
                                if STATUS_REJECTED in statuses_for_email:
                                    best_status_for_email = STATUS_REJECTED
                                elif STATUS_INCOMPLETE in statuses_for_email:
                                    best_status_for_email = STATUS_INCOMPLETE
                                elif STATUS_IN_REVIEW in statuses_for_email:
                                    best_status_for_email = STATUS_IN_REVIEW
                                elif STATUS_EXPIRED in statuses_for_email:
                                    best_status_for_email = STATUS_EXPIRED
                                elif STATUS_CLEARED in statuses_for_email:
                                    best_status_for_email = STATUS_CLEARED
                                else:
                                    best_status_for_email = statuses_for_email[0]
                        individual_statuses.append(best_status_for_email)

                    overall_status = STATUS_UNKNOWN
                    if not kyc_emails and not kyb_emails:
                        overall_status = STATUS_NO_FORM
                    elif not individual_statuses:
                        overall_status = STATUS_NOT_STARTED
                    elif any(s == STATUS_REJECTED for s in individual_statuses):
                        overall_status = STATUS_REJECTED
                    elif any(s == STATUS_INCOMPLETE for s in individual_statuses):
                        overall_status = STATUS_INCOMPLETE
                    elif any(s == STATUS_RETRY for s in individual_statuses):
                        overall_status = STATUS_RETRY
                    elif any(s == STATUS_IN_REVIEW for s in individual_statuses):
                        overall_status = STATUS_IN_REVIEW
                    elif any(s == STATUS_EXPIRED for s in individual_statuses):
                        overall_status = STATUS_EXPIRED
                    elif all(
                        s in [STATUS_CLEARED, STATUS_NOT_STARTED] for s in individual_statuses
                    ) and any(s == STATUS_CLEARED for s in individual_statuses):
                        overall_status = STATUS_CLEARED
                    elif all(s == STATUS_NOT_STARTED for s in individual_statuses):
                        overall_status = STATUS_NOT_STARTED
                    else:
                        overall_status = STATUS_UNKNOWN
                    grant_statuses.append(overall_status)

                projects_view_base["overall_status"] = grant_statuses
                processed["all_projects_view"] = projects_view_base

        # Detect mismatches between data providers
        processed["mismatches"] = detect_data_mismatches(processed)

        return processed

    # Call the processing function (result will be cached)
    processed_data = process_and_consolidate_data(loaded_data, typeform_num_controllers_field_id)

    # Final status update after processing is complete (only on initial load)
    if not st.session_state.get(SESSION_KEY_DATA_LOADED, False) or st.session_state.get(
        "show_final_status", True
    ):
        status_indicator.success("✅ Data loaded and processed. Ready.")
        st.session_state["show_final_status"] = False  # Prevent showing again during searches

    # Extract final DataFrames for display
    all_persons_df = processed_data.get("all_persons_df", pd.DataFrame())
    all_businesses_df = processed_data.get("all_businesses_df", pd.DataFrame())
    all_contributors_view = processed_data.get("all_contributors_view", pd.DataFrame())
    all_projects_view = processed_data.get("all_projects_view", pd.DataFrame())
    mismatches_data = processed_data.get("mismatches", {})

    # --- Display Information ---
    st.subheader("ℹ️ Project Status Information")
    with st.expander("About the Results & Status Codes", expanded=False):
        st.markdown(
            f"""
            This tool provides the latest known KYC (Know Your Customer) or KYB (Know Your Business)
            status for individuals and projects involved with Optimism grants. Status is determined
            by combining live data from Persona (our verification provider) with historical records.

            **Key Points:**
            * **Data Filtering:** Only records from the last **{FILTER_LAST_YEAR_DAYS} days** are included to focus on recent activity.
            * **Expiration:** Statuses automatically expire after **{EXPIRATION_DAYS} days**. Expired records require renewal.
            * **Data Sources:** Live data (Persona Inquiries/Cases), Typeform submissions (linking emails to grants), Legacy CSVs (GitHub).
            * **Mismatch Detection:** The "Mismatches" tab identifies records that exist in one data source but not others.
            * **Search:** Use the sidebar to look up by Individual (email, name, L2), Business (email, name, L2), or Grant ID.
            * **Consolidated Views:** The tabs below show filterable tables of all known contributors, businesses, and projects.
            """
        )
        st.markdown("**Status Code Meanings:**")
        st.markdown(f"- **{STATUS_CLEARED}**: Verification approved and current.")
        st.markdown(f"- **{STATUS_REJECTED}**: Verification declined. Cannot proceed.")
        st.markdown(
            f"- **{STATUS_IN_REVIEW}**: Submitted, pending compliance review (allow up to 72 hours)."
        )
        st.markdown(
            f"- **{STATUS_RETRY}**: Individual KYC incomplete (Persona status: `pending`, `created`, `expired`). Needs user action at [kyc.optimism.io](https://kyc.optimism.io/)."
        )
        st.markdown(
            f"- **{STATUS_INCOMPLETE}**: Business KYB incomplete (Persona Case status: `Waiting on UBOs`, `pending`, `created`, `expired`). Check associated emails for actions needed from controllers."
        )
        st.markdown(
            f"- **{STATUS_EXPIRED}**: Previously cleared, but older than {EXPIRATION_DAYS} days. Needs renewal."
        )
        st.markdown(
            f"- **{STATUS_NOT_STARTED}**: No verification record found, or process not initiated in Persona yet."
        )
        st.markdown(
            f"- **{STATUS_NO_FORM}**: Grant ID found (likely legacy), but no corresponding Typeform submission linking emails was found."
        )
        st.markdown(
            f"- **{STATUS_UNKNOWN}**: An unexpected status was encountered from an API or during processing."
        )

    # --- UI Interaction ---
    st.sidebar.header("🔍 Database Lookup")
    option = st.sidebar.selectbox(
        "Search By",
        ["Individual (KYC)", "Business (KYB)", "Grant ID"],
        key="search_option",
        help="Select the type of entity you want to search for.",
    )

    # Simple search input without triggering reruns
    search_term_to_use = st.sidebar.text_input(
        "Enter Search Term",
        key="search_term_widget_key",
        help="Enter Email, Name, L2 Address, or Grant ID. Search updates automatically.",
    ).strip()

    # --- Clear Cache Button ---
    if st.sidebar.button(
        "🔄 Clear Cache & Refresh Data", help="Clears cached data and re-fetches everything."
    ):
        st.cache_data.clear()
        # Clear session state related to loaded data
        keys_to_clear = [
            SESSION_KEY_PERSONA_INQUIRIES,
            SESSION_KEY_PERSONA_CASES,
            SESSION_KEY_DATA_LOADED,
            SESSION_KEY_APPLIED_FILTERS,
            "show_final_status",
        ]
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        st.success("Cache and session data cleared. Refreshing...")
        status_indicator.info("⏳ Refreshing data after cache clear...")
        st.rerun()

    # --- Display Search Results ---
    st.divider()
    st.header("🔎 Search Results")
    search_triggered = bool(search_term_to_use)
    if search_triggered:
        st.markdown(f"Searching **{option}** for: `{search_term_to_use}`")
        # No spinner needed here as data is already loaded (from session/cache)
        # with st.spinner("Searching..."):
        results_df = pd.DataFrame()
        if option == "Individual (KYC)":
            search_fields = ["name", "email", "l2_address"]
            results_df = perform_search(all_contributors_view, search_term_to_use, search_fields)
        elif option == "Business (KYB)":
            search_fields = ["business_name", "email", "l2_address"]
            results_df = perform_search(all_businesses_df, search_term_to_use, search_fields)
        elif option == "Grant ID":
            search_fields_text = ["grant_id", "project_name", "l2_address"]
            search_fields_list = ["kyc_emails", "kyb_emails"]
            term_lower = search_term_to_use.lower()
            if all_projects_view is not None and not all_projects_view.empty:
                mask = pd.Series(False, index=all_projects_view.index)
                for field in search_fields_text:
                    if field in all_projects_view.columns:
                        mask |= (
                            all_projects_view[field]
                            .fillna("")
                            .astype(str)
                            .str.lower()
                            .str.contains(term_lower, na=False, regex=False)
                        )
                for email_list_col in search_fields_list:
                    if email_list_col in all_projects_view.columns:

                        def check_email_list(emails):
                            if isinstance(emails, list):
                                return any(
                                    term_lower in email.lower()
                                    for email in emails
                                    if isinstance(email, str)
                                )
                            return False

                        mask |= all_projects_view[email_list_col].apply(check_email_list)
                results_df = all_projects_view[mask]
            else:
                results_df = pd.DataFrame()
        # Display results
        display_search_results(results_df, option, all_persons_df, all_businesses_df)
    else:
        st.info("Use the sidebar to search for an individual, business, or grant.")

    # --- Display Filterable Tables ---
    st.divider()
    st.header("📊 Consolidated Views")

    # --- Filtering Widgets (Wrapped in Form) ---
    st.sidebar.header("📊 Filter Views")

    # Initialize session state for filters if not already present
    if SESSION_KEY_APPLIED_FILTERS not in st.session_state:
        st.session_state[SESSION_KEY_APPLIED_FILTERS] = {"statuses": [], "rounds": []}

    # Get available options for filters
    combined_statuses = pd.concat(
        [
            all_contributors_view["status"].dropna()
            if all_contributors_view is not None and "status" in all_contributors_view
            else pd.Series(),
            all_businesses_df["status"].dropna()
            if all_businesses_df is not None and "status" in all_businesses_df
            else pd.Series(),
            all_projects_view["overall_status"].dropna()
            if all_projects_view is not None and "overall_status" in all_projects_view
            else pd.Series(),
        ]
    ).unique()
    available_statuses = sorted([s for s in combined_statuses if pd.notna(s)])

    available_rounds = []
    if all_projects_view is not None and "round_id" in all_projects_view:
        available_rounds = sorted(list(all_projects_view["round_id"].dropna().unique()))

    # Set defaults for the widgets based on currently applied filters or all options if none applied yet
    default_statuses = (
        st.session_state[SESSION_KEY_APPLIED_FILTERS]["statuses"]
        if st.session_state[SESSION_KEY_APPLIED_FILTERS]["statuses"]
        else available_statuses
    )
    default_rounds = (
        st.session_state[SESSION_KEY_APPLIED_FILTERS]["rounds"]
        if st.session_state[SESSION_KEY_APPLIED_FILTERS]["rounds"]
        else available_rounds
    )

    # Create a form for the filters
    with st.sidebar.form("filter_form"):
        st.markdown("Select filters and click Apply.")
        selected_statuses_widget = st.multiselect(
            "Filter by Status",
            available_statuses,
            default=default_statuses,
            key="filter_widget_status",
        )
        selected_rounds_widget = st.multiselect(
            "Filter by Round ID (Projects)",
            available_rounds,
            default=default_rounds,
            key="filter_widget_round",
        )
        submitted = st.form_submit_button("Apply Filters")
        if submitted:
            st.session_state[SESSION_KEY_APPLIED_FILTERS]["statuses"] = selected_statuses_widget
            st.session_state[SESSION_KEY_APPLIED_FILTERS]["rounds"] = selected_rounds_widget
            st.rerun()  # Rerun to apply the filters

    # --- Apply Filters and Display Tabs ---
    applied_statuses = st.session_state[SESSION_KEY_APPLIED_FILTERS]["statuses"]
    applied_rounds = st.session_state[SESSION_KEY_APPLIED_FILTERS]["rounds"]

    # Define base column config
    table_column_config = {
        "updated_at": st.column_config.DatetimeColumn(
            "Last Updated", format="YYYY-MM-DD HH:mm", width="small"
        ),
        "submitted_at": st.column_config.DatetimeColumn(
            "Form Submitted", format="YYYY-MM-DD HH:mm", width="small"
        ),
        "l2_address": st.column_config.TextColumn("L2 Address", width="medium"),
        "op_amt": st.column_config.NumberColumn("OP Amount", format="%.2f", width="small"),
        "email": st.column_config.TextColumn("Email", width="medium"),
        "name": st.column_config.TextColumn("Individual Name", width="medium"),
        "business_name": st.column_config.TextColumn("Business Name", width="medium"),
        "project_name": st.column_config.TextColumn("Project Name", width="medium"),
        "round_id": st.column_config.TextColumn("Round ID", width="small"),
        "status": st.column_config.TextColumn("KYC/KYB Status", width="medium"),
        "overall_status": st.column_config.TextColumn("Grant Status", width="medium"),
        "inquiry_id": None,
        "case_id": None,
        "form_id": None,
        "project_id": None,
        "source": None,
        "kyc_emails": None,
        "kyb_emails": None,
    }

    tab1, tab2, tab3, tab4 = st.tabs(
        ["👤 Contributors (KYC)", "🏢 Businesses (KYB)", "📄 Projects (Grants)", "⚠️ Mismatches"]
    )

    with tab1:
        st.subheader("All Contributors (Latest KYC Status)")
        df_to_display = all_contributors_view
        if df_to_display is not None and not df_to_display.empty:
            # Apply filter only if filter list is not empty (i.e., user selected specific statuses)
            if applied_statuses and "status" in df_to_display.columns:
                filtered_contributors = df_to_display[
                    df_to_display["status"].isin(applied_statuses)
                ]
            else:
                filtered_contributors = (
                    df_to_display  # Show all if no specific statuses are selected
                )

            contributor_cols = [
                "name",
                "email",
                "l2_address",
                "status",
                "updated_at",
                "project_name",
                "round_id",
                "op_amt",
            ]
            display_df = filtered_contributors[
                [col for col in contributor_cols if col in filtered_contributors.columns]
            ]
            st.dataframe(
                display_df,
                use_container_width=True,
                column_config=table_column_config,
                hide_index=True,
            )
            st.caption(
                f"Displaying {len(filtered_contributors)} of {len(df_to_display)} contributors based on applied filters."
            )
        else:
            st.warning("No contributor data available.")

    with tab2:
        st.subheader("All Businesses (Latest KYB Status)")
        df_to_display = all_businesses_df
        if df_to_display is not None and not df_to_display.empty:
            if applied_statuses and "status" in df_to_display.columns:
                filtered_businesses = df_to_display[df_to_display["status"].isin(applied_statuses)]
            else:
                filtered_businesses = df_to_display

            business_cols = ["business_name", "email", "l2_address", "status", "updated_at"]
            display_df = filtered_businesses[
                [col for col in business_cols if col in filtered_businesses.columns]
            ]
            st.dataframe(
                display_df,
                use_container_width=True,
                column_config=table_column_config,
                hide_index=True,
            )
            st.caption(
                f"Displaying {len(filtered_businesses)} of {len(df_to_display)} businesses based on applied filters."
            )
        else:
            st.warning("No business data available.")

    with tab3:
        st.subheader("All Projects (Overall Grant Status)")
        df_to_display = all_projects_view
        if df_to_display is not None and not df_to_display.empty:
            filtered_projects = df_to_display.copy()
            if applied_statuses and "overall_status" in filtered_projects.columns:
                filtered_projects = filtered_projects[
                    filtered_projects["overall_status"].isin(applied_statuses)
                ]
            if applied_rounds and "round_id" in filtered_projects.columns:
                filtered_projects = filtered_projects[
                    filtered_projects["round_id"].isin(applied_rounds)
                ]

            project_cols = [
                "grant_id",
                "project_name",
                "round_id",
                "overall_status",
                "l2_address",
                "submitted_at",
            ]
            display_df = filtered_projects[
                [col for col in project_cols if col in filtered_projects.columns]
            ]
            st.dataframe(
                display_df,
                use_container_width=True,
                column_config=table_column_config,
                hide_index=True,
            )
            st.caption(
                f"Displaying {len(filtered_projects)} of {len(df_to_display)} projects based on applied filters."
            )
        else:
            st.warning("No project data available.")

    with tab4:
        st.subheader("Data Mismatches Between Providers")
        st.info(
            "This tab shows records that exist in one data source but not in others, helping identify data inconsistencies."
        )

        if mismatches_data:
            # Create expandable sections for each mismatch type
            mismatch_configs = {
                "typeform_without_persona": {
                    "title": "📝 Typeform Submissions Missing in Persona",
                    "description": "Grant submissions that have KYC/KYB emails but no corresponding Persona verification records.",
                    "columns": [
                        "grant_id",
                        "project_id",
                        "missing_emails",
                        "email_type",
                        "submitted_at",
                        "l2_address",
                    ],
                },
                "persona_without_typeform": {
                    "title": "🔍 Persona Records Missing in Typeform",
                    "description": "Verification records in Persona that don't link to any grant submissions.",
                    "columns": [
                        "email",
                        "name",
                        "business_name",
                        "record_type",
                        "status",
                        "updated_at",
                        "l2_address",
                    ],
                },
                "legacy_without_live": {
                    "title": "📜 Legacy Contributors Missing Live Records",
                    "description": "Historical contributors who don't have current Persona verification records.",
                    "columns": [
                        "email",
                        "name",
                        "project_name",
                        "round_id",
                        "op_amt",
                        "updated_at",
                        "l2_address",
                    ],
                },
                "projects_without_typeform": {
                    "title": "🏗️ Legacy Projects Missing Typeform Submissions",
                    "description": "Projects from legacy data that don't have corresponding grant submission forms.",
                    "columns": ["grant_id", "project_name", "round_id", "updated_at"],
                },
            }

            total_mismatches = sum(len(df) for df in mismatches_data.values())
            st.metric("Total Mismatched Records", total_mismatches)

            for mismatch_key, config in mismatch_configs.items():
                if mismatch_key in mismatches_data and not mismatches_data[mismatch_key].empty:
                    df = mismatches_data[mismatch_key]
                    with st.expander(f"{config['title']} ({len(df)} records)", expanded=False):
                        st.markdown(f"**Description:** {config['description']}")

                        # Display available columns
                        available_cols = [col for col in config["columns"] if col in df.columns]
                        if available_cols:
                            display_df = df[available_cols].copy()

                            # Apply column config
                            mismatch_column_config = {
                                "updated_at": st.column_config.DatetimeColumn(
                                    "Last Updated", format="YYYY-MM-DD HH:mm"
                                ),
                                "submitted_at": st.column_config.DatetimeColumn(
                                    "Form Submitted", format="YYYY-MM-DD HH:mm"
                                ),
                                "l2_address": st.column_config.TextColumn(
                                    "L2 Address", width="medium"
                                ),
                                "op_amt": st.column_config.NumberColumn("OP Amount", format="%.2f"),
                                "email": st.column_config.TextColumn("Email", width="medium"),
                                "missing_emails": st.column_config.TextColumn(
                                    "Missing Emails", width="large"
                                ),
                                "mismatch_type": None,  # Hide this column as it's shown in the title
                            }

                            st.dataframe(
                                display_df,
                                use_container_width=True,
                                column_config=mismatch_column_config,
                                hide_index=True,
                            )
                        else:
                            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.success(
                "🎉 No data mismatches detected! All records are properly linked across data providers."
            )
            st.info("This means:")
            st.markdown("- All Typeform submissions have corresponding Persona records")
            st.markdown("- All Persona records link to grant submissions")
            st.markdown("- Legacy data aligns with live verification records")
            st.markdown("- All projects have proper form submissions")


# --- Entry Point ---
if __name__ == "__main__":
    main()
