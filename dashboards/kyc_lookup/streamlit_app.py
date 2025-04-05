import streamlit as st
import pandas as pd
import numpy as np
import requests
from io import StringIO
from datetime import datetime, timedelta, timezone


# Email and password verification functionality
def check_credentials():
    """Returns `True` if the user has a valid email and the correct password."""

    def login_form():
        with st.form("credentials"):
            st.text_input("Email", key="email")
            st.text_input("Password", type="password", key="password")
            submitted = st.form_submit_button("Login")
            if submitted:
                return validate_credentials()
        return False

    def validate_credentials():
        if "email" in st.session_state and "password" in st.session_state:
            # Get the user's email and password from session state
            email = st.session_state["email"].lower()
            password = st.session_state["password"]

            # Check if the email is in the allowed list and password is correct
            allowed_emails = st.secrets.get("allowed_emails", [])
            correct_password = st.secrets.get("password", "")

            if email in [e.lower() for e in allowed_emails] and password == correct_password:
                st.session_state["authenticated"] = True
                return True

            st.error("Invalid email or password")
        return False

    # Return True if the user is authenticated
    if st.session_state.get("authenticated", False):
        return True

    # Show login form if not authenticated
    return login_form()


# Check authentication before showing the app content
if not check_credentials():
    st.stop()

# Continue with the rest of the app
st.set_page_config(page_title="KYC Lookup Tool", page_icon="🗝️")
st.title("🗝️ KYC Lookup Tool")

st.subheader("Project Status")
with st.expander("About the Results"):
    st.markdown(
        "**Every project must complete KYC (or KYB for businesses) in order to receive tokens or join the Superchain.**"
    )
    st.info(
        "This tool can be used to lookup project status for a specific grant round or workflow. If you do not see the expected grants round here, or you see other unexpected results, please reach out to the Grant Program Manager to correct this issue."
    )
    st.markdown('**What should I do if a project I\'m talking to is not in *"cleared"* status?**')
    st.warning(
        '🌕 *"retry"* means that the individual will need to re-attempt their KYC. They did not submit all documents, and should start over at kyc.optimism.io/  \n  \n 🔵 *"incomplete"* means we are waiting for 1+ business controllers to finish uploading their documents. Please direct them to check their emails.  \n  \n 🟠  *"in review"* means that this team or individual is waiting on a compliance review. Please let them know it may be up to 72 hours before a final decision is reached.    \n  \n 🛑 *"rejected"* teams will not be able to move forward with us. We cannot deliver tokens, and any signed agreements may be null and void. Reach out to compliance@optimism.io if you have any questions or suspect this decision may have been reached in error.'
    )

## PERSONA-------------------------------------------------------------------


@st.cache_data(ttl=600)
def fetch_data(api_key, base_url):
    results = []
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"page[size]": 100}
    next_page_after = None

    while True:
        response = requests.get(base_url, headers=headers, params=params)
        response_data = response.json()
        # print(response_data)
        results.extend(response_data.get("data", []))
        if "data" in response_data:
            filtered_inquiries = [
                inquiry
                for inquiry in response_data["data"]
                if inquiry["attributes"]["status"] not in ["created", "open"]
            ]
            results.extend(filtered_inquiries)
        next_link = response_data.get("links", {}).get("next")
        if next_link:
            next_cursor = next_link.split("page%5Bafter%5D=")[-1].split("&")[0]
            params["page[after]"] = next_cursor
            print(next_cursor)
        else:
            break

    return results


def process_inquiries(results):
    records = []
    for item in results:
        inquiry_id = item["id"]
        attributes = item.get("attributes", {})
        name_first = attributes.get("name-first", "") or ""
        name_middle = attributes.get("name-middle", "") or ""
        name_last = attributes.get("name-last", "") or ""
        name = f"{name_first} {name_middle} {name_last}".strip()
        email = attributes.get("email-address", "") or ""
        email = email.lower().strip()
        updated_at = attributes.get("updated-at")
        status = attributes.get("status")
        l2_address = attributes.get("fields", {}).get("l-2-address", {}).get("value", np.nan)

        if pd.notna(l2_address) and l2_address.lower().strip().startswith("0x"):
            l2_address = l2_address.lower().strip()
        else:
            l2_address = np.nan

        if "@" not in email:
            email = ""

        if status == "approved":
            status = "🟢 cleared"
        if status in ["expired", "pending", "created"]:
            status = "🌕 retry"
        if status == "declined":
            status = "🛑 rejected"
        if status == "needs_review":
            status = "🟠 in review"

        records.append(
            {
                "inquiry_id": inquiry_id,
                "name": name,
                "email": email,
                "l2_address": l2_address,
                "updated_at": updated_at,
                "status": status,
            }
        )

    return pd.DataFrame(records)


def process_cases(results):
    records = []
    for item in results:
        case_id = item["id"]
        inquiries = item.get("relationships", {}).get("inquiries", {}).get("data", [])
        inquiry_id = inquiries[0]["id"] if inquiries else np.nan
        attributes = item.get("attributes", {})
        status = attributes.get("status")
        fields = attributes.get("fields", {})
        business_name = fields.get("business-name", {}).get("value", "")
        email = fields.get("form-filler-email-address", {}).get("value", np.nan)
        email = str(email).lower().strip() if pd.notna(email) else ""
        updated_at = attributes.get("updated-at")
        l2_address = fields.get("l-2-address", {}).get("value", np.nan)

        if pd.notna(l2_address) and l2_address.lower().strip().startswith("0x"):
            l2_address = l2_address.lower().strip()
        else:
            l2_address = np.nan

        if status == "Approved":
            status = "🟢 cleared"
        if status in ["expired", "pending", "created", "Waiting on UBOs"]:
            status = "🔵 incomplete"
        if status == "Declined":
            status = "🛑 rejected"
        if status in ["Ready for Review"]:
            status = "🟠 in review"

        if business_name:
            records.append(
                {
                    "case_id": case_id,
                    "business_name": business_name,
                    "email": email,
                    "l2_address": l2_address,
                    "updated_at": updated_at,
                    "status": status,
                }
            )

    return pd.DataFrame(records)


@st.cache_data(ttl=600)
def tf_fetch(typeform_key, url):
    all_items = []
    page_size = 1000
    after = None

    while True:
        paginated_url = f"{url}?page_size={page_size}"
        if after:
            paginated_url += f"&after={after}"

        response = requests.get(paginated_url, headers={"Authorization": f"Bearer {typeform_key}"})

        if response.status_code != 200:
            raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

        data = response.json()
        items = data.get("items", [])
        all_items.extend(items)
        after = data.get("page", {}).get("after")
        if not after:
            break
    return {"items": all_items}


def typeform_to_dataframe(response_data, existing_data=None):
    if isinstance(response_data, dict):
        items = response_data.get("items", [])
    else:
        raise ValueError("Unexpected response_data format")

    form_entries = []

    for item in items:
        grant_id = item.get("hidden", {}).get("grant_id", np.nan)
        updated_at = item.get("submitted_at", np.nan)

        if pd.isna(grant_id):
            continue

        entry = {
            "form_id": item.get("response_id", np.nan),
            "project_id": item.get("hidden", {}).get("project_id", np.nan),
            "grant_id": grant_id,
            "l2_address": item.get("hidden", {}).get("l2_address", np.nan),
            "updated_at": updated_at,
        }

        kyc_emails = []
        kyb_emails = []
        found_kyb_field = False
        number_of_kyb_emails = 0

        for answer in item.get("answers", []):
            field_type = answer.get("field", {}).get("type")
            field_id = answer.get("field", {}).get("id")

            if field_type == "email":
                email = answer.get("email")
                email = email.lower().strip()

                if found_kyb_field:
                    kyb_emails.append(email)
                else:
                    kyc_emails.append(email)

            if field_type == "number" and field_id == "v8dfrNJiIQaZ":
                number_of_kyb_emails = answer.get("number", 0)
                found_kyb_field = True

        kyb_emails = kyb_emails[:number_of_kyb_emails]

        for i in range(10):
            entry[f"kyc_email{i}"] = kyc_emails[i] if i < len(kyc_emails) else np.nan

        for i in range(5):
            entry[f"kyb_email{i}"] = kyb_emails[i] if i < len(kyb_emails) else np.nan

        form_entries.append(entry)

    new_df = pd.DataFrame(form_entries)

    if existing_data is not None:
        new_entries = new_df[~new_df["form_id"].isin(existing_data["form_id"])]
        updated_df = pd.concat([existing_data, new_entries], ignore_index=True)
    else:
        updated_df = new_df

    return updated_df if not updated_df.empty else None


## LEGACY DATA -------------------------------------------------------------------


def fetch_csv(owner, repo, path, access_token):
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    headers = {"Authorization": f"token {access_token}", "Accept": "application/vnd.github.v3.raw"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        csv_content = response.content.decode("utf-8")
        df = pd.read_csv(StringIO(csv_content))
        return df
    else:
        st.error(f"Failed to fetch the file from {path}: {response.status_code}")
        return None


def main():
    ##st.title('KYC Database')

    api_key = st.secrets["persona"]["api_key"]
    typeform_key = st.secrets["typeform"]["typeform_key"]

    access_token = st.secrets["github"]["access_token"]
    owner = "dioptx"
    repo = "the-trojans"

    contributors_path = "grants.contributors.csv"
    projects_path = "grants.projects.csv"
    persons_path = "legacy.persons.csv"
    businesses_path = "legacy.businesses.csv"
    form_path = "legacy.form.csv"

    contributors_df = fetch_csv(owner, repo, contributors_path, access_token)
    projects_df = fetch_csv(owner, repo, projects_path, access_token)
    persons_df = fetch_csv(owner, repo, persons_path, access_token)
    businesses_df = fetch_csv(owner, repo, businesses_path, access_token)
    # --------------------------------------------------------------------------

    if "inquiries_data" not in st.session_state:
        st.session_state.inquiries_data = None
    if "cases_data" not in st.session_state:
        st.session_state.cases_data = None
    if "typeform_data" not in st.session_state:
        st.session_state.typeform_data = None

    refresh_button = st.button("Refresh")

    if refresh_button:
        inquiries_data = fetch_data(
            api_key, "https://app.withpersona.com/api/v1/inquiries?refresh=true"
        )
        cases_data = fetch_data(api_key, "https://app.withpersona.com/api/v1/cases?refresh=true")
        form_entries = tf_fetch(typeform_key, "https://api.typeform.com/forms/KoPTjofd/responses")
        typeform_data = typeform_to_dataframe(form_entries)
        st.session_state.inquiries_data = inquiries_data
        st.session_state.cases_data = cases_data
        st.session_state.typeform_data = typeform_data
    else:
        if st.session_state.inquiries_data is None:
            inquiries_data = fetch_data(api_key, "https://app.withpersona.com/api/v1/inquiries")
            st.session_state.inquiries_data = inquiries_data
        else:
            inquiries_data = st.session_state.inquiries_data

        if st.session_state.cases_data is None:
            cases_data = fetch_data(api_key, "https://app.withpersona.com/api/v1/cases")
            st.session_state.cases_data = cases_data
        else:
            cases_data = st.session_state.cases_data

        if st.session_state.typeform_data is None:
            form_entries = tf_fetch(
                typeform_key, "https://api.typeform.com/forms/KoPTjofd/responses"
            )
            if form_entries is None:
                st.error("Failed to fetch Typeform data.")
            else:
                typeform_data = typeform_to_dataframe(form_entries)
                if typeform_data is not None and not typeform_data.empty:
                    st.session_state.typeform_data = typeform_data
                else:
                    st.error("No entries returned from Typeform.")
        else:
            typeform_data = st.session_state.typeform_data

    st.sidebar.header("Database Lookup")
    option = st.sidebar.selectbox("Project Type", ["Superchain", "Vendor", "Contribution Path"])
    search_term = st.sidebar.text_input("Enter search term (name, l2_address, or email)")
    st.sidebar.header("Search by Grant")
    grant_id_input = st.sidebar.text_input("Enter Grant ID").strip()

    inquiries_df = process_inquiries(inquiries_data)
    cases_df = process_cases(cases_data)

    contributors_path = "grants.contributors.csv"
    projects_path = "grants.projects.csv"
    persons_path = "legacy.persons.csv"
    businesses_path = "legacy.businesses.csv"
    form_path = "legacy.form.csv"

    contributors_df = fetch_csv(owner, repo, contributors_path, access_token)
    projects_df = fetch_csv(owner, repo, projects_path, access_token)
    persons_df = fetch_csv(owner, repo, persons_path, access_token)
    businesses_df = fetch_csv(owner, repo, businesses_path, access_token)

    if persons_df is not None and "updated_at" in persons_df.columns:
        try:
            persons_df["updated_at"] = pd.to_datetime(persons_df["updated_at"], utc=True)
        except Exception as e:
            st.error(f"Error converting 'updated_at' to datetime: {e}")
            st.stop()

    if businesses_df is not None and "updated_at" in businesses_df.columns:
        try:
            businesses_df["updated_at"] = pd.to_datetime(businesses_df["updated_at"], utc=True)
        except Exception as e:
            st.error(f"Error converting 'updated_at' to datetime: {e}")
            st.stop()

    if inquiries_df is not None and "updated_at" in inquiries_df.columns:
        inquiries_df["updated_at"] = pd.to_datetime(inquiries_df["updated_at"], utc=True)

    if cases_df is not None and "updated_at" in cases_df.columns:
        cases_df["updated_at"] = pd.to_datetime(cases_df["updated_at"], utc=True)

    if persons_df is not None and inquiries_df is not None:
        current_date_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
        one_year_ago_utc = current_date_utc - timedelta(days=365)

    if businesses_df is not None and cases_df is not None:
        current_date_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
        one_year_ago_utc = current_date_utc - timedelta(days=365)

    def display_results(df, columns, message, status_column="status", date_column="updated_at"):
        if df.empty:
            st.write("No matching results found.")
            return
        st.write(df[columns])

        if date_column in df.columns and not df[date_column].isnull().all():
            most_recent_status = df.loc[df[date_column].idxmax(), status_column]
            st.write(f"### {message.format(status=most_recent_status)}")
        else:
            empty_row = {col: "" for col in columns}
            empty_row[date_column] = ""
            empty_row[status_column] = "not started"
            df = pd.DataFrame([empty_row])
            st.write(f"### {message.format(status='not clear')}")

    def search_and_display(
        df,
        search_term,
        columns_to_display,
        message,
        status_column="status",
        date_column="updated_at",
    ):
        if not search_term.strip():
            display_results(
                pd.DataFrame(columns=columns_to_display), columns_to_display, message, status_column
            )
            return
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
        df["status"] = df["status"].fillna("not started")
        grant_id_search = (
            df.get("grant_id", pd.Series([""] * len(df)))
            .astype(str)
            .str.contains(search_term, case=False, na=False)
        )
        name_search = df.get("name", pd.Series([""] * len(df))).str.contains(
            search_term, case=False, na=False
        )
        business_name_search = df.get("business_name", pd.Series([""] * len(df))).str.contains(
            search_term, case=False, na=False
        )
        email_search = df["email"].str.contains(search_term, case=False, na=False)
        l2_address_search = df["l2_address"].str.contains(search_term, case=False, na=False)
        filtered_df = df[name_search | business_name_search | email_search | l2_address_search]
        if not filtered_df.empty:
            display_results(filtered_df, columns_to_display, message, status_column)

    all_persons_df = pd.concat([persons_df, inquiries_df], ignore_index=True)
    all_persons_df["status"] = (
        all_persons_df.sort_values("updated_at").groupby("email")["status"].transform("last")
    )
    all_persons_df["l2_address"] = (
        all_persons_df.sort_values("updated_at").groupby("email")["l2_address"].transform("last")
    )
    all_persons_df["updated_at"] = (
        all_persons_df.sort_values("updated_at").groupby("email")["updated_at"].transform("last")
    )
    all_persons_df["name"] = (
        all_persons_df.sort_values("updated_at").groupby("email")["name"].transform("last")
    )
    all_persons_df.loc[
        (all_persons_df["status"] == "cleared") & (all_persons_df["updated_at"] < one_year_ago_utc),
        "status",
    ] = "expired"
    all_contributors = contributors_df.merge(
        all_persons_df[["email", "name", "status", "l2_address", "updated_at"]],
        on="email",
        how="outer",
    )
    all_contributors["status"] = all_contributors["status"].fillna("not started")
    all_contributors["l2_address"] = all_contributors["l2_address_x"].combine_first(
        all_contributors["l2_address_y"]
    )
    all_contributors["l2_address"] = all_contributors.apply(
        lambda row: row["l2_address_x"] if pd.notna(row["l2_address_x"]) else row["l2_address_y"],
        axis=1,
    )
    all_contributors = all_contributors.drop(columns=["l2_address_x", "l2_address_y"])
    all_contributors = all_contributors[
        ~(all_contributors["email"].isnull() & all_contributors["avatar"].isnull())
    ]
    all_contributors.drop_duplicates(subset=["email", "round_id", "op_amt"], inplace=True)

    all_businesses = pd.concat([businesses_df, cases_df], ignore_index=True)
    all_businesses = all_businesses.sort_values("updated_at")
    all_businesses["status"] = all_businesses.groupby(["email", "business_name"])[
        "status"
    ].transform("last")
    all_businesses["l2_address"] = all_businesses.groupby(["email", "business_name"])[
        "l2_address"
    ].transform("last")
    all_businesses["updated_at"] = all_businesses.groupby(["email", "business_name"])[
        "updated_at"
    ].transform("last")
    all_businesses.loc[
        (all_businesses["status"] == "cleared") & (all_businesses["updated_at"] < one_year_ago_utc),
        "status",
    ] = "expired"
    all_businesses = all_businesses[~(all_businesses["email"].isnull())]
    all_businesses.drop_duplicates(subset=["email", "business_name"], inplace=True)

    # def generate_all_projects(typeform_data, all_contributors, all_businesses):
    #     typeform_data['updated_at'] = pd.to_datetime(typeform_data['updated_at'], errors='coerce')
    #     typeform_data_sorted = typeform_data.sort_values(by='updated_at', ascending=False)
    #     typeform_data_unique = typeform_data_sorted.drop_duplicates(subset='grant_id', keep='first')
    #     all_projects_rows = []
    #     for index, row in typeform_data.iterrows():
    #         grant_id = row['grant_id']
    #         for i in range(10):
    #             kyc_email = row.get(f'kyc_email{i}', np.nan)
    #             if pd.notna(kyc_email):
    #                 contributor_row = all_contributors.loc[all_contributors['email'] == kyc_email]
    #                 status = contributor_row['status'].max() if not contributor_row.empty else 'not started'
    #                 all_projects_rows.append({
    #                     'grant_id': grant_id,
    #                     'email': kyc_email,
    #                     'status': status,
    #                     'type': 'kyc'
    #                 })
    #         for i in range(5):
    #             kyb_email = row.get(f'kyb_email{i}', np.nan)
    #             if pd.notna(kyb_email):
    #                 business_row = all_businesses.loc[all_businesses['email'] == kyb_email]
    #                 status = business_row['status'].max() if not business_row.empty else 'not started'
    #                 all_projects_rows.append({
    #                     'grant_id': grant_id,
    #                     'email': kyb_email,
    #                     'status': status,
    #                     'type': 'kyb'
    #                 })
    #     all_projects_df = pd.DataFrame(all_projects_rows)
    #     return all_projects_df
    # all_projects = generate_all_projects(typeform_data, all_contributors, all_businesses)

    typeform_data["grant_id"] = typeform_data["grant_id"].astype(str)
    projects_df["grant_id"] = projects_df["grant_id"].astype(str)

    typeform_data = typeform_data.sort_values(by="updated_at").drop_duplicates(
        subset="grant_id", keep="last"
    )
    projects_df = projects_df.drop_duplicates(subset="grant_id", keep="last")

    all_projects = pd.merge(
        typeform_data, projects_df, on="grant_id", how="outer", suffixes=("_typeform", "_project")
    )
    all_projects["l2_address"] = all_projects["l2_address_typeform"].combine_first(
        all_projects["l2_address_project"]
    )
    all_projects["project_id"] = all_projects["project_id_typeform"].combine_first(
        all_projects["project_id_project"]
    )
    all_projects = all_projects.drop(
        columns=[
            "l2_address_typeform",
            "l2_address_project",
            "project_id_typeform",
            "project_id_project",
        ]
    )
    all_projects["updated_at"] = pd.to_datetime(all_projects["updated_at"], errors="coerce")
    all_projects = all_projects.sort_values(by=["grant_id", "updated_at"]).drop_duplicates(
        subset="grant_id", keep="last"
    )

    kyc_emails_dict = {}
    kyb_emails_dict = {}

    for index, row in all_projects.iterrows():
        grant_id = row["grant_id"]

        for i in range(10):
            kyc_email = row.get(f"kyc_email{i}")
            if pd.notna(kyc_email):
                if grant_id not in kyc_emails_dict:
                    kyc_emails_dict[grant_id] = set()
                kyc_emails_dict[grant_id].add(kyc_email)

        for i in range(5):
            kyb_email = row.get(f"kyb_email{i}")
            if pd.notna(kyb_email):
                if grant_id not in kyb_emails_dict:
                    kyb_emails_dict[grant_id] = set()
                kyb_emails_dict[grant_id].add(kyb_email)

        kyc_emails = {grant_id: list(emails) for grant_id, emails in kyc_emails_dict.items()}
        kyb_emails = {grant_id: list(emails) for grant_id, emails in kyb_emails_dict.items()}

    kyc_results = []
    for grant_id, emails in kyc_emails_dict.items():
        for email in emails:
            status = all_contributors.loc[all_contributors["email"] == email, "status"].values
            kyc_results.append(
                {
                    "email": email,
                    "grant_id": grant_id,
                    "status": status[0] if status.size > 0 else "not started",
                }
            )
    kyc_df = pd.DataFrame(kyc_results)
    kyc_df["grant_id"] = kyc_df["grant_id"].astype(str)

    kyb_results = []
    for grant_id, emails in kyb_emails_dict.items():
        for email in emails:
            status = all_businesses.loc[all_businesses["email"] == email, "status"].values
            kyb_results.append(
                {
                    "email": email,
                    "grant_id": grant_id,
                    "status": status[0] if status.size > 0 else "not started",
                }
            )
    kyb_df = pd.DataFrame(kyb_results)
    kyb_df["grant_id"] = kyb_df["grant_id"].astype(str)

    if option in ["Superchain", "Vendor"]:
        if search_term:
            st.title("KYB Status")
        search_and_display(
            all_businesses,
            search_term,
            ["business_name", "email", "l2_address", "updated_at", "status"],
            "This team is {status} for KYB.",
        )
    elif option == "Contribution Path":
        if search_term:
            st.title("KYC Status")
        if "avatar" not in all_contributors.columns:
            all_contributors["avatar"] = ""
        if search_term:
            search_and_display(
                all_contributors,
                search_term,
                ["avatar", "email", "l2_address", "updated_at", "status"],
                "This contributor is {status} for KYC.",
            )
    # elif option == 'Grants Round':

    if grant_id_input:
        st.title("Grant Status")
        grant_id_input = str(grant_id_input)

        kyc_matches = kyc_df[kyc_df["grant_id"] == grant_id_input]
        kyb_matches = kyb_df[kyb_df["grant_id"] == grant_id_input]

        if kyc_matches.empty and kyb_matches.empty:
            st.write(
                "No form on file. Please ask the user to complete the KYC Form using the unique link sent in their award email."
            )
        else:
            if not kyc_matches.empty:
                st.write("KYC Results:")
                st.write(kyc_matches)

            if not kyb_matches.empty:
                st.write("KYB Results:")
                st.write(kyb_matches)

    #     overall_status = 'not started'
    #     if not kyc_df.empty or not kyb_df.empty:
    #         all_kyc_statuses = kyc_df['status'].values
    #         all_kyb_statuses = kyb_df['status'].values
    #         if all(status == 'cleared' for status in all_kyc_statuses) and all(status == 'cleared' for status in all_kyb_statuses):
    #             overall_status = 'cleared'
    #         elif any(status == 'rejected' for status in all_kyc_statuses) or any(status == 'rejected' for status in all_kyb_statuses):
    #             overall_status = 'rejected'
    #         else:
    #             overall_status = 'incomplete'

    #     all_projects['status'] = overall_status

    #     if search_term:
    #         search_and_display(all_projects, search_term, ['project_name', 'email', 'l2_address', 'updated_at', 'status'],
    #                            f"{all_projects['project_name'].iloc[0]} is {overall_status} for KYC.")

    #         if not kyc_df.empty:
    #             st.write("KYC emails")
    #             st.write(kyc_df)

    #         if not kyb_df.empty:
    #             st.write("KYB emails")
    #             st.write(kyb_df)
    #     else:
    #         st.write('*Use the search tool on the left-hand side to input an L2 address, project name, or admin email* 💬')

    # display_results(filtered_df, ['project_name', 'email', 'l2_address', 'round_id', 'grant_id', 'status'],
    # "This project is {status} for KYC.")

    ## TESTING--------------------------------------------------

    # st.write(typeform_data)
    # # st.write('all projects table')
    # st.write(all_projects)
    # st.write(kyc_df)
    # st.write(kyb_df)

    ## Contributors-------------------------------------------------------

    st.header("______________________________")
    st.header("Individual Contributors")

    all_persons_df = pd.concat([persons_df, inquiries_df], ignore_index=True)
    all_persons_df["status"] = (
        all_persons_df.sort_values("updated_at").groupby("email")["status"].transform("last")
    )
    all_persons_df["l2_address"] = (
        all_persons_df.sort_values("updated_at").groupby("email")["l2_address"].transform("last")
    )
    all_persons_df.loc[
        (all_persons_df["status"] == "cleared") & (all_persons_df["updated_at"] < one_year_ago_utc),
        "status",
    ] = "expired"

    merged_df = contributors_df.merge(
        all_persons_df[["email", "status", "l2_address"]], on="email", how="left"
    )
    merged_df["status"] = merged_df["status"].fillna("not started")
    merged_df["l2_address"] = merged_df["l2_address_x"].combine_first(merged_df["l2_address_y"])
    merged_df["l2_address"] = merged_df.apply(
        lambda row: row["l2_address_x"] if pd.notna(row["l2_address_x"]) else row["l2_address_y"],
        axis=1,
    )
    merged_df = merged_df.drop(columns=["l2_address_x", "l2_address_y"])
    merged_df = merged_df[~(merged_df["email"].isnull() & merged_df["avatar"].isnull())]
    merged_df.drop_duplicates(subset=["email", "round_id", "op_amt"], inplace=True)

    projects_list = [
        "Ambassadors",
        "NumbaNERDs",
        "SupportNERDs",
        "Translators",
        "Badgeholders",
        "WLTA",
        "WLTA Judge",
        "Thank Optimism",
    ]
    projects_selection = st.multiselect(
        "Select the Contributor Path", projects_list + ["Other"], projects_list + ["Other"]
    )

    if "Other" in projects_selection:
        filtered_df = merged_df[~merged_df["project_name"].isin(projects_list)]
        if set(projects_selection) - {"Other"}:
            filtered_df = pd.concat(
                [
                    filtered_df,
                    merged_df[merged_df["project_name"].isin(set(projects_selection) - {"Other"})],
                ]
            )
    else:
        filtered_df = (
            merged_df[merged_df["project_name"].isin(projects_selection)]
            if projects_selection
            else merged_df
        )

    st.write(filtered_df)

    ## Grants Rounds--------------------------------------------

    st.header("Active Grants Rounds")

    url = "https://api.github.com/repos/dioptx/the-trojans/contents/grants.projects.csv"

    headers = {
        "Authorization": f"token {st.secrets['github']['access_token']}",
        "Accept": "application/vnd.github.v3.raw",
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        csv_content = response.content.decode("utf-8")
        df = pd.read_csv(StringIO(csv_content))
        rounds_list = df.round_id.unique()
        rounds_selection = st.multiselect(
            "Select the Grant Round",
            list(rounds_list),
            ["rpgf2", "rpgf3", "season5-builders-19", "season5-growth-19"],
        )

        if "Other" in rounds_selection:
            filtered_df = df[
                ~df["round_id"].isin(["rpgf2", "rpgf3", "season5-builders-19", "season5-growth-19"])
            ]
            if set(rounds_selection) - {"Other"}:
                filtered_df = pd.concat(
                    [filtered_df, df[df["round_id"].isin(set(rounds_selection) - {"Other"})]]
                )
        else:
            filtered_df = df[df["round_id"].isin(rounds_selection)] if rounds_selection else df

        st.write(filtered_df)
    else:
        st.error(f"Failed to fetch the file: {response.status_code}")


if __name__ == "__main__":
    main()
