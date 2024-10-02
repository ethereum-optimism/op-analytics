import os
import json
import gspread
import pandas as pd

from op_coreutils.logger import LOGGER
from op_coreutils.path import repo_path

log = LOGGER.get_logger()

_GSHEETS_LOCATIONS = {}

_GSHEETS_JSON_FILE = ".gsheets.json"


def load_locations():
    """Load the Google Sheets locations map.

    Google sheet URLs are loaded from a .gsheets.json file located at the root of the repo.
    This allows us to refer to Google Sheets using simpler names insted of having to pass around
    the full URL.
    """
    global _GSHEETS_LOCATIONS

    if _GSHEETS_LOCATIONS:
        # Only load locations once.
        return

    gsheets_path = repo_path(_GSHEETS_JSON_FILE)

    if not os.path.exists(gsheets_path):
        raise ValueError(f"{_GSHEETS_JSON_FILE} file is not present on the repo")

    with open(gsheets_path, "r") as fobj:
        for row in json.load(fobj):
            _GSHEETS_LOCATIONS[row["name"]] = row


def update_gsheet(location_name: str, worksheet_name: str, dataframe: pd.DataFrame):
    """Write a pandas dadtaframe to a Google Sheet."""
    global _GSHEETS_LOCATIONS

    load_locations()

    sheet = _GSHEETS_LOCATIONS[location_name]

    gc = gspread.service_account(filename=os.environ["GITHUB_ACTIONS_GCP_SERVICE_ACCOUNT"])
    sh = gc.open_by_url(sheet["url"])
    worksheet = sh.worksheet(worksheet_name)
    worksheet.update([dataframe.columns.values.tolist()] + dataframe.values.tolist())
    log.info(f"Wrote {dataframe.shape} cells to {location_name}#{worksheet_name}")
