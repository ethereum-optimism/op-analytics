import json
import os
from unittest.mock import MagicMock

import gspread
import pandas as pd


from op_coreutils.logger import structlog
from op_coreutils.path import repo_path
from op_coreutils.gcpauth import get_credentials

log = structlog.get_logger()

_GSHEETS_CLIENT: gspread.client.Client | None = None
_GSHEETS_LOCATIONS: dict | None = None

_GSHEETS_JSON_FILE = ".gsheets.json"


def init_client():
    """Init client and load the Google Sheets locations map.

    Google sheet URLs are loaded from a .gsheets.json file located at the root of the repo.
    This allows us to refer to Google Sheets using simpler names insted of having to pass around
    the full URL.
    """
    global _GSHEETS_LOCATIONS
    global _GSHEETS_CLIENT

    # Only load locations once.
    if _GSHEETS_LOCATIONS is None:
        gsheets_path = repo_path(_GSHEETS_JSON_FILE)

        if not os.path.exists(gsheets_path):
            log.info(
                f"Could not find _GSHEETS_JSON_FILE at {gsheets_path}. Defaulting to empty config."
            )
            _GSHEETS_LOCATIONS = {}

        else:
            _GSHEETS_LOCATIONS = {}
            with open(gsheets_path, "r") as fobj:
                for row in json.load(fobj):
                    _GSHEETS_LOCATIONS[row["name"]] = row

        if len(_GSHEETS_LOCATIONS) == 0:
            _GSHEETS_CLIENT = MagicMock()
        else:
            scoped_creds = get_credentials().with_scopes(gspread.auth.DEFAULT_SCOPES)
            _GSHEETS_CLIENT = gspread.client.Client(
                auth=scoped_creds, http_client=gspread.http_client.HTTPClient
            )

    if _GSHEETS_CLIENT is None or _GSHEETS_LOCATIONS is None:
        raise RuntimeError("GSheets client was not properly initialized.")


def update_gsheet(location_name: str, worksheet_name: str, dataframe: pd.DataFrame):
    """Write a pandas dadtaframe to a Google Sheet."""
    global _GSHEETS_LOCATIONS
    global _GSHEETS_CLIENT

    init_client()

    if _GSHEETS_LOCATIONS is None:
        raise RuntimeError("GSHEETS locations was not properly initialized.")

    if location_name not in _GSHEETS_LOCATIONS:
        log.warn(
            f"Location {location_name} is not present in _GSHEETS_LOCATIONS. Will skip writing."
        )
        return

    sheet = _GSHEETS_LOCATIONS[location_name]

    sh = _GSHEETS_CLIENT.open_by_url(sheet["url"])
    worksheet = sh.worksheet(worksheet_name)
    worksheet.update([dataframe.columns.values.tolist()] + dataframe.values.tolist())
    log.info(f"Wrote {dataframe.shape} cells to Google Sheets: {location_name}#{worksheet_name}")
