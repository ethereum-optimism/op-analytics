from unittest.mock import MagicMock

import gspread
import pandas as pd


from op_coreutils.logger import structlog
from op_coreutils.gcpauth import get_credentials
from op_coreutils.env.vault import env_get

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
    global _GSHEETS_CLIENT
    global _GSHEETS_LOCATIONS

    if _GSHEETS_CLIENT is None:
        _GSHEETS_LOCATIONS = env_get("gsheets")

        if len(_GSHEETS_LOCATIONS) == 0:
            _GSHEETS_CLIENT = MagicMock()
        else:
            scoped_creds = get_credentials().with_scopes(gspread.auth.DEFAULT_SCOPES)
            _GSHEETS_CLIENT = gspread.client.Client(
                auth=scoped_creds, http_client=gspread.http_client.HTTPClient
            )

    if _GSHEETS_CLIENT is None:
        raise RuntimeError("GSheets client was not properly initialized.")

    return _GSHEETS_LOCATIONS, _GSHEETS_CLIENT


def get_worksheet(location_name: str, worksheet_name: str):
    locations, client = init_client()

    if location_name not in locations:
        log.warn(
            f"Location {location_name} is not present in _GSHEETS_LOCATIONS. Will skip writing."
        )
        return

    sh = client.open_by_url(locations[location_name])
    worksheet = sh.worksheet(worksheet_name)
    return worksheet


def update_gsheet(location_name: str, worksheet_name: str, dataframe: pd.DataFrame):
    """Write a pandas dataframe to a Google Sheet."""
    worksheet = get_worksheet(location_name, worksheet_name)
    worksheet.update([dataframe.columns.values.tolist()] + dataframe.values.tolist())
    log.info(f"Wrote {dataframe.shape} cells to Google Sheets: {location_name}#{worksheet_name}")


def read_gsheet(location_name: str, worksheet_name: str):
    """Read data from a google sheet"""
    worksheet = get_worksheet(location_name, worksheet_name)
    return worksheet.get_all_records()
