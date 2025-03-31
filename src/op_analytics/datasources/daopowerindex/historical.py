from dataclasses import dataclass

import polars as pl
import requests

from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.request import new_session, get_data
from op_analytics.coreutils.misc import raise_for_schema_mismatch
from op_analytics.coreutils.time import now_dt, datestr_subtract

HISTORICAL_ENDPOINT = "https://api.daocpi.com/api/historic-cpi"

HISTORICAL_CPI_SCHEMA = {
    "date": pl.String(),
    "hhi": pl.Float64(),
    "cpi": pl.Float64(),
}


@dataclass
class HistoricalCPI:
    df: pl.DataFrame

    @classmethod
    def fetch(
        cls,
        session: requests.Session | None = None,
        fetch_dt: str | None = None,
    ) -> "HistoricalCPI":
        session = session or new_session()

        api_key = env_get("DAOCPI_API_KEY")

        # If no fetch_dt is provided we will fetch the latest available data
        # from the last 5 days.
        if fetch_dt is None:
            today = now_dt()
            fetch_dt = [datestr_subtract(today, n) for n in range(5)]
        else:
            fetch_dt = [fetch_dt]

        for dt in fetch_dt:
            try:
                response = get_data(
                    session,
                    HISTORICAL_ENDPOINT,
                    headers={"x-api-key": api_key},
                    params={"date": dt},
                )
            except requests.exceptions.HTTPError as ex:
                if ex.response.status_code == 404:
                    continue
                else:
                    raise ex

            df = pl.DataFrame(
                [
                    {
                        "date": response["date"],
                        "hhi": response["HHI"],
                        "cpi": response["CPI"],
                    }
                ]
            )

            raise_for_schema_mismatch(
                actual_schema=df.schema,
                expected_schema=pl.Schema(HISTORICAL_CPI_SCHEMA),
            )

            return cls(df=df)

        raise Exception(f"Failed to fetch data, no data found in: {fetch_dt}")
