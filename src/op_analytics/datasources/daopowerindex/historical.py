from dataclasses import dataclass
from datetime import datetime

import polars as pl
import requests

from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.request import new_session, get_data
from op_analytics.coreutils.misc import raise_for_schema_mismatch


HISTORICAL_ENDPOINT = "https://api.daocpi.com/api/historic-cpi"

HISTORICAL_CPI_SCHEMA = {
    "dt": pl.String(),
    "hhi": pl.Float64(),
    "cpi": pl.Float64(),
}


@dataclass
class HistoricalCPI:
    df: pl.DataFrame

    @classmethod
    def fetch(cls, session: requests.Session | None = None) -> "HistoricalCPI":
        session = session or new_session()

        api_key = env_get("DAOCPI_API_KEY")

        response = get_data(
            session,
            HISTORICAL_ENDPOINT,
            headers={"x-api-key": api_key},
        )

        rows = []
        for row in response:
            # date is formatted as "2024-11-30T00:00:00.000Z"
            # we want to convert it to "2024-11-30"
            dt = datetime.strptime(row["date"], "%Y-%m-%dT00:00:00.000Z").strftime("%Y-%m-%d")

            rows.append(
                {
                    "dt": dt,
                    "hhi": row["HHI"],
                    "cpi": row["CPI"],
                }
            )
        df = pl.DataFrame(rows)

        raise_for_schema_mismatch(
            actual_schema=df.schema,
            expected_schema=pl.Schema(HISTORICAL_CPI_SCHEMA),
        )

        return cls(df=df)
