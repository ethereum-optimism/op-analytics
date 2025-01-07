import polars as pl
from op_analytics.coreutils.time import dt_fromepoch


def get_total_data_chart_df(summary_response) -> pl.DataFrame:
    rows = []
    for row in summary_response["totalDataChart"]:
        rows.append(
            {
                "dt": dt_fromepoch(row[0]),
                "total_usd": row[1],
            }
        )

    df = pl.DataFrame(
        rows,
        schema={
            "dt": pl.String,
            "total_usd": pl.Int64,
        },
    )

    return df
