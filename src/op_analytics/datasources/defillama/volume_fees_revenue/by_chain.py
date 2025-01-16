import polars as pl

from op_analytics.coreutils.time import dt_fromepoch


def get_chain_df(chain_responses: dict[str, dict]) -> pl.DataFrame:
    rows = []
    for chain, chain_data in chain_responses.items():
        for row in chain_data["totalDataChart"]:
            rows.append(
                {
                    "chain": chain,
                    "dt": dt_fromepoch(row[0]),
                    "total_usd": row[1],
                }
            )

    df = pl.DataFrame(
        rows,
        schema={
            "dt": pl.String,
            "chain": pl.String,
            "total_usd": pl.Int64,
        },
    )
    return df


def get_chain_breakdown_df(chain_responses: dict[str, dict]) -> pl.DataFrame:
    rows = []
    for chain, chain_data in chain_responses.items():
        for row in chain_data["totalDataChartBreakdown"]:
            dt = dt_fromepoch(row[0])
            assert isinstance(row[1], dict)

            for breakdown_name, total_usd in row[1].items():
                rows.append(
                    {
                        "dt": dt,
                        "chain": chain,
                        "breakdown_name": breakdown_name,
                        "total_usd": total_usd,
                    }
                )

    df = pl.DataFrame(
        rows,
        schema={
            "dt": pl.String,
            "chain": pl.String,
            "breakdown_name": pl.String,
            "total_usd": pl.Int64,
        },
    )
    return df
