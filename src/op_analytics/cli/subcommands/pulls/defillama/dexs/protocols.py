import polars as pl


def get_protocols_df(summary_response):
    """The Protocols dataframe is obtained from the summary endpoint response.

    The summary endpoint can be called for various data types:

    - dailyVolume
    - dailyFees
    - dailyRevenue
    """

    protocols = summary_response["protocols"]

    MUST_HAVE_FIELDS = [
        "defillamaId",
        "name",
        "displayName",
        "module",
        "category",
        "logo",
        "chains",
        "protocolType",
        "methodologyURL",
        "methodology",
        "latestFetchIsOk",
        "slug",
        "id",
    ]
    OPTIONAL_FIELDS = [
        "parentProtocol",
        "total24h",
        "total48hto24h",
        "total7d",
        "total14dto7d",
        "total60dto30d",
        "total30d",
        "total1y",
        "totalAllTime",
        "average1y",
        "change_1d",
        "change_7d",
        "change_1m",
        "change_7dover7d",
        "change_30dover30d",
        "breakdown24h",
    ]

    total_metadata: list[dict] = []

    for element in protocols:
        metadata_row: dict[str, None | str | list[dict[str, str]]] = {}
        for key in MUST_HAVE_FIELDS:
            if key == "methodology":
                metadata_row[key] = convert_to_list_of_keyval(element[key])
            else:
                metadata_row[key] = element[key]
        for key in OPTIONAL_FIELDS:
            if key == "breakdown24h":
                metadata_row[key] = convert_to_chain_breakdown(element.get(key))
            else:
                metadata_row[key] = element.get(key)

        total_metadata.append(metadata_row)

    protocols_df = pl.DataFrame(
        total_metadata,
        schema={
            "defillamaId": pl.String,
            "name": pl.String,
            "displayName": pl.String,
            "module": pl.String,
            "category": pl.String,
            "logo": pl.String,
            "chains": pl.List(pl.String),
            "protocolType": pl.String,
            "methodologyURL": pl.String,
            "methodology": LIST_OF_KEYVAL,
            "latestFetchIsOk": pl.Boolean,
            "slug": pl.String,
            "id": pl.String,
            "parentProtocol": pl.String,
            "total24h": pl.Int64,
            "total48hto24h": pl.Int64,
            "total7d": pl.Int64,
            "total14dto7d": pl.Int64,
            "total60dto30d": pl.Int64,
            "total30d": pl.Int64,
            "total1y": pl.Int64,
            "totalAllTime": pl.Float64,
            "average1y": pl.Float64,
            "change_1d": pl.Float64,
            "change_7d": pl.Float64,
            "change_1m": pl.Float64,
            "change_7dover7d": pl.Float64,
            "change_30dover30d": pl.Float64,
            "breakdown24h": LIST_OF_CHAIN_VALUES,
        },
    )

    return protocols_df


LIST_OF_KEYVAL = pl.List(
    pl.Struct(
        [
            pl.Field("key", pl.String),
            pl.Field("value", pl.String),
        ]
    )
)

LIST_OF_CHAIN_VALUES = pl.List(
    pl.Struct(
        [
            pl.Field("chain", pl.String),
            pl.Field("name", pl.String),
            pl.Field("value", pl.Int64),
        ]
    )
)


def convert_to_list_of_keyval(data) -> list[dict[str, str]]:
    if isinstance(data, dict):
        result = []
        for key, val in data.items():
            result.append({"key": key, "value": val})
        return result

    elif isinstance(data, str):
        return [{"key": "fallback", "value": data}]

    else:
        raise ValueError(f"invalid methodology: {data!r}")


def convert_to_chain_breakdown(data) -> list[dict[str, str]] | None:
    if data is None:
        return None

    result = []
    if isinstance(data, dict):
        for chain, volumes in data.items():
            assert isinstance(volumes, dict)
            for name, value in volumes.items():
                result.append(
                    {
                        "chain": chain,
                        "name": name,
                        "value": value,
                    }
                )
    return result
