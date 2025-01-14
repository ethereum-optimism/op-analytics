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
