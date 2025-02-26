from op_analytics.datasources.defillama.chaintvl.metadata import (
    get_dfl_chains,
    extract_chain_metadata,
)
from op_analytics.datasources.defillama.chaintvl.chain import Chain
from op_analytics.coreutils.testutils.inputdata import InputTestData

TESTDATA = InputTestData.at(__file__)


# Sample data resembling the API response
sample_summary = [
    {
        "gecko_id": "layer_one",
        "tvl": 12345678.91011,
        "tokenSymbol": "L1",
        "cmcId": "1234",
        "name": "Layer_One",
        "chainId": 1111,
    },
    {
        "gecko_id": "layer_two",
        "tvl": 987654.321,
        "tokenSymbol": "L2",
        "cmcId": "4321",
        "name": "Layer_Two",
        "chainId": 9,
    },
]

sample_metadata = {
    "protocols": [],
    "chainCoingeckoIds": {
        "Layer_One": {
            "geckoId": "layerone",
            "symbol": "L1",
            "cmcId": "1",
            "categories": ["SVM"],
            "chainId": 1,
            "github": [
                "layer_one_git",
            ],
            "twitter": "layer_one",
            "url": "https://layer.one/",
        },
        "Layer_Two": {
            "geckoId": None,
            "symbol": None,
            "cmcId": None,
            "github": ["two-org"],
            "categories": ["EVM", "Rollup", "Superchain"],
            "parent": {"chain": "layer_one", "types": ["L2", "gas"]},
            "chainId": 2,
            "twitter": "two",
            "url": "https://www.layer.two/",
        },
        "Layer_Three": {
            "geckoId": "three",
            "symbol": "L3",
            "cmcId": "3",
            "categories": ["EVM"],
            "parent": {"chain": "Layer Two", "types": ["L3"]},
            "twitter": "Three",
            "url": "https://layer.three",
        },
    },
}

sample_tvl_data = {
    "Layer_One": [
        {"date": 1731542400, "tvl": 12340000.00},
        {"date": 1731628800, "tvl": 12345678.90},
    ],
    "Layer_Two": [
        {"date": 1731542400, "tvl": 1000000000.00},
        {"date": 1731628800, "tvl": 1000000001.00},
    ],
}


def test_format_summary():
    chains_list = get_dfl_chains(sample_summary)
    expected_list = ["Layer_One", "Layer_Two"]

    assert chains_list == expected_list


def test_extract_metadata():
    chain_list = get_dfl_chains(sample_summary)

    metadata_df = extract_chain_metadata(
        chain_metadata=sample_metadata["chainCoingeckoIds"],  # type: ignore
        dfl_chains=chain_list,
    )

    expected_dicts = [
        {
            "chain_name": "Layer_One",
            "chain_id": 1,
            "dfl_tracks_tvl": 1,
            "is_evm": 0,
            "is_superchain": 0,
            "layer": "L1",
            "is_rollup": 0,
            "gecko_id": "layerone",
            "cmc_id": "1",
            "symbol": "L1",
        },
        {
            "chain_name": "Layer_Two",
            "chain_id": 2,
            "dfl_tracks_tvl": 1,
            "is_evm": 1,
            "is_superchain": 1,
            "layer": "L2",
            "is_rollup": 1,
            "gecko_id": None,
            "cmc_id": None,
            "symbol": None,
        },
        {
            "chain_name": "Layer_Three",
            "chain_id": None,
            "dfl_tracks_tvl": 0,
            "is_evm": 1,
            "is_superchain": 0,
            "layer": "L3",
            "is_rollup": 1,
            "gecko_id": "three",
            "cmc_id": "3",
            "symbol": "L3",
        },
    ]
    print(metadata_df.to_dicts())
    assert metadata_df.to_dicts() == expected_dicts


def test_pull_historical_single_chain_tvl():
    sample_tvl = [
        {"date": 1731542400, "tvl": 12340000.00},
        {"date": 1731628800, "tvl": 12345678.90},
    ]

    chain = Chain.of("Layer_One", sample_tvl)
    assert chain.rows == [
        {"chain_name": "Layer_One", "dt": "2024-11-14", "tvl": 12340000.0},
        {"chain_name": "Layer_One", "dt": "2024-11-15", "tvl": 12345678.9},
    ]
