from unittest.mock import patch

from op_analytics.coreutils.testutils.inputdata import InputTestData


from op_analytics.cli.subcommands.pulls.defillama import defillama_historical_chain_tvl

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
    summary_df = defillama_historical_chain_tvl.format_chain_summary(sample_summary)
    expected_dict = [
        {"chain": "Layer_One", "chain_id": 1111},
        {"chain": "Layer_Two", "chain_id": 9},
    ]

    assert summary_df.to_dicts() == expected_dict


def test_extract_metadata():
    summary_df = defillama_historical_chain_tvl.format_chain_summary(sample_summary)
    metadata_df = defillama_historical_chain_tvl.extract_chain_metadata(
        sample_metadata["chainCoingeckoIds"], summary_df
    )

    expected_dicts = [
        {
            "chain": "Layer_One",
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
            "chain": "Layer_Two",
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
            "chain": "Layer_Three",
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


def test_construct_urls():
    summary_df = defillama_historical_chain_tvl.format_chain_summary(sample_summary)
    urls = defillama_historical_chain_tvl.construct_urls(
        summary_df, None, defillama_historical_chain_tvl.CHAINS_TVL_ENDPOINT
    )

    expected_urls = {
        "Layer_One": "https://api.llama.fi/v2/historicalChainTvl/Layer_One",
        "Layer_Two": "https://api.llama.fi/v2/historicalChainTvl/Layer_Two",
    }

    assert urls == expected_urls


def test_extract_chain_tvl_to_dataframe():
    extracted_df = defillama_historical_chain_tvl.extract_chain_tvl_to_dataframe(sample_tvl_data)
    expected_dict = [
        {"chain": "Layer_One", "date": "2024-11-14", "tvl": 12340000.0},
        {"chain": "Layer_One", "date": "2024-11-15", "tvl": 12345678.9},
        {"chain": "Layer_Two", "date": "2024-11-14", "tvl": 1000000000.0},
        {"chain": "Layer_Two", "date": "2024-11-15", "tvl": 1000000001.0},
    ]

    assert extracted_df.to_dicts() == expected_dict


@patch("op_analytics.cli.subcommands.pulls.defillama.defillama_historical_chain_tvl.get_data")
@patch(
    "op_analytics.cli.subcommands.pulls.defillama.defillama_historical_chain_tvl.upsert_unpartitioned_table"
)
def test_pull_historical_single_chain_tvl(
    mock_upsert_unpartitioned_table,
    mock_get_data,
):
    sample_tvl = [
        {"date": 1731542400, "tvl": 12340000.00},
        {"date": 1731628800, "tvl": 12345678.90},
    ]
    # Mock get_data to return sample summary and breakdown data
    mock_get_data.side_effect = [
        sample_summary,  # Summary data
        sample_metadata,  # Metadata
        sample_tvl,  # TVL historical data
    ]

    # Call the function under test
    result = defillama_historical_chain_tvl.pull_historical_chain_tvl(pull_chains=["Layer_One"])

    # Assertions
    assert len(result.metadata_df) == 3  # Only 'Layer_Two'
    assert len(result.tvl_df) == 2  # Two data points

    # Verify that get_data was called twice (summary, metadata, and tvl)
    assert mock_get_data.call_count == 3

    # Check that BigQuery functions were called with correct parameters
    mock_upsert_unpartitioned_table.call_count == 2
    assert mock_upsert_unpartitioned_table.call_args_list[0].kwargs["unique_keys"] == [
        "chain_id",
        "chain",
    ]
    assert mock_upsert_unpartitioned_table.call_args_list[1].kwargs["unique_keys"] == [
        "date",
        "chain_id",
        "chain",
    ]


@patch("op_analytics.cli.subcommands.pulls.defillama.defillama_historical_chain_tvl.get_data")
@patch(
    "op_analytics.cli.subcommands.pulls.defillama.defillama_historical_chain_tvl.upsert_unpartitioned_table"
)
def test_pull_historical_all_chain_tvl(
    mock_upsert_unpartitioned_table,
    mock_get_data,
):
    # Mock get_data to return sample summary and breakdown data for both stablecoins
    mock_get_data.side_effect = [
        sample_summary,  # Summary data
        sample_metadata,  # Metadata
        [
            {"date": 1731542400, "tvl": 12340000.00},
            {"date": 1731628800, "tvl": 12345678.90},
        ],
        [
            {"date": 1731542400, "tvl": 1000000000.00},
            {"date": 1731628800, "tvl": 1000000001.00},
        ],  # TVL historical data
    ]
    # Call the function under test without specifying stablecoin_ids (process all)
    result = defillama_historical_chain_tvl.pull_historical_chain_tvl()

    # Assertions
    assert len(result.metadata_df) == 3  # All three chains
    assert len(result.tvl_df) >= 3  # Data points from both chains

    # Verify that get_data was called four times (summary, metadata, and two chain tvls)
    assert mock_get_data.call_count == 4

    # Check that BigQuery functions were called with correct parameters
    mock_upsert_unpartitioned_table.call_count == 2
    assert mock_upsert_unpartitioned_table.call_args_list[0].kwargs["unique_keys"] == [
        "chain_id",
        "chain",
    ]
    assert mock_upsert_unpartitioned_table.call_args_list[1].kwargs["unique_keys"] == [
        "date",
        "chain_id",
        "chain",
    ]
    # Check that metadata contains all three chains
    assert set(result.metadata_df["chain"].to_list()) == {"Layer_One", "Layer_Two", "Layer_Three"}

    # Check that breakdown contains data from both stablecoins
    assert set(result.tvl_df["chain"].unique()) == {
        "Layer_One",
        "Layer_Two",
    }
