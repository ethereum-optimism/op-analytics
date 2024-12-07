from unittest.mock import patch

from op_analytics.cli.subcommands.pulls.defillama import historical_chain_tvl
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
    chains_list = historical_chain_tvl.get_dfl_chains(sample_summary)
    expected_list = ["Layer_One", "Layer_Two"]

    assert chains_list == expected_list


def test_extract_metadata():
    chain_list = historical_chain_tvl.get_dfl_chains(sample_summary)

    metadata_df = historical_chain_tvl.extract_chain_metadata(
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


def test_construct_urls():
    chain_list = historical_chain_tvl.get_dfl_chains(sample_summary)
    urls = historical_chain_tvl.construct_urls(
        chain_list, None, historical_chain_tvl.CHAINS_TVL_ENDPOINT
    )

    expected_urls = {
        "Layer_One": "https://api.llama.fi/v2/historicalChainTvl/Layer_One",
        "Layer_Two": "https://api.llama.fi/v2/historicalChainTvl/Layer_Two",
    }

    assert urls == expected_urls


def test_extract_chain_tvl_to_dataframe():
    extracted_df = historical_chain_tvl.extract_chain_tvl_to_dataframe(sample_tvl_data)
    expected_dict = [
        {"chain_name": "Layer_One", "dt": "2024-11-14", "tvl": 12340000.0},
        {"chain_name": "Layer_One", "dt": "2024-11-15", "tvl": 12345678.9},
        {"chain_name": "Layer_Two", "dt": "2024-11-14", "tvl": 1000000000.0},
        {"chain_name": "Layer_Two", "dt": "2024-11-15", "tvl": 1000000001.0},
    ]

    assert extracted_df.to_dicts() == expected_dict


@patch("op_analytics.cli.subcommands.pulls.defillama.historical_chain_tvl.get_data")
@patch("op_analytics.coreutils.partitioned.dailydata.PartitionedWriteManager.write")
def test_pull_historical_single_chain_tvl(
    mock_write,
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
    result = historical_chain_tvl.pull_historical_chain_tvl(pull_chains=["Layer_One"])

    # Assertions
    assert len(result.metadata_df) == 3  # Only 'Layer_Two'
    assert len(result.tvl_df) == 2  # Two data points

    # Check write calls
    write_calls = [
        dict(
            dataset_name=_.kwargs["output_data"].root_path,
            df_columns=_.kwargs["output_data"].dataframe.columns,
            num_rows=len(_.kwargs["output_data"].dataframe),
        )
        for _ in mock_write.call_args_list
    ]
    assert write_calls == [
        {
            "dataset_name": "defillama/chains_metadata_v1",
            "df_columns": [
                "chain_name",
                "chain_id",
                "dfl_tracks_tvl",
                "is_evm",
                "is_superchain",
                "layer",
                "is_rollup",
                "gecko_id",
                "cmc_id",
                "symbol",
                "dt",
            ],
            "num_rows": 3,
        },
        {
            "dataset_name": "defillama/historical_chain_tvl_v1",
            "df_columns": ["chain_name", "tvl", "dt"],
            "num_rows": 1,
        },
        {
            "dataset_name": "defillama/historical_chain_tvl_v1",
            "df_columns": ["chain_name", "tvl", "dt"],
            "num_rows": 1,
        },
    ]


@patch("op_analytics.cli.subcommands.pulls.defillama.historical_chain_tvl.get_data")
@patch("op_analytics.coreutils.partitioned.dailydata.PartitionedWriteManager.write")
def test_pull_historical_all_chain_tvl(
    mock_write,
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
    result = historical_chain_tvl.pull_historical_chain_tvl()

    # Assertions
    assert len(result.metadata_df) == 3  # All three chains
    assert len(result.tvl_df) >= 3  # Data points from both chains

    # Check that metadata contains all three chains
    assert set(result.metadata_df["chain_name"].to_list()) == {
        "Layer_One",
        "Layer_Two",
        "Layer_Three",
    }

    # Check that breakdown contains data from both stablecoins
    assert set(result.tvl_df["chain_name"].unique()) == {
        "Layer_One",
        "Layer_Two",
    }

    # Check write calls
    write_calls = [
        dict(
            dataset_name=_.kwargs["output_data"].root_path,
            df_columns=_.kwargs["output_data"].dataframe.columns,
            num_rows=len(_.kwargs["output_data"].dataframe),
        )
        for _ in mock_write.call_args_list
    ]
    assert write_calls == [
        {
            "dataset_name": "defillama/chains_metadata_v1",
            "df_columns": [
                "chain_name",
                "chain_id",
                "dfl_tracks_tvl",
                "is_evm",
                "is_superchain",
                "layer",
                "is_rollup",
                "gecko_id",
                "cmc_id",
                "symbol",
                "dt",
            ],
            "num_rows": 3,
        },
        {
            "dataset_name": "defillama/historical_chain_tvl_v1",
            "df_columns": ["chain_name", "tvl", "dt"],
            "num_rows": 2,
        },
        {
            "dataset_name": "defillama/historical_chain_tvl_v1",
            "df_columns": ["chain_name", "tvl", "dt"],
            "num_rows": 2,
        },
    ]
