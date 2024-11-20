import polars as pl
from op_analytics.coreutils.testutils.inputdata import InputTestData
from unittest.mock import patch


from op_analytics.cli.subcommands.pulls.defillama import protocols

TESTDATA = InputTestData.at(__file__)


sample_protocols = [
    {
        "name": "ProtocolOne",
        "slug": "protocol_one",
        "category": "Finance",
        "parentProtocol": "parent#protocol_one",
    },
    {
        "name": "ProtocolTwo",
        "slug": "protocol_two",
        "category": "Gaming",
        "parentProtocol": None,
    },
]

sample_protocol_data = {
    "protocol_one": {
        "id": 101,
        "chainTvls": {
            "ChainAlpha": {
                "tvl": [
                    {"date": 1731542400, "totalLiquidityUSD": 12345.67},
                    {"date": 1731628800, "totalLiquidityUSD": 23456.78},
                ],
                "tokensInUsd": [
                    {
                        "date": 1731542400,
                        "tokens": {"TokenA": 5555.55, "TokenB": 6666.66},
                    },
                    {
                        "date": 1731628800,
                        "tokens": {"TokenA": 7777.77, "TokenB": 8888.88},
                    },
                ],
            },
            "ChainBeta": {
                "tvl": [
                    {"date": 1731542400, "totalLiquidityUSD": 34567.89},
                    {"date": 1731628800, "totalLiquidityUSD": 45678.90},
                    {"date": 1731628801, "totalLiquidityUSD": 45678.91},
                ],
                "tokensInUsd": [
                    {
                        "date": 1731542400,
                        "tokens": {"TokenC": 1234.56, "TokenD": 2345.67},
                    },
                    {
                        "date": 1731628800,
                        "tokens": {"TokenC": 3456.78, "TokenD": 4567.89},
                    },
                ],
            },
        },
    },
    "protocol_two": {
        "id": 202,
        "chainTvls": {
            "ChainGamma": {
                "tvl": [
                    {"date": 1731542400, "totalLiquidityUSD": 56789.01},
                    {"date": 1731628800, "totalLiquidityUSD": 67890.12},
                ],
                "tokensInUsd": [
                    {
                        "date": 1731542400,
                        "tokens": {"TokenX": 11111.11, "TokenY": 22222.22},
                    },
                    {
                        "date": 1731628800,
                        "tokens": {"TokenX": 33333.33, "TokenY": 44444.44},
                    },
                    {
                        "date": 1731628801,
                        "tokens": {"TokenX": 55555.55, "TokenY": 66666.66},
                    },
                ],
            }
        },
    },
}


def test_extract_protocol_metadata():
    metadata_df = protocols.extract_protocol_metadata(sample_protocols)
    expected_dicts = [
        {
            "protocol_name": "ProtocolOne",
            "protocol_slug": "protocol_one",
            "protocol_category": "Finance",
            "parent_protocol": "protocol_one",
        },
        {
            "protocol_name": "ProtocolTwo",
            "protocol_slug": "protocol_two",
            "protocol_category": "Gaming",
            "parent_protocol": "protocol_two",
        },
    ]
    assert metadata_df.to_dicts() == expected_dicts


def test_construct_urls():
    metadata_df = pl.DataFrame(
        [
            {"protocol_slug": "protocol_one"},
            {"protocol_slug": "protocol_two"},
        ]
    )
    urls = protocols.construct_urls(metadata_df, None, "https://api.llama.fi/protocol/{slug}")
    expected_urls = {
        "protocol_one": "https://api.llama.fi/protocol/protocol_one",
        "protocol_two": "https://api.llama.fi/protocol/protocol_two",
    }
    assert urls == expected_urls


def test_extract_protocol_tvl_to_dataframes_app_tvl():
    app_tvl_df, _ = protocols.extract_protocol_tvl_to_dataframes(sample_protocol_data)

    # Expected App TVL Data
    expected_app_tvl = [
        {
            "protocol_slug": "protocol_one",
            "chain": "ChainAlpha",
            "dt": "2024-11-14",
            "total_app_tvl": 12345.67,
        },
        {
            "protocol_slug": "protocol_one",
            "chain": "ChainAlpha",
            "dt": "2024-11-15",
            "total_app_tvl": 23456.78,
        },
        {
            "protocol_slug": "protocol_one",
            "chain": "ChainBeta",
            "dt": "2024-11-14",
            "total_app_tvl": 34567.89,
        },
        {
            "protocol_slug": "protocol_one",
            "chain": "ChainBeta",
            "dt": "2024-11-15",
            "total_app_tvl": 45678.90,
        },
        {
            "protocol_slug": "protocol_two",
            "chain": "ChainGamma",
            "dt": "2024-11-14",
            "total_app_tvl": 56789.01,
        },
        {
            "protocol_slug": "protocol_two",
            "chain": "ChainGamma",
            "dt": "2024-11-15",
            "total_app_tvl": 67890.12,
        },
    ]
    assert app_tvl_df.to_dicts() == expected_app_tvl


def test_extract_protocol_tvl_to_dataframes_app_token_tvl():
    _, app_token_tvl_df = protocols.extract_protocol_tvl_to_dataframes(sample_protocol_data)
    expected_app_token_tvl = [
        {
            "protocol_slug": "protocol_one",
            "chain": "ChainAlpha",
            "dt": "2024-11-14",
            "token": "TokenA",
            "app_token_tvl": 5555.55,
        },
        {
            "protocol_slug": "protocol_one",
            "chain": "ChainAlpha",
            "dt": "2024-11-14",
            "token": "TokenB",
            "app_token_tvl": 6666.66,
        },
        {
            "protocol_slug": "protocol_one",
            "chain": "ChainAlpha",
            "dt": "2024-11-15",
            "token": "TokenA",
            "app_token_tvl": 7777.77,
        },
        {
            "protocol_slug": "protocol_one",
            "chain": "ChainAlpha",
            "dt": "2024-11-15",
            "token": "TokenB",
            "app_token_tvl": 8888.88,
        },
        {
            "protocol_slug": "protocol_one",
            "chain": "ChainBeta",
            "dt": "2024-11-14",
            "token": "TokenC",
            "app_token_tvl": 1234.56,
        },
        {
            "protocol_slug": "protocol_one",
            "chain": "ChainBeta",
            "dt": "2024-11-14",
            "token": "TokenD",
            "app_token_tvl": 2345.67,
        },
        {
            "protocol_slug": "protocol_one",
            "chain": "ChainBeta",
            "dt": "2024-11-15",
            "token": "TokenC",
            "app_token_tvl": 3456.78,
        },
        {
            "protocol_slug": "protocol_one",
            "chain": "ChainBeta",
            "dt": "2024-11-15",
            "token": "TokenD",
            "app_token_tvl": 4567.89,
        },
        {
            "protocol_slug": "protocol_two",
            "chain": "ChainGamma",
            "dt": "2024-11-14",
            "token": "TokenX",
            "app_token_tvl": 11111.11,
        },
        {
            "protocol_slug": "protocol_two",
            "chain": "ChainGamma",
            "dt": "2024-11-14",
            "token": "TokenY",
            "app_token_tvl": 22222.22,
        },
        {
            "protocol_slug": "protocol_two",
            "chain": "ChainGamma",
            "dt": "2024-11-15",
            "token": "TokenX",
            "app_token_tvl": 33333.33,
        },
        {
            "protocol_slug": "protocol_two",
            "chain": "ChainGamma",
            "dt": "2024-11-15",
            "token": "TokenY",
            "app_token_tvl": 44444.44,
        },
    ]
    assert app_token_tvl_df.to_dicts() == expected_app_token_tvl


@patch("op_analytics.cli.subcommands.pulls.defillama.protocols.get_data")
@patch("op_analytics.cli.subcommands.pulls.defillama.protocols.upsert_unpartitioned_table")
@patch("op_analytics.cli.subcommands.pulls.defillama.protocols.upsert_partitioned_table")
def test_pull_single_protocol_tvl(
    mock_upsert_partitioned_table,
    mock_upsert_unpartitioned_table,
    mock_get_data,
):
    # Mock get_data to return sample summary and breakdown data
    mock_get_data.side_effect = [
        sample_protocols,  # metadata
        sample_protocol_data.get("protocol_one"),  # protocol tvl
    ]

    # Call the function under test
    result = protocols.pull_protocol_tvl(pull_protocols=["protocol_one"])

    # Assertions
    assert len(result.metadata_df) == 2
    assert len(result.app_tvl_df) == 4
    assert len(result.app_token_tvl_df) == 8

    # Verify that get_data was called twice (summary, metadata, and tvl)
    assert mock_get_data.call_count == 2

    # Check that BigQuery functions were called with correct parameters
    mock_upsert_unpartitioned_table.call_count == 3

    assert mock_upsert_unpartitioned_table.call_args_list[0].kwargs["unique_keys"] == [
        "protocol_slug",
    ]
    assert mock_upsert_partitioned_table.call_args_list[0].kwargs["unique_keys"] == [
        "dt",
        "protocol_slug",
        "chain",
    ]
    assert mock_upsert_partitioned_table.call_args_list[1].kwargs["unique_keys"] == [
        "dt",
        "protocol_slug",
        "chain",
        "token",
    ]


@patch("op_analytics.cli.subcommands.pulls.defillama.protocols.get_data")
@patch("op_analytics.cli.subcommands.pulls.defillama.protocols.upsert_unpartitioned_table")
@patch("op_analytics.cli.subcommands.pulls.defillama.protocols.upsert_partitioned_table")
def test_pull_all_protocol_tvl(
    mock_upsert_partitioned_table,
    mock_upsert_unpartitioned_table,
    mock_get_data,
):
    # Mock get_data to return sample summary and breakdown data
    mock_get_data.side_effect = [
        sample_protocols,  # metadata
        sample_protocol_data.get("protocol_one"),  # protocol tvl
        sample_protocol_data.get("protocol_two"),  # protocol tvl
    ]

    # Call the function under test
    result = protocols.pull_protocol_tvl()

    # Assertions
    assert len(result.metadata_df) == 2  # Only 'Layer_Two'
    assert len(result.app_tvl_df) == 6  # Two data points
    assert len(result.app_token_tvl_df) == 12  # Two data points

    # Verify that get_data was called twice (summary, metadata, and tvl)
    assert mock_get_data.call_count == 3

    # Check that BigQuery functions were called with correct parameters
    mock_upsert_unpartitioned_table.call_count == 3

    assert mock_upsert_unpartitioned_table.call_args_list[0].kwargs["unique_keys"] == [
        "protocol_slug",
    ]
    assert mock_upsert_partitioned_table.call_args_list[0].kwargs["unique_keys"] == [
        "dt",
        "protocol_slug",
        "chain",
    ]
    assert mock_upsert_partitioned_table.call_args_list[1].kwargs["unique_keys"] == [
        "dt",
        "protocol_slug",
        "chain",
        "token",
    ]
