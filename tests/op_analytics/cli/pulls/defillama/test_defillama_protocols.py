from datetime import date
from typing import Any
from unittest.mock import patch

import polars as pl

from op_analytics.cli.subcommands.pulls.defillama import protocols
from op_analytics.coreutils.testutils.inputdata import InputTestData

TESTDATA = InputTestData.at(__file__)


sample_protocols: list[dict[str, Any]] = [
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
                "tokens": [
                    {
                        "date": 1731542400,
                        "tokens": {"TokenA": 1.1, "TokenB": 2.2},
                    },
                    {
                        "date": 1731628800,
                        "tokens": {"TokenA": 3.3, "TokenB": 4.4},
                    },
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
                "tokens": [
                    {
                        "date": 1731542400,
                        "tokens": {"TokenC": 1.2, "TokenD": 3.4},
                    },
                    {
                        "date": 1731628800,
                        "tokens": {"TokenC": 4.5, "TokenD": 6.6},
                    },
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
                "tokens": [
                    {
                        "date": 1731542400,
                        "tokens": {"TokenX": 9.9, "TokenY": 8.8},
                    },
                    {
                        "date": 1731628800,
                        "tokens": {"TokenX": 7.7, "TokenY": 6.6},
                    },
                    {
                        "date": 1731628801,
                        "tokens": {"TokenX": 5.5, "TokenY": 4.4},
                    },
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

expected_metadata_dicts = [
    {
        "protocol_name": "ProtocolOne",
        "protocol_slug": "protocol_one",
        "protocol_category": "Finance",
        "parent_protocol": "protocol_one",
        "wrong_liquidity": None,
        "misrepresented_tokens": None,
    },
    {
        "protocol_name": "ProtocolTwo",
        "protocol_slug": "protocol_two",
        "protocol_category": "Gaming",
        "parent_protocol": "protocol_two",
        "wrong_liquidity": None,
        "misrepresented_tokens": None,
    },
]

expected_single_protocol_app_tvl_dicts = [
    {
        "protocol_slug": "protocol_one",
        "chain": "ChainAlpha",
        "dt": "2024-11-15",
        "total_app_tvl": 23456.78,
    },
    {
        "protocol_slug": "protocol_one",
        "chain": "ChainBeta",
        "dt": "2024-11-15",
        "total_app_tvl": 45678.9,
    },
]

expected_single_protocol_app_token_tvl_dicts = [
    {
        "protocol_slug": "protocol_one",
        "chain": "ChainAlpha",
        "dt": "2024-11-15",
        "token": "TokenA",
        "app_token_tvl": 3.3,
        "app_token_tvl_usd": 7777.77,
    },
    {
        "protocol_slug": "protocol_one",
        "chain": "ChainAlpha",
        "dt": "2024-11-15",
        "token": "TokenB",
        "app_token_tvl": 4.4,
        "app_token_tvl_usd": 8888.88,
    },
    {
        "protocol_slug": "protocol_one",
        "chain": "ChainBeta",
        "dt": "2024-11-15",
        "token": "TokenC",
        "app_token_tvl": 4.5,
        "app_token_tvl_usd": 3456.78,
    },
    {
        "protocol_slug": "protocol_one",
        "chain": "ChainBeta",
        "dt": "2024-11-15",
        "token": "TokenD",
        "app_token_tvl": 6.6,
        "app_token_tvl_usd": 4567.89,
    },
]

expected_all_protocol_app_tvl_dicts = [
    {
        "protocol_slug": "protocol_one",
        "chain": "ChainAlpha",
        "dt": "2024-11-15",
        "total_app_tvl": 23456.78,
    },
    {
        "protocol_slug": "protocol_one",
        "chain": "ChainBeta",
        "dt": "2024-11-15",
        "total_app_tvl": 45678.9,
    },
    {
        "protocol_slug": "protocol_two",
        "chain": "ChainGamma",
        "dt": "2024-11-15",
        "total_app_tvl": 67890.12,
    },
]

expected_all_protocol_app_token_tvl_dicts = [
    {
        "protocol_slug": "protocol_one",
        "chain": "ChainAlpha",
        "dt": "2024-11-15",
        "token": "TokenA",
        "app_token_tvl": 3.3,
        "app_token_tvl_usd": 7777.77,
    },
    {
        "protocol_slug": "protocol_one",
        "chain": "ChainAlpha",
        "dt": "2024-11-15",
        "token": "TokenB",
        "app_token_tvl": 4.4,
        "app_token_tvl_usd": 8888.88,
    },
    {
        "protocol_slug": "protocol_one",
        "chain": "ChainBeta",
        "dt": "2024-11-15",
        "token": "TokenC",
        "app_token_tvl": 4.5,
        "app_token_tvl_usd": 3456.78,
    },
    {
        "protocol_slug": "protocol_one",
        "chain": "ChainBeta",
        "dt": "2024-11-15",
        "token": "TokenD",
        "app_token_tvl": 6.6,
        "app_token_tvl_usd": 4567.89,
    },
    {
        "protocol_slug": "protocol_two",
        "chain": "ChainGamma",
        "dt": "2024-11-15",
        "token": "TokenX",
        "app_token_tvl": 7.7,
        "app_token_tvl_usd": 33333.33,
    },
    {
        "protocol_slug": "protocol_two",
        "chain": "ChainGamma",
        "dt": "2024-11-15",
        "token": "TokenY",
        "app_token_tvl": 6.6,
        "app_token_tvl_usd": 44444.44,
    },
]


def test_extract_protocol_metadata():
    metadata_df = protocols.extract_protocol_metadata(sample_protocols)
    expected_dicts = [
        {
            "protocol_name": "ProtocolOne",
            "protocol_slug": "protocol_one",
            "protocol_category": "Finance",
            "parent_protocol": "protocol_one",
            "wrong_liquidity": None,
            "misrepresented_tokens": None,
        },
        {
            "protocol_name": "ProtocolTwo",
            "protocol_slug": "protocol_two",
            "protocol_category": "Gaming",
            "parent_protocol": "protocol_two",
            "wrong_liquidity": None,
            "misrepresented_tokens": None,
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
    slugs = protocols.construct_slugs(metadata_df, None)
    expected_slugs = ["protocol_one", "protocol_two"]
    assert slugs == expected_slugs


@patch("op_analytics.cli.subcommands.pulls.defillama.protocols.get_data")
@patch("op_analytics.cli.subcommands.pulls.defillama.protocols.now_date")
@patch("op_analytics.coreutils.partitioned.dailydata.DataWriter.write")
def test_pull_single_protocol_tvl(
    mock_write,
    mock_now_date,
    mock_get_data,
):
    mock_now_date.return_value = date(2024, 11, 22)

    # Mock get_data to return sample summary and breakdown data
    mock_get_data.side_effect = [
        sample_protocols,  # metadata
        sample_protocol_data.get("protocol_one"),  # protocol tvl
    ]

    # Call the function under test
    result = protocols.pull_protocol_tvl(pull_protocols=["protocol_one"])

    # Assertions
    assert result.metadata_df.to_dicts() == expected_metadata_dicts
    assert (
        result.app_tvl_df.sort("dt", "protocol_slug", "chain").to_dicts()
        == expected_single_protocol_app_tvl_dicts
    )
    assert (
        result.app_token_tvl_df.sort("dt", "protocol_slug", "chain", "token").to_dicts()
        == expected_single_protocol_app_token_tvl_dicts
    )

    # Verify that get_data was called twice (summary, metadata, and tvl)
    assert mock_get_data.call_count == 2

    # Check that writer functions were called with correct parameters
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
            "dataset_name": "defillama/protocols_metadata_v1",
            "df_columns": [
                "protocol_name",
                "protocol_slug",
                "protocol_category",
                "parent_protocol",
                "wrong_liquidity",
                "misrepresented_tokens",
                "dt",
            ],
            "num_rows": 2,
        },
        {
            "dataset_name": "defillama/protocols_tvl_v1",
            "df_columns": ["protocol_slug", "chain", "total_app_tvl", "dt"],
            "num_rows": 2,
        },
        {
            "dataset_name": "defillama/protocols_token_tvl_v1",
            "df_columns": [
                "protocol_slug",
                "chain",
                "token",
                "app_token_tvl",
                "app_token_tvl_usd",
                "dt",
            ],
            "num_rows": 4,
        },
    ]


@patch("op_analytics.cli.subcommands.pulls.defillama.protocols.get_data")
@patch("op_analytics.cli.subcommands.pulls.defillama.protocols.now_date")
@patch("op_analytics.coreutils.partitioned.dailydata.DataWriter.write")
def test_pull_all_protocol_tvl(
    mock_write,
    mock_now_date,
    mock_get_data,
):
    mock_now_date.return_value = date(2024, 11, 22)

    # Mock get_data to return sample summary and breakdown data
    mock_get_data.side_effect = [
        sample_protocols,  # metadata
        sample_protocol_data.get("protocol_one"),  # protocol tvl
        sample_protocol_data.get("protocol_two"),  # protocol tvl
    ]

    # Call the function under test
    result = protocols.pull_protocol_tvl()

    # Assertions
    assert result.metadata_df.to_dicts() == expected_metadata_dicts
    assert (
        result.app_tvl_df.sort("dt", "protocol_slug", "chain").to_dicts()
        == expected_all_protocol_app_tvl_dicts
    )
    assert (
        result.app_token_tvl_df.sort("dt", "protocol_slug", "chain", "token").to_dicts()
        == expected_all_protocol_app_token_tvl_dicts
    )

    # Verify that get_data was called twice (summary, metadata, and tvl)
    assert mock_get_data.call_count == 3

    # Check that BigQuery functions were called with correct parameters
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
            "dataset_name": "defillama/protocols_metadata_v1",
            "df_columns": [
                "protocol_name",
                "protocol_slug",
                "protocol_category",
                "parent_protocol",
                "wrong_liquidity",
                "misrepresented_tokens",
                "dt",
            ],
            "num_rows": 2,
        },
        {
            "dataset_name": "defillama/protocols_tvl_v1",
            "df_columns": ["protocol_slug", "chain", "total_app_tvl", "dt"],
            "num_rows": 3,
        },
        {
            "dataset_name": "defillama/protocols_token_tvl_v1",
            "df_columns": [
                "protocol_slug",
                "chain",
                "token",
                "app_token_tvl",
                "app_token_tvl_usd",
                "dt",
            ],
            "num_rows": 6,
        },
    ]
