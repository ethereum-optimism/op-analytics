import json
import os

import polars as pl

from op_analytics.datasources.defillama.volumefeesrevenue.protocols import get_protocols_df


def test_protocols_df_full_response():
    with open(
        os.path.join(os.path.dirname(__file__), "mockdata/dexs_daily_volume_summary_response.json"),
        "r",
    ) as fobj:
        data = json.load(fobj)

    df = get_protocols_df(data)

    assert len(df) == 634

    assert df.schema == {
        "defillamaId": pl.String,
        "name": pl.String,
        "displayName": pl.String,
        "module": pl.String,
        "category": pl.String,
        "logo": pl.String,
        "chains": pl.List(pl.String),
        "protocolType": pl.String,
        "methodologyURL": pl.String,
        "methodology": pl.List(pl.Struct({"key": pl.String, "value": pl.String})),
        "latestFetchIsOk": pl.Boolean,
        "slug": pl.String,
        "id": pl.String,
        "parentProtocol": pl.String,
    }

    actual = df.head().to_dicts()
    assert actual == [
        {
            "defillamaId": "3",
            "name": "Curve DEX",
            "displayName": "Curve DEX",
            "module": "curve",
            "category": "Dexes",
            "logo": "https://icons.llamao.fi/icons/protocols/curve.png",
            "chains": [
                "Ethereum",
                "Polygon",
                "Fantom",
                "Arbitrum",
                "Avalanche",
                "Optimism",
                "Gnosis",
                "Fraxtal",
            ],
            "protocolType": "protocol",
            "methodologyURL": "https://github.com/DefiLlama/dimension-adapters/blob/master/dexs/curve",
            "methodology": [
                {"key": "UserFees", "value": "Swap fees paid by users"},
                {"key": "Fees", "value": "Swap fees paid by users"},
                {
                    "key": "Revenue",
                    "value": "Percentage of swap fees going to treasury and/or token holders",
                },
                {"key": "ProtocolRevenue", "value": "Percentage of swap fees going to treasury"},
                {"key": "HoldersRevenue", "value": "Money going to governance token holders"},
                {"key": "SupplySideRevenue", "value": "Liquidity providers revenue"},
            ],
            "latestFetchIsOk": True,
            "slug": "curve-dex",
            "id": "3",
            "parentProtocol": "parent#curve-finance",
        },
        {
            "defillamaId": "116",
            "name": "Balancer V1",
            "displayName": "Balancer V1",
            "module": "balancer",
            "category": "Dexes",
            "logo": "https://icons.llamao.fi/icons/protocols/balancer-v1.png",
            "chains": ["Ethereum"],
            "protocolType": "protocol",
            "methodologyURL": "https://github.com/DefiLlama/dimension-adapters/blob/master/dexs/balancer",
            "methodology": [
                {"key": "UserFees", "value": "Swap fees paid by users"},
                {"key": "Fees", "value": "Swap fees paid by users"},
                {
                    "key": "Revenue",
                    "value": "Percentage of swap fees going to treasury and/or token holders",
                },
                {"key": "ProtocolRevenue", "value": "Percentage of swap fees going to treasury"},
                {"key": "HoldersRevenue", "value": "Money going to governance token holders"},
                {"key": "SupplySideRevenue", "value": "Liquidity providers revenue"},
            ],
            "latestFetchIsOk": True,
            "slug": "balancer-v1",
            "id": "116",
            "parentProtocol": "parent#balancer",
        },
        {
            "defillamaId": "119",
            "name": "SushiSwap",
            "displayName": "SushiSwap",
            "module": "sushiswap",
            "category": "Dexes",
            "logo": "https://icons.llamao.fi/icons/protocols/sushiswap.png",
            "chains": [
                "Ethereum",
                "BSC",
                "Polygon",
                "Arbitrum",
                "Avalanche",
                "Harmony",
                "Fuse",
                "CORE",
                "Blast",
                "Fantom",
            ],
            "protocolType": "protocol",
            "methodologyURL": "https://github.com/DefiLlama/dimension-adapters/blob/master/dexs/sushiswap",
            "methodology": [
                {"key": "UserFees", "value": "Users pay a 0.3% fee on each trade"},
                {"key": "Fees", "value": "SushiSwap charges a flat 0.3% fee"},
                {"key": "Revenue", "value": "A 0.05% of each trade goes to treasury"},
                {"key": "ProtocolRevenue", "value": "Treasury receives a share of the fees"},
                {"key": "HoldersRevenue", "value": "None"},
                {
                    "key": "SupplySideRevenue",
                    "value": "Liquidity providers get 5/6 of all trades in their pools",
                },
            ],
            "latestFetchIsOk": True,
            "slug": "sushiswap",
            "id": "119",
            "parentProtocol": "parent#sushi",
        },
        {
            "defillamaId": "127",
            "name": "KyberSwap Classic",
            "displayName": "KyberSwap - Classic",
            "module": "kyberswap",
            "category": "Dexes",
            "logo": "https://icons.llamao.fi/icons/protocols/kyberswap-classic.png",
            "chains": [
                "Optimism",
                "Arbitrum",
                "Fantom",
                "Avalanche",
                "BSC",
                "Polygon",
                "Ethereum",
                "ZKsync Era",
                "Linea",
                "Scroll",
            ],
            "protocolType": "protocol",
            "methodologyURL": "https://github.com/DefiLlama/dimension-adapters/blob/master/dexs/kyberswap",
            "methodology": [
                {"key": "UserFees", "value": "Swap fees paid by users"},
                {"key": "Fees", "value": "Swap fees paid by users"},
                {
                    "key": "Revenue",
                    "value": "Percentage of swap fees going to treasury and/or token holders",
                },
                {"key": "ProtocolRevenue", "value": "Percentage of swap fees going to treasury"},
                {"key": "HoldersRevenue", "value": "Money going to governance token holders"},
                {"key": "SupplySideRevenue", "value": "Liquidity providers revenue"},
            ],
            "latestFetchIsOk": True,
            "slug": "kyberswap-classic",
            "id": "127",
            "parentProtocol": "parent#kyberswap",
        },
        {
            "defillamaId": "133",
            "name": "Shell Protocol",
            "displayName": "Shell Protocol",
            "module": "shell-protocol",
            "category": "Dexes",
            "logo": "https://icons.llamao.fi/icons/protocols/shell-protocol.jpg",
            "chains": ["Arbitrum"],
            "protocolType": "protocol",
            "methodologyURL": "https://github.com/DefiLlama/dimension-adapters/blob/master/dexs/shell-protocol",
            "methodology": [
                {"key": "UserFees", "value": "Swap fees paid by users"},
                {"key": "Fees", "value": "Swap fees paid by users"},
                {
                    "key": "Revenue",
                    "value": "Percentage of swap fees going to treasury and/or token holders",
                },
                {"key": "ProtocolRevenue", "value": "Percentage of swap fees going to treasury"},
                {"key": "HoldersRevenue", "value": "Money going to governance token holders"},
                {"key": "SupplySideRevenue", "value": "Liquidity providers revenue"},
            ],
            "latestFetchIsOk": True,
            "slug": "shell-protocol",
            "id": "133",
            "parentProtocol": None,
        },
    ]


def test_breakdown24h():
    mock_response = {
        "protocols": [
            {
                "total24h": 400688463,
                "total48hto24h": 363047289,
                "total7d": 3632338610,
                "total14dto7d": 3303808860,
                "total60dto30d": 26678113925,
                "total30d": 19781592318,
                "total1y": 180577721770,
                "totalAllTime": 207992316157,
                "average1y": None,
                "change_1d": 10.37,
                "change_7d": 9.56,
                "change_1m": -55.8,
                "change_7dover7d": 9.94,
                "change_30dover30d": -25.85,
                "breakdown24h": {"solana": {"Orca": 395260883}, "eclipse": {"Orca": 5427580}},
                "defillamaId": "283",
                "name": "Orca",
                "displayName": "Orca",
                "module": "orca",
                "category": "Dexes",
                "logo": "https://icons.llamao.fi/icons/protocols/orca.jpg",
                "chains": ["Solana", "Eclipse"],
                "protocolType": "protocol",
                "methodologyURL": "https://github.com/DefiLlama/dimension-adapters/blob/master/dexs/orca",
                "methodology": {
                    "UserFees": "Swap fees paid by users",
                    "Fees": "Swap fees paid by users",
                    "Revenue": "Percentage of swap fees going to treasury and/or token holders",
                    "ProtocolRevenue": "Percentage of swap fees going to treasury",
                    "HoldersRevenue": "Money going to governance token holders",
                    "SupplySideRevenue": "Liquidity providers revenue",
                },
                "latestFetchIsOk": True,
                "slug": "orca",
                "id": "283",
            }
        ]
    }

    df = get_protocols_df(mock_response)
    actual = df.to_dicts()
    assert actual == [
        {
            "defillamaId": "283",
            "name": "Orca",
            "displayName": "Orca",
            "module": "orca",
            "category": "Dexes",
            "logo": "https://icons.llamao.fi/icons/protocols/orca.jpg",
            "chains": ["Solana", "Eclipse"],
            "protocolType": "protocol",
            "methodologyURL": "https://github.com/DefiLlama/dimension-adapters/blob/master/dexs/orca",
            "methodology": [
                {"key": "UserFees", "value": "Swap fees paid by users"},
                {"key": "Fees", "value": "Swap fees paid by users"},
                {
                    "key": "Revenue",
                    "value": "Percentage of swap fees going to treasury and/or token holders",
                },
                {"key": "ProtocolRevenue", "value": "Percentage of swap fees going to treasury"},
                {"key": "HoldersRevenue", "value": "Money going to governance token holders"},
                {"key": "SupplySideRevenue", "value": "Liquidity providers revenue"},
            ],
            "latestFetchIsOk": True,
            "slug": "orca",
            "id": "283",
            "parentProtocol": None,
        }
    ]
