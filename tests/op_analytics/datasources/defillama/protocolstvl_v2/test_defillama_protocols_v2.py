from op_analytics.datasources.defillama.protocolstvl.protocol import ProtocolTVL


PEPPERCOIN_RESPONSE = {
    "id": "5800",
    "name": "Peppercoin",
    "address": "chiliz:0x60F397acBCfB8f4e3234C659A3E10867e6fA6b67",
    "symbol": "PEPPER",
    "url": "https://www.peppercoin.com",
    "description": "Peppercoin is the home of Pepper Inc., empowering the community to stake, vote, and shape the future of PEPPER",
    "chain": "Chiliz",
    "logo": "https://icons.llama.fi/peppercoin.png",
    "audits": "0",
    "audit_note": None,
    "gecko_id": "pepper",
    "cmcId": "33603",
    "category": "Governance Incentives",
    "chains": ["Chiliz"],
    "oracles": [],
    "forkedFrom": [],
    "module": "chiliz-peppercoin/index.js",
    "twitter": "PepperChain",
    "listedAt": 1739812859,
    "chainTvls": {
        "Chiliz": {
            "tvl": [
                {"date": 1739750400, "totalLiquidityUSD": 0},
                {"date": 1739836800, "totalLiquidityUSD": 0},
                {"date": 1740003011, "totalLiquidityUSD": 0},
            ],
            "tokensInUsd": [
                {"date": 1739750400, "tokens": {}},
                {"date": 1739836800, "tokens": {}},
                {"date": 1740003011, "tokens": {}},
            ],
            "tokens": [
                {"date": 1739750400, "tokens": {}},
                {"date": 1739836800, "tokens": {}},
                {"date": 1740003011, "tokens": {}},
            ],
        },
        "Chiliz-staking": {
            "tvl": [
                {"date": 1739750400, "totalLiquidityUSD": 1481522.92399},
                {"date": 1739836800, "totalLiquidityUSD": 1511553.22643},
                {"date": 1740003011, "totalLiquidityUSD": 1742574.37318},
            ],
            "tokensInUsd": [
                {"date": 1739750400, "tokens": {"pepper": 1481522.92399}},
                {"date": 1739836800, "tokens": {"pepper": 1511553.22643}},
                {"date": 1740003011, "tokens": {"pepper": 1742574.37318}},
            ],
            "tokens": [
                {"date": 1739750400, "tokens": {"pepper": 1179556468142140}},
                {"date": 1739836800, "tokens": {"pepper": 1181824258350236}},
                {"date": 1740003011, "tokens": {"pepper": 1413280108011290.8}},
            ],
        },
        "staking": {
            "tvl": [
                {"date": 1739750400, "totalLiquidityUSD": 1481522.92399},
                {"date": 1739836800, "totalLiquidityUSD": 1511553.22643},
                {"date": 1740003011, "totalLiquidityUSD": 1742574.37318},
            ],
            "tokensInUsd": [
                {"date": 1739750400, "tokens": {"pepper": 1481522.92399}},
                {"date": 1739836800, "tokens": {"pepper": 1511553.22643}},
                {"date": 1740003011, "tokens": {"pepper": 1742574.37318}},
            ],
            "tokens": [
                {"date": 1739750400, "tokens": {"pepper": 1179556468142140}},
                {"date": 1739836800, "tokens": {"pepper": 1181824258350236}},
                {"date": 1740003011, "tokens": {"pepper": 1413280108011290.8}},
            ],
        },
    },
    "tvl": [
        {"date": 1739750400, "totalLiquidityUSD": 0},
        {"date": 1739836800, "totalLiquidityUSD": 0},
        {"date": 1740003011, "totalLiquidityUSD": 0},
    ],
    "tokensInUsd": [
        {"date": 1739750400, "tokens": {}},
        {"date": 1739836800, "tokens": {}},
        {"date": 1740003011, "tokens": {}},
    ],
    "tokens": [
        {"date": 1739750400, "tokens": {}},
        {"date": 1739836800, "tokens": {}},
        {"date": 1740003011, "tokens": {}},
    ],
    "currentChainTvls": {"Chiliz": 0, "Chiliz-staking": 1742574.37318, "staking": 1742574.37318},
    "raises": [],
    "metrics": {},
    "mcap": 0,
    "misrepresentedTokens": True,
}


def test_parse_protocol():
    data = ProtocolTVL.of(slug="peppercoin", data=PEPPERCOIN_RESPONSE)

    assert data.tvl_df.to_dicts() == [
        {
            "protocol_slug": "peppercoin",
            "chain": "Chiliz",
            "dt": "2025-02-17",
            "total_app_tvl": 0.0,
        },
        {
            "protocol_slug": "peppercoin",
            "chain": "Chiliz",
            "dt": "2025-02-18",
            "total_app_tvl": 0.0,
        },
        {
            "protocol_slug": "peppercoin",
            "chain": "Chiliz-staking",
            "dt": "2025-02-17",
            "total_app_tvl": 1481522.92399,
        },
        {
            "protocol_slug": "peppercoin",
            "chain": "Chiliz-staking",
            "dt": "2025-02-18",
            "total_app_tvl": 1511553.22643,
        },
        {
            "protocol_slug": "peppercoin",
            "chain": "staking",
            "dt": "2025-02-17",
            "total_app_tvl": 1481522.92399,
        },
        {
            "protocol_slug": "peppercoin",
            "chain": "staking",
            "dt": "2025-02-18",
            "total_app_tvl": 1511553.22643,
        },
    ]

    assert data.token_tvl_df.to_dicts() == [
        {
            "protocol_slug": "peppercoin",
            "chain": "Chiliz-staking",
            "dt": "2025-02-17",
            "token": "pepper",
            "app_token_tvl": 1179556468142140.0,
            "app_token_tvl_usd": 1481522.92399,
        },
        {
            "protocol_slug": "peppercoin",
            "chain": "Chiliz-staking",
            "dt": "2025-02-18",
            "token": "pepper",
            "app_token_tvl": 1181824258350236.0,
            "app_token_tvl_usd": 1511553.22643,
        },
        {
            "protocol_slug": "peppercoin",
            "chain": "staking",
            "dt": "2025-02-17",
            "token": "pepper",
            "app_token_tvl": 1179556468142140.0,
            "app_token_tvl_usd": 1481522.92399,
        },
        {
            "protocol_slug": "peppercoin",
            "chain": "staking",
            "dt": "2025-02-18",
            "token": "pepper",
            "app_token_tvl": 1181824258350236.0,
            "app_token_tvl_usd": 1511553.22643,
        },
    ]
