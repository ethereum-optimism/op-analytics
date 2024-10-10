import json

from op_coreutils.testutils.pathmanager import PathManager

from op_analytics.cli.subcommands.pulls import defillama

TESTCASE = PathManager.at(__file__)


def test_process_breakdown_data():
    with open(TESTCASE.path("mockdata/stablecoin_tether.json"), "r") as fobj:
        data = json.load(fobj)

    dataframe, metadata = defillama.process_breakdown_data(data)

    expected_metadata = {
        "id": "1",
        "name": "Tether",
        "address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
        "symbol": "USDT",
        "url": "https://tether.to/",
        "pegType": "peggedUSD",
        "pegMechanism": "fiat-backed",
        "description": "Launched in 2014, Tether tokens pioneered the stablecoin model. Tether tokens are pegged to real-world currencies on a 1-to-1 basis. This offers traders, merchants and funds a low volatility solution when exiting positions in the market.",
        "mintRedeemDescription": "Tether customers who have undergone a verification process can exchange USD for USDT and redeem USDT for USD.",
        "onCoinGecko": "true",
        "gecko_id": "tether",
        "cmcId": "825",
        "priceSource": "defillama",
        "twitter": "https://twitter.com/Tether_to",
    }
    assert metadata == expected_metadata

    actual_rows = dataframe.to_dicts()[:10]
    expected_rows = [
        {
            "chain": "Optimism",
            "dt": "2022-05-11",
            "circulating": 19021887.0,
            "bridged_to": 19021887.0,
            "minted": None,
            "unreleased": None,
            "id": "1",
            "name": "Tether",
            "symbol": "USDT",
        },
        {
            "chain": "Optimism",
            "dt": "2022-05-12",
            "circulating": 19021887.0,
            "bridged_to": 19021887.0,
            "minted": None,
            "unreleased": None,
            "id": "1",
            "name": "Tether",
            "symbol": "USDT",
        },
        {
            "chain": "Optimism",
            "dt": "2022-05-13",
            "circulating": 19021906.0,
            "bridged_to": 19021906.0,
            "minted": None,
            "unreleased": None,
            "id": "1",
            "name": "Tether",
            "symbol": "USDT",
        },
        {
            "chain": "Optimism",
            "dt": "2022-05-14",
            "circulating": 19023188.0,
            "bridged_to": 19023188.0,
            "minted": None,
            "unreleased": None,
            "id": "1",
            "name": "Tether",
            "symbol": "USDT",
        },
        {
            "chain": "Optimism",
            "dt": "2022-05-15",
            "circulating": 18844204.0,
            "bridged_to": 18844204.0,
            "minted": None,
            "unreleased": None,
            "id": "1",
            "name": "Tether",
            "symbol": "USDT",
        },
        {
            "chain": "Optimism",
            "dt": "2022-05-16",
            "circulating": 18850486.0,
            "bridged_to": 18850486.0,
            "minted": None,
            "unreleased": None,
            "id": "1",
            "name": "Tether",
            "symbol": "USDT",
        },
        {
            "chain": "Optimism",
            "dt": "2022-05-17",
            "circulating": 18875986.0,
            "bridged_to": 18875986.0,
            "minted": None,
            "unreleased": None,
            "id": "1",
            "name": "Tether",
            "symbol": "USDT",
        },
        {
            "chain": "Optimism",
            "dt": "2022-05-18",
            "circulating": 19207042.0,
            "bridged_to": 19207042.0,
            "minted": None,
            "unreleased": None,
            "id": "1",
            "name": "Tether",
            "symbol": "USDT",
        },
        {
            "chain": "Optimism",
            "dt": "2022-05-19",
            "circulating": 19358232.0,
            "bridged_to": 19358232.0,
            "minted": None,
            "unreleased": None,
            "id": "1",
            "name": "Tether",
            "symbol": "USDT",
        },
        {
            "chain": "Optimism",
            "dt": "2022-05-20",
            "circulating": 19368126.0,
            "bridged_to": 19368126.0,
            "minted": None,
            "unreleased": None,
            "id": "1",
            "name": "Tether",
            "symbol": "USDT",
        },
    ]

    assert actual_rows == expected_rows
