from op_analytics.datasources.l2beat.tvsbreakdown import parse_tvs, clean_dataframe
from op_analytics.datasources.l2beat.utils import L2BeatProject


def test_parse_tvs_empty():
    response_data = {
        "success": True,
        "data": {
            "dataTimestamp": 1744124400,
            "breakdown": {"canonical": [], "native": [], "external": []},
        },
    }
    actual = parse_tvs(response_data, L2BeatProject(id="funki", slug="funki"))
    assert actual == []


def test_parse_tvs_funki():
    # https://l2beat.com/api/scaling/tvs/funki/breakdown
    response_data = {
        "success": True,
        "data": {
            "dataTimestamp": 1744124400,
            "breakdown": {
                "canonical": [
                    {
                        "assetId": "ethereum-0x9F52c8ecbEe10e00D9faaAc5Ee9Ba0fF6550F511",
                        "chain": {"name": "ethereum", "id": 1},
                        "amount": 9419186.975963663,
                        "usdValue": 416047.46675758,
                        "usdPrice": "0.04417021",
                        "isGasToken": False,
                        "tokenAddress": "0x9F52c8ecbEe10e00D9faaAc5Ee9Ba0fF6550F511",
                        "escrows": [
                            {
                                "amount": 9419186.975963663,
                                "usdValue": 416047.46675758,
                                "escrowAddress": "0xA2C1C1A473250094a6244F2bcf6Cb51F670Ad3aC",
                                "name": "L1StandardBridge",
                                "isSharedEscrow": False,
                                "url": "https://etherscan.io/address/0xA2C1C1A473250094a6244F2bcf6Cb51F670Ad3aC",
                            }
                        ],
                        "iconUrl": "https://coin-images.coingecko.com/coins/images/21070/large/SipherToken.png?1696520453",
                        "symbol": "SIPHER",
                        "name": "Sipher Token",
                        "url": "https://etherscan.io/address/0x9F52c8ecbEe10e00D9faaAc5Ee9Ba0fF6550F511",
                        "supply": "zero",
                    },
                    {
                        "assetId": "ethereum-native",
                        "chain": {"name": "ethereum", "id": 1},
                        "amount": 72.52974917119296,
                        "usdValue": 111459.32322348957,
                        "usdPrice": "1536.7394",
                        "isGasToken": True,
                        "escrows": [
                            {
                                "amount": 72.52974917119296,
                                "usdValue": 111459.32322348957,
                                "escrowAddress": "0x5C9C7f98eD153a2deAA981eB5C97B31744AccF22",
                                "name": "OptimismPortal",
                                "isSharedEscrow": False,
                                "url": "https://etherscan.io/address/0x5C9C7f98eD153a2deAA981eB5C97B31744AccF22",
                            }
                        ],
                        "iconUrl": "https://assets.coingecko.com/coins/images/279/large/ethereum.png?1595348880",
                        "symbol": "ETH",
                        "name": "Ether",
                        "supply": "zero",
                    },
                    {
                        "assetId": "ethereum-0xdAC17F958D2ee523a2206206994597C13D831ec7",
                        "chain": {"name": "ethereum", "id": 1},
                        "amount": 47.307409,
                        "usdValue": 47.2901843723831,
                        "usdPrice": "0.9996359",
                        "isGasToken": False,
                        "tokenAddress": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                        "escrows": [
                            {
                                "amount": 47.307409,
                                "usdValue": 47.2901843723831,
                                "escrowAddress": "0xA2C1C1A473250094a6244F2bcf6Cb51F670Ad3aC",
                                "name": "L1StandardBridge",
                                "isSharedEscrow": False,
                                "url": "https://etherscan.io/address/0xA2C1C1A473250094a6244F2bcf6Cb51F670Ad3aC",
                            }
                        ],
                        "iconUrl": "https://assets.coingecko.com/coins/images/325/large/Tether.png?1696501661",
                        "symbol": "USDT",
                        "name": "Tether USD",
                        "url": "https://etherscan.io/address/0xdAC17F958D2ee523a2206206994597C13D831ec7",
                        "supply": "zero",
                    },
                    {
                        "assetId": "ethereum-0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                        "chain": {"name": "ethereum", "id": 1},
                        "amount": 19.1,
                        "usdValue": 19.098713615,
                        "usdPrice": "0.99993265",
                        "isGasToken": False,
                        "tokenAddress": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                        "escrows": [
                            {
                                "amount": 19.1,
                                "usdValue": 19.098713615,
                                "escrowAddress": "0xA2C1C1A473250094a6244F2bcf6Cb51F670Ad3aC",
                                "name": "L1StandardBridge",
                                "isSharedEscrow": False,
                                "url": "https://etherscan.io/address/0xA2C1C1A473250094a6244F2bcf6Cb51F670Ad3aC",
                            }
                        ],
                        "iconUrl": "https://assets.coingecko.com/coins/images/6319/large/usdc.png?1696506694",
                        "symbol": "USDC",
                        "name": "USD Coin",
                        "url": "https://etherscan.io/address/0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                        "supply": "zero",
                    },
                ],
                "native": [],
                "external": [],
            },
        },
    }

    actual = clean_dataframe(
        parse_tvs(response_data, L2BeatProject(id="funki", slug="funki"))
    ).to_dicts()
    assert actual == [
        {
            "dt": "2025-04-08",
            "project_id": "funki",
            "project_slug": "funki",
            "timestamp": 1744124400,
            "asset_id": "ethereum-0x9F52c8ecbEe10e00D9faaAc5Ee9Ba0fF6550F511",
            "chain_name": "ethereum",
            "chain_id": 1,
            "amount": 9419186.975963663,
            "usd_value": 416047.46675758,
            "usd_price": 0.04417021,
            "is_gas_token": False,
            "token_address": "0x9f52c8ecbee10e00d9faaac5ee9ba0ff6550f511",
            "escrow_address": "0xa2c1c1a473250094a6244f2bcf6cb51f670ad3ac",
            "escrow_name": "L1StandardBridge",
            "is_shared_escrow": False,
            "escrow_url": "https://etherscan.io/address/0xA2C1C1A473250094a6244F2bcf6Cb51F670Ad3aC",
            "asset_url": "https://etherscan.io/address/0x9F52c8ecbEe10e00D9faaAc5Ee9Ba0fF6550F511",
            "icon_url": "https://coin-images.coingecko.com/coins/images/21070/large/SipherToken.png?1696520453",
            "symbol": "SIPHER",
            "name": "Sipher Token",
            "supply": "zero",
            "category": "canonical",
        },
        {
            "dt": "2025-04-08",
            "project_id": "funki",
            "project_slug": "funki",
            "timestamp": 1744124400,
            "asset_id": "ethereum-native",
            "chain_name": "ethereum",
            "chain_id": 1,
            "amount": 72.52974917119296,
            "usd_value": 111459.32322348957,
            "usd_price": 1536.7394,
            "is_gas_token": True,
            "token_address": None,
            "escrow_address": "0x5c9c7f98ed153a2deaa981eb5c97b31744accf22",
            "escrow_name": "OptimismPortal",
            "is_shared_escrow": False,
            "escrow_url": "https://etherscan.io/address/0x5C9C7f98eD153a2deAA981eB5C97B31744AccF22",
            "asset_url": None,
            "icon_url": "https://assets.coingecko.com/coins/images/279/large/ethereum.png?1595348880",
            "symbol": "ETH",
            "name": "Ether",
            "supply": "zero",
            "category": "canonical",
        },
        {
            "dt": "2025-04-08",
            "project_id": "funki",
            "project_slug": "funki",
            "timestamp": 1744124400,
            "asset_id": "ethereum-0xdAC17F958D2ee523a2206206994597C13D831ec7",
            "chain_name": "ethereum",
            "chain_id": 1,
            "amount": 47.307409,
            "usd_value": 47.2901843723831,
            "usd_price": 0.9996359,
            "is_gas_token": False,
            "token_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "escrow_address": "0xa2c1c1a473250094a6244f2bcf6cb51f670ad3ac",
            "escrow_name": "L1StandardBridge",
            "is_shared_escrow": False,
            "escrow_url": "https://etherscan.io/address/0xA2C1C1A473250094a6244F2bcf6Cb51F670Ad3aC",
            "asset_url": "https://etherscan.io/address/0xdAC17F958D2ee523a2206206994597C13D831ec7",
            "icon_url": "https://assets.coingecko.com/coins/images/325/large/Tether.png?1696501661",
            "symbol": "USDT",
            "name": "Tether USD",
            "supply": "zero",
            "category": "canonical",
        },
        {
            "dt": "2025-04-08",
            "project_id": "funki",
            "project_slug": "funki",
            "timestamp": 1744124400,
            "asset_id": "ethereum-0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "chain_name": "ethereum",
            "chain_id": 1,
            "amount": 19.1,
            "usd_value": 19.098713615,
            "usd_price": 0.99993265,
            "is_gas_token": False,
            "token_address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "escrow_address": "0xa2c1c1a473250094a6244f2bcf6cb51f670ad3ac",
            "escrow_name": "L1StandardBridge",
            "is_shared_escrow": False,
            "escrow_url": "https://etherscan.io/address/0xA2C1C1A473250094a6244F2bcf6Cb51F670Ad3aC",
            "asset_url": "https://etherscan.io/address/0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "icon_url": "https://assets.coingecko.com/coins/images/6319/large/usdc.png?1696506694",
            "symbol": "USDC",
            "name": "USD Coin",
            "supply": "zero",
            "category": "canonical",
        },
    ]


def test_parse_tvs_zkfair():
    # https://l2beat.com/api/scaling/tvs/zkfair/breakdown
    response_data = {
        "success": True,
        "data": {
            "dataTimestamp": 1744124400,
            "breakdown": {
                "canonical": [
                    {
                        "assetId": "ethereum-0xe4815AE53B124e7263F08dcDBBB757d41Ed658c6",
                        "chain": {"name": "ethereum", "id": 1},
                        "amount": 11547.4375,
                        "usdValue": 29.096637164725003,
                        "usdPrice": "0.0025197484",
                        "tokenAddress": "0xe4815AE53B124e7263F08dcDBBB757d41Ed658c6",
                        "escrows": [
                            {
                                "amount": 11547.4375,
                                "usdValue": 29.096637164725003,
                                "escrowAddress": "0x9cb4706e20A18E59a48ffa7616d700A3891e1861",
                                "name": "Bridge",
                                "isSharedEscrow": False,
                                "url": "https://etherscan.io/address/0x9cb4706e20A18E59a48ffa7616d700A3891e1861",
                            }
                        ],
                        "iconUrl": "https://assets.coingecko.com/coins/images/13585/large/image_2024-01-16_172847810.png?1705397359",
                        "symbol": "ZKS",
                        "name": "Zks",
                        "url": "https://etherscan.io/address/0xe4815AE53B124e7263F08dcDBBB757d41Ed658c6",
                        "supply": "zero",
                    }
                ],
                "native": [
                    {
                        "assetId": "zkfair-0x1cD3E2A23C45A690a18Ed93FD1412543f464158F",
                        "chain": {"name": "zkfair", "id": 42766},
                        "amount": 9886400000,
                        "usdValue": 832179.2176959999,
                        "usdPrice": "0.00008417414",
                        "tokenAddress": "0x1cD3E2A23C45A690a18Ed93FD1412543f464158F",
                        "iconUrl": "https://assets.coingecko.com/coins/images/34288/large/r8A3J3kf_400x400.jpg?1704455147",
                        "symbol": "ZKF",
                        "name": "ZKF",
                        "url": "https://scan.zkfair.io/address/0x1cD3E2A23C45A690a18Ed93FD1412543f464158F",
                        "supply": "circulatingSupply",
                    }
                ],
                "external": [],
            },
        },
    }

    actual = clean_dataframe(
        parse_tvs(response_data, L2BeatProject(id="funki", slug="funki"))
    ).to_dicts()
    assert actual == [
        {
            "dt": "2025-04-08",
            "project_id": "funki",
            "project_slug": "funki",
            "timestamp": 1744124400,
            "asset_id": "ethereum-0xe4815AE53B124e7263F08dcDBBB757d41Ed658c6",
            "chain_name": "ethereum",
            "chain_id": 1,
            "amount": 11547.4375,
            "usd_value": 29.096637164725003,
            "usd_price": 0.0025197484,
            "is_gas_token": None,
            "token_address": "0xe4815ae53b124e7263f08dcdbbb757d41ed658c6",
            "escrow_address": "0x9cb4706e20a18e59a48ffa7616d700a3891e1861",
            "escrow_name": "Bridge",
            "is_shared_escrow": False,
            "escrow_url": "https://etherscan.io/address/0x9cb4706e20A18E59a48ffa7616d700A3891e1861",
            "asset_url": "https://etherscan.io/address/0xe4815AE53B124e7263F08dcDBBB757d41Ed658c6",
            "icon_url": "https://assets.coingecko.com/coins/images/13585/large/image_2024-01-16_172847810.png?1705397359",
            "symbol": "ZKS",
            "name": "Zks",
            "supply": "zero",
            "category": "canonical",
        },
        {
            "dt": "2025-04-08",
            "project_id": "funki",
            "project_slug": "funki",
            "timestamp": 1744124400,
            "asset_id": "zkfair-0x1cD3E2A23C45A690a18Ed93FD1412543f464158F",
            "chain_name": "zkfair",
            "chain_id": 42766,
            "amount": 9886400000.0,
            "usd_value": 832179.2176959999,
            "usd_price": 8.417414e-05,
            "is_gas_token": None,
            "token_address": "0x1cd3e2a23c45a690a18ed93fd1412543f464158f",
            "escrow_address": None,
            "escrow_name": None,
            "is_shared_escrow": None,
            "escrow_url": None,
            "asset_url": "https://scan.zkfair.io/address/0x1cD3E2A23C45A690a18Ed93FD1412543f464158F",
            "icon_url": "https://assets.coingecko.com/coins/images/34288/large/r8A3J3kf_400x400.jpg?1704455147",
            "symbol": "ZKF",
            "name": "ZKF",
            "supply": "circulatingSupply",
            "category": "native",
        },
    ]


def test_parse_tvs_zkfair_with_updated_l2beat_json():
    # https://l2beat.com/api/scaling/tvs/zkfair/breakdown
    response_data = {
        "success": True,
        "data": {
            "dataTimestamp": 1745323200,
            "breakdown": {
                "canonical": [
                    {
                        "mode": "auto",
                        "id": "zkfair-ZKS",
                        "priceId": "zkspace",
                        "symbol": "ZKS",
                        "name": "Zks",
                        "iconUrl": "https://assets.coingecko.com/coins/images/13585/large/image_2024-01-16_172847810.png?1705397359",
                        "amount": 11547.44,
                        "category": "other",
                        "source": "canonical",
                        "isAssociated": False,
                        "address": {
                            "address": "0xe4815AE53B124e7263F08dcDBBB757d41Ed658c6",
                            "url": "https://etherscan.io/address/0xe4815AE53B124e7263F08dcDBBB757d41Ed658c6",
                        },
                        "formula": {
                            "type": "balanceOfEscrow",
                            "chain": "ethereum",
                            "sinceTimestamp": 1702879283,
                            "address": "0xe4815AE53B124e7263F08dcDBBB757d41Ed658c6",
                            "decimals": 18,
                            "escrowAddress": "0x9cb4706e20A18E59a48ffa7616d700A3891e1861",
                        },
                        "usdValue": 18.08,
                        "escrow": {
                            "address": "0x9cb4706e20A18E59a48ffa7616d700A3891e1861",
                            "url": "https://etherscan.io/address/0x9cb4706e20A18E59a48ffa7616d700A3891e1861",
                            "name": "OldBridge",
                        },
                    }
                ],
                "external": [],
                "native": [
                    {
                        "mode": "auto",
                        "id": "zkfair-ZKF",
                        "priceId": "zkfair",
                        "symbol": "ZKF",
                        "name": "ZKF",
                        "iconUrl": "https://assets.coingecko.com/coins/images/34288/large/r8A3J3kf_400x400.jpg?1704455147",
                        "amount": 9923400000,
                        "category": "other",
                        "source": "native",
                        "isAssociated": True,
                        "address": {
                            "address": "0x1cD3E2A23C45A690a18Ed93FD1412543f464158F",
                            "url": "https://scan.zkfair.io/address/0x1cD3E2A23C45A690a18Ed93FD1412543f464158F",
                        },
                        "formula": {
                            "type": "circulatingSupply",
                            "sinceTimestamp": 1704412800,
                            "apiId": "zkfair",
                            "decimals": 18,
                            "address": "0x1cD3E2A23C45A690a18Ed93FD1412543f464158F",
                            "chain": "zkfair",
                        },
                        "usdValue": 538376.7,
                    }
                ],
            },
        },
    }

    actual = clean_dataframe(
        parse_tvs(response_data, L2BeatProject(id="funki", slug="funki"))
    ).to_dicts()
    assert actual == [
        {
            "dt": "2025-04-22",
            "project_id": "funki",
            "project_slug": "funki",
            "timestamp": 1745323200,
            "asset_id": "zkfair-ZKS",
            "chain_name": "ethereum",
            "chain_id": None,
            "amount": 11547.44,
            "usd_value": 18.08,
            "usd_price": None,
            "is_gas_token": None,
            "token_address": "0xe4815ae53b124e7263f08dcdbbb757d41ed658c6",
            "escrow_address": "0x9cb4706e20a18e59a48ffa7616d700a3891e1861",
            "escrow_name": "OldBridge",
            "is_shared_escrow": None,
            "escrow_url": "https://etherscan.io/address/0x9cb4706e20A18E59a48ffa7616d700A3891e1861",
            "asset_url": "https://etherscan.io/address/0xe4815AE53B124e7263F08dcDBBB757d41Ed658c6",
            "icon_url": "https://assets.coingecko.com/coins/images/13585/large/image_2024-01-16_172847810.png?1705397359",
            "symbol": "ZKS",
            "name": "Zks",
            "supply": None,
            "category": "canonical",
        },
        {
            "dt": "2025-04-22",
            "project_id": "funki",
            "project_slug": "funki",
            "timestamp": 1745323200,
            "asset_id": "zkfair-ZKF",
            "chain_name": "zkfair",
            "chain_id": None,
            "amount": 9923400000.0,
            "usd_value": 538376.7,
            "usd_price": None,
            "is_gas_token": None,
            "token_address": "0x1cd3e2a23c45a690a18ed93fd1412543f464158f",
            "escrow_address": None,
            "escrow_name": None,
            "is_shared_escrow": None,
            "escrow_url": None,
            "asset_url": "https://scan.zkfair.io/address/0x1cD3E2A23C45A690a18Ed93FD1412543f464158F",
            "icon_url": "https://assets.coingecko.com/coins/images/34288/large/r8A3J3kf_400x400.jpg?1704455147",
            "symbol": "ZKF",
            "name": "ZKF",
            "supply": None,
            "category": "native",
        },
    ]
