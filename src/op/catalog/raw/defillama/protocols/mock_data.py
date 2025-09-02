from typing import Iterable, Dict, Any


def generate(_req=None) -> Iterable[Dict[str, Any]]:
    yield {
        "id": "2269",
        "name": "Aave",
        "symbol": "AAVE",
        "category": "Lending",
        "chains": ["Ethereum", "Polygon"],
        "tvl": 5200000000.0,
        "chainTvls": {"Ethereum": 3200000000.0, "Polygon": 2000000000.0},
        "change_1d": 2.1,
        "change_7d": -5.3,
    }
    yield {
        "id": "9999",
        "name": "Velodrome",
        "symbol": "VELO",
        "category": "DEX",
        "chains": ["Optimism"],
        "tvl": 123456789.0,
        "chainTvls": {"Optimism": 123456789.0},
        "change_1d": 0.4,
        "change_7d": 1.2,
    }
