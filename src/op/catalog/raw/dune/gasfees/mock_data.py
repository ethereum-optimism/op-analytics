from datetime import date, timedelta
from typing import Dict, Iterable, Any

from .config import Config

_CHAIN_IDS: Dict[str, int] = {
    "ethereum": 1,
    "optimism": 10,
    "arbitrum": 42161,
    "base": 8453,
    "polygon": 137,
    "bsc": 56,
}

_DISPLAY_NAMES: Dict[str, str] = {
    "ethereum": "Ethereum",
    "optimism": "Optimism",
    "arbitrum": "Arbitrum",
    "base": "Base",
    "polygon": "Polygon",
    "bsc": "BNB Chain",
}

def _chains_for(cfg: Config) -> list[str]:
    if cfg.single_chain and cfg.single_chain != "none":
        return [cfg.single_chain]
    return ["optimism", "ethereum", "arbitrum"]

def generate(cfg: Config) -> Iterable[Dict[str, Any]]:
    """
    Produce deterministic dummy rows that match schema.Record.
    Respects trailing_days, ending_days, and single_chain.
    """
    end: date = date.today() - timedelta(days=cfg.ending_days)
    start: date = end - timedelta(days=cfg.trailing_days)
    chains = _chains_for(cfg)

    d = start
    while d <= end:
        for chain in chains:
            # Pseudo-deterministic daily fee in USD
            base = abs(hash((chain, d.toordinal()))) % 500_000  # cents-ish
            tx_fee_usd = (50_00 + base) / 100.0  # $50.00 + noise
            chain_l = chain.lower()
            yield {
                "blockchain": chain,
                "dt": d,
                "tx_fee_usd": float(tx_fee_usd),
                "chain_id": _CHAIN_IDS.get(chain_l, 0),
                "display_name": _DISPLAY_NAMES.get(chain_l, chain.title()),
            }
        d += timedelta(days=1)
