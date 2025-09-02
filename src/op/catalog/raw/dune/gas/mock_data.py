from datetime import date, timedelta
from typing import Dict, Iterable, Any

from .config import Config

# Minimal mapping for common chains; extend as needed.
_CHAIN_IDS: Dict[str, int] = {
    "ethereum": 1,
    "optimism": 10,
    "arbitrum": 42161,
    "base": 8453,
    "polygon": 137,
    "bsc": 56,
}

def _chains_for(cfg: Config) -> list[str]:
    if cfg.single_chain and cfg.single_chain != "none":
        return [cfg.single_chain]
    # Provide a reasonable default set for dummy data
    return ["optimism", "ethereum", "arbitrum"]

def generate(cfg: Config) -> Iterable[Dict[str, Any]]:
    """
    Produce deterministic-looking dummy rows that match `schema.Record`.
    Respects trailing_days, ending_days, and single_chain.
    """
    end: date = date.today() - timedelta(days=cfg.ending_days)
    start: date = end - timedelta(days=cfg.trailing_days)
    chains = _chains_for(cfg)

    d = start
    while d <= end:
        for chain in chains:
            # Deterministic-ish pseudo values per (chain, day)
            base = abs(hash((chain, d.toordinal()))) % 900_000
            sum_evm_gas_used = 100_000 + base
            yield {
                "blockchain": chain,
                "dt": d,  # date object (matches schema.Record)
                "sum_evm_gas_used": int(sum_evm_gas_used),
                "chain_id": _CHAIN_IDS.get(chain.lower(), 0),
            }
        d += timedelta(days=1)
