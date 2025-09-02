from datetime import date, timedelta
from typing import Iterable

def generate(req) -> Iterable[dict]:
    """
    Emit a few deterministic dummy rows for testing.
    Expected fields are aligned with the SQL query output.
    """
    anchor = getattr(req, "anchor_day", date.today())
    # Produce 3 days of sample data around the anchor
    chains = [
        ("ethereum", 1, "ETH"),
        ("optimism", 10, "ETH"),
        ("arbitrum", 42161, "ETH"),
    ]
    for d in range(3):
        dt = anchor - timedelta(days=d)
        for name, chain_id, currency in chains:
            yield {
                "blockchain": name,
                "dt": dt.isoformat(),
                "display_name": name.capitalize(),
                "chain_id": chain_id,
                "tx_fee_currency": currency,
                "num_txs": 1000 + d,
                "num_blocks": 7200 - d,
                "tx_fee_native": 12.34 + d,
                "tx_fee_usd": 3456.78 + d * 10,
                "median_tx_fee_native": 0.00042 + d * 0.00001,
                "median_tx_fee_usd": 0.95 + d * 0.01,
            }
