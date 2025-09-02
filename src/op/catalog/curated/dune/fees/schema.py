from dataclasses import dataclass
from datetime import date

@dataclass(frozen=True)
class Record:
    dt_day: date
    chain_key: str
    blockchain: str
    display_name: str
    chain_id: int | None
    txs_per_day: int
    blocks_per_day: int
    median_tx_fee_usd: float | None
    tx_fee_usd: float | None
