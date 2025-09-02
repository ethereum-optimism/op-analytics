from dataclasses import dataclass
from datetime import date

@dataclass(frozen=True)
class Record:
    blockchain: str
    dt: date
    tx_fee_usd: float
    chain_id: int
    display_name: str
