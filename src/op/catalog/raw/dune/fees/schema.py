from dataclasses import dataclass
from datetime import date

@dataclass(frozen=True)
class Record:
    blockchain: str
    dt: date
    display_name: str
    chain_id: int
    tx_fee_currency: str
    num_txs: int
    num_blocks: int
    tx_fee_native: float
    tx_fee_usd: float
    median_tx_fee_native: float
    median_tx_fee_usd: float
