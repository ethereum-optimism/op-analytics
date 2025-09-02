from dataclasses import dataclass
from datetime import date

@dataclass(frozen=True)
class Record:
    dt_day: date
    chain_key: str
    txs_per_day: int
    blocks_per_day: int
    median_tx_fee_usd: float
    tx_fee_usd: float
    sum_evm_gas_used: float
    sum_evm_gas_used_per_second: float
