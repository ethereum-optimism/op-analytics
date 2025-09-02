from dataclasses import dataclass
from datetime import date

@dataclass(frozen=True)
class Record:
    blockchain: str
    dt: date
    sum_evm_gas_used: int
    chain_id: int
