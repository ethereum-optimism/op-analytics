from dataclasses import dataclass
from .product_contract import ProductContract


@dataclass(frozen=True)
class Port:
    name: str
    contract: ProductContract
