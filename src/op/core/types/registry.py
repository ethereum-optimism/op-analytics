from dataclasses import dataclass
from typing import Dict, List, Union

from .location import Location, MultiLocation
from .env import EnvProfile
from .product_ref import ProductRef


@dataclass(frozen=True)
class ProductRegistration:
    product: ProductRef
    by_env: Dict[str, Union[Location, MultiLocation]]  # env.name -> location(s)

@dataclass
class ProductRegistry:
    _entries: Dict[ProductRef, ProductRegistration]

    def register(self, reg: ProductRegistration) -> None:
        self._entries[reg.product] = reg

    # “Catalog-like” helpers, but ergonomically attached to products:
    def resolve_write_location(self, env: EnvProfile, product: ProductRef) -> Location:
        loc = self._entries[product].by_env[env.name]
        return loc.primary if isinstance(loc, MultiLocation) else loc

    def resolve_read_locations(self, env: EnvProfile, product: ProductRef) -> List[Location]:
        loc = self._entries[product].by_env[env.name]
        if isinstance(loc, MultiLocation):
            return [loc.primary, *loc.legacy]
        return [loc]
