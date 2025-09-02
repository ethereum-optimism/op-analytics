from dataclasses import dataclass
from typing import Dict, Union, List

from .location import Location, FileLocation, TableLocation, MultiLocation
from .env import EnvProfile
from .product_ref import ProductRef


@dataclass
class Catalog:
    entries: Dict[ProductRef, Union[Location, MultiLocation]]

    def resolve_write_location(self, env: EnvProfile, product: ProductRef) -> Location:
        loc = self.entries.get(product)
        if loc is None:
            raise KeyError(f"No catalog entry for {product.id}")
        return loc.primary if isinstance(loc, MultiLocation) else loc

    def resolve_read_locations(self, env: EnvProfile, product: ProductRef) -> List[Location]:
        loc = self.entries.get(product)
        if loc is None:
            raise KeyError(f"No catalog entry for {product.id}")
        if isinstance(loc, MultiLocation):
            return [loc.primary, *loc.legacy]
        return [loc]