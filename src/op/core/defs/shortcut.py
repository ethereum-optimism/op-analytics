from typing import Type, Iterable

from .product_contract import ProductContract
from .source_binding import SourceBinding


def required_from_source(
    sb: SourceBinding,
    *,
    row_type: Type,
    required_fields: Iterable[str] = (),
) -> ProductContract:
    return ProductContract(
        product=sb.product,
        row_type=row_type,
        required_fields=tuple(required_fields),
    )