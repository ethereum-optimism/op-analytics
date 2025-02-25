from decimal import Decimal

from op_analytics.datasources.defillama.stablecoins.stablecoin import safe_decimal


def test():
    assert safe_decimal(841651352.651346) == Decimal("841651352.651346")
    assert safe_decimal(9066541.887213) == Decimal("9066541.887213")
    assert safe_decimal(555500) == Decimal("555500")
    assert safe_decimal(None) is None
