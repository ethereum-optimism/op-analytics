from datetime import date
from unittest.mock import patch

from op_analytics.datasources.dune.utils import determine_lookback


def test_default():
    l0, l1 = determine_lookback()
    assert l0 == 7
    assert l1 == 0


def test_backfill():
    with patch("op_analytics.datasources.dune.utils.now_date") as mock:
        mock.return_value = date(2025, 3, 1)
        l0, l1 = determine_lookback(min_dt="2025-01-01", max_dt="2025-02-01")
        assert l0 == 59
        assert l1 == 28
