import datetime

from op_datasets.utils.daterange import DateRange


def test_daterange():
    dr = DateRange.from_spec("@20241001:20241030")
    assert dr == DateRange(min=datetime.date(2024, 10, 1), max=datetime.date(2024, 10, 30))
    assert len(dr) == 29


def test_blockrange_plus():
    dr = DateRange.from_spec("@20241001:+30")
    assert dr == DateRange(min=datetime.date(2024, 10, 1), max=datetime.date(2024, 10, 31))
    assert len(dr) == 30


def test_epcoh():
    dr = DateRange.from_spec("@20241001:+30")
    assert dr == DateRange(min=datetime.date(2024, 10, 1), max=datetime.date(2024, 10, 31))
    assert dr.min_ts == 1727740800
    assert dr.max_ts == 1730332800
