from datetime import datetime, date
from op_analytics.coreutils.time import (
    dt_fromepoch,
    datetime_fromepoch,
    parse_isoformat,
    surrounding_dates,
    datestr_subtract,
)


def test_epoch_dt():
    actual = dt_fromepoch(0)
    assert actual == "1970-01-01"


def test_epoch_datetime():
    actual = datetime_fromepoch(0)
    assert actual == datetime(1970, 1, 1)


def test_epoch_dt_01():
    actual = dt_fromepoch(1729797569)
    assert actual == "2024-10-24"


def test_epoch_datetime_01():
    actual = datetime_fromepoch(1729797569)
    assert actual == datetime(2024, 10, 24, 19, 19, 29)


def test_iso_with_z():
    # Z suffix should be interpreted as UTC and return a naive UTC datetime
    actual = parse_isoformat("2024-12-10T15:30:00Z")
    expected = datetime(2024, 12, 10, 15, 30, 0)
    assert actual == expected, f"Expected {expected}, but got {actual}"


def test_iso_without_z():
    # No timezone info should be treated as UTC
    actual = parse_isoformat("2024-12-10T15:30:00")
    expected = datetime(2024, 12, 10, 15, 30, 0)
    assert actual == expected, f"Expected {expected}, but got {actual}"


def test_iso_with_offset():
    # Convert 15:30:00+02:00 to UTC -> 13:30:00 UTC
    actual = parse_isoformat("2024-12-10T15:30:00+02:00")
    expected = datetime(2024, 12, 10, 13, 30, 0)
    assert actual == expected, f"Expected {expected}, but got {actual}"


def test_iso_with_milliseconds():
    # Milliseconds should be preserved
    actual = parse_isoformat("2024-12-10T15:30:00.123Z")
    expected = datetime(2024, 12, 10, 15, 30, 0, 123000)
    assert actual == expected, f"Expected {expected}, but got {actual}"


def test_invalid_iso_string():
    # Invalid strings should raise ValueError
    try:
        parse_isoformat("2024-12-10T15:30:00INVALID")
    except ValueError:
        pass
    else:
        assert False, "Expected ValueError for invalid ISO string"


def test_empty_iso_string():
    # Empty strings should raise ValueError
    try:
        parse_isoformat("")
    except ValueError:
        pass
    else:
        assert False, "Expected ValueError for empty ISO string"


def test_surrounding_dates():
    actual1 = surrounding_dates(date(2024, 11, 5))
    assert actual1 == [
        date(2024, 11, 4),
        date(2024, 11, 5),
        date(2024, 11, 6),
    ]

    actual2 = surrounding_dates(date(2024, 11, 5), minus_delta=0)
    assert actual2 == [
        date(2024, 11, 5),
        date(2024, 11, 6),
    ]


def test_datestr_subtract_01():
    assert datestr_subtract("2025-01-10", 0) == "2025-01-10"
    assert datestr_subtract("2025-01-10", 3) == "2025-01-07"
    assert datestr_subtract("2025-01-10", 12) == "2024-12-29"
    assert datestr_subtract("2025-01-10", -4) == "2025-01-14"
