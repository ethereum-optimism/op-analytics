from datetime import datetime, timezone
from op_analytics.coreutils.time import dt_fromepoch, datetime_fromepoch, parse_isoformat


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
    actual = parse_isoformat("2024-12-10T15:30:00Z")
    expected = datetime(2024, 12, 10, 15, 30, 0, tzinfo=timezone.utc)
    assert actual == expected, f"Expected {expected}, but got {actual}"


def test_iso_without_z():
    actual = parse_isoformat("2024-12-10T15:30:00")
    expected = datetime(2024, 12, 10, 15, 30, 0, tzinfo=timezone.utc)
    assert actual == expected, f"Expected {expected}, but got {actual}"


def test_invalid_iso_string():
    try:
        parse_isoformat("2024-12-10T15:30:00INVALID")
    except ValueError:
        pass  # Test passes if ValueError is raised
    else:
        assert False, "Expected ValueError for invalid ISO string"


def test_empty_iso_string():
    try:
        parse_isoformat("")
    except ValueError:
        pass  # Test passes if ValueError is raised
    else:
        assert False, "Expected ValueError for empty ISO string"


def test_iso_with_milliseconds():
    actual = parse_isoformat("2024-12-10T15:30:00.123Z")
    expected = datetime(2024, 12, 10, 15, 30, 0, 123000, tzinfo=timezone.utc)
    assert actual == expected, f"Expected {expected}, but got {actual}"
