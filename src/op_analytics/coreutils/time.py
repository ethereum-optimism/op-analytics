from datetime import date, datetime, timedelta, timezone


def now() -> datetime:
    """Returns a naive datetime object with the current UTC timestamp"""

    return datetime.now(timezone.utc).replace(tzinfo=None)


def now_trunc() -> datetime:
    """Returns a naive datetime object with the current UTC timestamp truncated to the nearest second."""

    return now().replace(microsecond=0)


def now_dt() -> str:
    """Return the date value in YYYY-MM-DD format for the current time."""
    return now_trunc().strftime("%Y-%m-%d")


def now_date() -> date:
    """Return the current date."""
    return now_trunc().date()


def now_friendly_timestamp() -> str:
    """Return the current time as a string that is ok to use in filenames."""
    return now_trunc().strftime("%Y%m%dT%H%M%S")


def dt_fromepoch(epoch: int) -> str:
    return datetime_fromepoch(epoch).strftime("%Y-%m-%d")


def datetime_fromepoch(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).replace(tzinfo=None)


def date_toepoch(dateval: date) -> int:
    dtval = datetime(year=dateval.year, month=dateval.month, day=dateval.day, tzinfo=timezone.utc)
    return int(dtval.timestamp())


def datetime_toepoch(datetimeval: datetime) -> int:
    assert datetimeval.tzinfo is None
    return int(datetimeval.replace(tzinfo=timezone.utc).timestamp())


def datetime_fromdate(dateval: date) -> datetime:
    return datetime(year=dateval.year, month=dateval.month, day=dateval.day, tzinfo=None)


def date_fromstr(val: str) -> date:
    return date.fromisoformat(val)


def datetime_fromdt(dtval: str) -> datetime:
    return datetime_fromdate(date_fromstr(dtval))


def date_tostr(val: date) -> str:
    return val.strftime("%Y-%m-%d")


def surrounding_dates(dateval: date, minus_delta: int = 1, plus_delta: int = 1) -> list[date]:
    day_before = dateval - timedelta(days=minus_delta)
    day_after = dateval + timedelta(days=plus_delta)

    result = []
    val = day_before
    while val <= day_after:
        result.append(val)
        val += timedelta(days=1)

    return result


def epoch_is_date(epoch: int) -> bool:
    """Return true if seconds since epoch is at exactly midnight."""
    return epoch % 86400 == 0


def parse_isoformat(iso_string: str) -> datetime:
    """
    Parse an ISO 8601 formatted string into a naive datetime object in UTC.
    If the string has a timezone, convert it to UTC. If it doesn't, assume it's already UTC.

    Examples:
        parse_isoformat("2024-12-10T15:30:00Z") -> datetime(2024, 12, 10, 15, 30, 0)
        parse_isoformat("2024-12-10T15:30:00+02:00") -> datetime(2024, 12, 10, 13, 30, 0)
        parse_isoformat("2024-12-10T15:30:00") -> datetime(2024, 12, 10, 15, 30, 0)
    """
    parsed_datetime = datetime.fromisoformat(iso_string)

    if parsed_datetime.tzinfo is not None:
        # Convert to UTC and remove timezone
        parsed_datetime = parsed_datetime.astimezone(timezone.utc).replace(tzinfo=None)
    else:
        # Assume naive datetime is already in UTC
        parsed_datetime = parsed_datetime.replace(tzinfo=None)

    return parsed_datetime
