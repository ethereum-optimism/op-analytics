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


def surrounding_dates(dateval: date) -> list[date]:
    day_before = dateval - timedelta(days=1)
    day_after = dateval + timedelta(days=1)

    return [day_before, dateval, day_after]


def epoch_is_date(epoch: int) -> bool:
    """Return true if seconds since epoch is at exactly midnight."""
    return epoch % 86400 == 0
