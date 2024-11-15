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


def dt_fromepoch(epoch: int) -> str:
    return datetime_fromepoch(epoch).strftime("%Y-%m-%d")


def datetime_fromepoch(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).replace(tzinfo=None)


def date_toepoch(dateval: date) -> int:
    dtval = datetime(year=dateval.year, month=dateval.month, day=dateval.day, tzinfo=timezone.utc)
    return int(dtval.timestamp())


def date_fromstr(val: str) -> date:
    return date.fromisoformat(val)


def surrounding_dates(dateval: date) -> list[date]:
    day_before = dateval - timedelta(days=1)
    day_after = dateval + timedelta(days=1)

    return [day_before, dateval, day_after]
