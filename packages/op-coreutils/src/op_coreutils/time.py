from datetime import datetime, timezone, date


def now():
    """Returns a naive datetime object with the current UTC timestamp"""

    return datetime.now(timezone.utc).replace(tzinfo=None)


def now_seconds():
    """Returns a naive datetime object with the current UTC timestamp in seconds resolution."""

    return now().replace(microsecond=0)


def now_dt() -> str:
    """Return the date value in YYYY-MM-DD format for the current time."""
    return now_seconds().strftime("%Y-%m-%d")


def dt_fromepoch(epoch: int) -> str:
    return datetime.fromtimestamp(epoch).strftime("%Y-%m-%d")


def date_toepoch(dateval: date) -> int:
    dtval = datetime(year=dateval.year, month=dateval.month, day=dateval.day, tzinfo=timezone.utc)
    return int(dtval.timestamp())
