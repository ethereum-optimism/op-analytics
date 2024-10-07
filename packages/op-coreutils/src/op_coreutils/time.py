from datetime import datetime, timezone


def now():
    """Returns a naive datetime object with the current UTC timestamp"""

    return datetime.now(timezone.utc).replace(tzinfo=None)


def now_seconds():
    """Returns a naive datetime object with the current UTC timestamp in seconds resolution."""

    return now().replace(microsecond=0)


def now_dt() -> str:
    return now_seconds().strftime("%Y-%m-%d")
