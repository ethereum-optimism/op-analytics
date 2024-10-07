import logging
import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars
from structlog.typing import EventDict

from op_coreutils.env.aware import current_environment

CURRENT_ENV = current_environment().name


def add_oplabs_env(logger: logging.Logger, method_name: str, event_dict: EventDict) -> EventDict:
    event_dict["oplabs"] = CURRENT_ENV

    return event_dict


structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        add_oplabs_env,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.NOTSET),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=False,
)

__all__ = ["bind_contextvars", "clear_contextvars"]


def numtext(number):
    if number.is_integer():
        return f"{int(number)}"
    else:
        return f"{number:3.1f}"


def human_size(size_bytes, suffix="B"):
    """Human-readable file sizes."""
    for unit in ("", "K", "M", "G", "T", "P", "E", "Z"):
        if abs(size_bytes) < 1000.0:
            return f"{numtext(size_bytes)}{unit}{suffix}"
        size_bytes /= 1000.0
    return f"{size_bytes:.1f}Yi{suffix}"


def human_rows(num_rows):
    return human_size(num_rows, suffix="rows")
