import logging
import resource
from op_analytics.coreutils.time import now
import sys

import orjson
import structlog
from structlog.contextvars import bind_contextvars, bound_contextvars, clear_contextvars
from structlog.typing import EventDict

from op_analytics.coreutils.env.aware import current_environment, is_k8s

CURRENT_ENV = current_environment().name

# Global variables to track ETA
start_time = None
total_tasks = None


def add_oplabs_env(logger: logging.Logger, method_name: str, event_dict: EventDict) -> EventDict:
    if CURRENT_ENV != "UNDEFINED":
        event_dict["env"] = CURRENT_ENV
    return event_dict


def pass_through(logger: logging.Logger, method_name: str, event_dict: EventDict) -> EventDict:
    return event_dict


CALLSITE_PARAMETERS = structlog.processors.CallsiteParameterAdder(
    [
        structlog.processors.CallsiteParameter.FILENAME,
        structlog.processors.CallsiteParameter.LINENO,
    ]
)


def human_readable_time(seconds: int) -> str:
    """Convert seconds into a human-readable format like '5m 34s'."""
    minutes, secs = divmod(seconds, 60)
    if minutes > 0:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def eta_processor(logger: logging.Logger, method_name: str, event_dict: EventDict) -> EventDict:
    """
    Adds ETA estimation to log messages containing `target_id` and "Fetched from".
    Resets tracking variables when a new session or batch is detected.
    """
    global start_time, total_tasks

    target_id = event_dict.get("target_id")
    event_msg = event_dict.get("event", "")

    if target_id and "Fetched from" in event_msg:
        try:
            current_str, total_str = target_id.split("/")
            current_num = int(current_str)
            total_num = int(total_str)
        except ValueError:
            # If parsing fails, just return without modification
            return event_dict

        if total_tasks is None or total_tasks != total_num:
            start_time = now()
            total_tasks = total_num

        elapsed_time = now() - start_time
        avg_time_per_task = elapsed_time / current_num if current_num > 0 else 0
        remaining_time = avg_time_per_task * (total_num - current_num)
        eta = human_readable_time(int(remaining_time))

        event_dict["event"] = f"{event_msg} (ETA: {eta})"

    return event_dict


def configuration():
    if is_k8s():
        return dict(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.processors.add_log_level,
                structlog.dev.set_exc_info,
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                eta_processor,
                structlog.processors.JSONRenderer(serializer=orjson.dumps),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
            context_class=dict,
            logger_factory=structlog.BytesLoggerFactory(),
            cache_logger_on_first_use=True,
        )
    else:
        return dict(
            processors=[
                CALLSITE_PARAMETERS,
                structlog.contextvars.merge_contextvars,
                add_oplabs_env,
                structlog.processors.add_log_level,
                structlog.processors.StackInfoRenderer(),
                structlog.dev.set_exc_info,
                structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
                eta_processor,
                structlog.dev.ConsoleRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
            cache_logger_on_first_use=False,
        )


structlog.configure(**configuration())

__all__ = ["structlog", "bind_contextvars", "clear_contextvars", "bound_contextvars"]


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


def human_interval(num_seconds: int) -> str:
    if num_seconds < 120:
        return f"{num_seconds} secs"
    elif num_seconds < 7200:
        num_minutes = num_seconds / 60
        return f"{num_minutes:.1f} mins"
    elif num_seconds < 24 * 3600 * 3:
        num_hours = num_seconds / 3600
        return f"{num_hours:.1f} hrs"
    else:
        num_days = num_seconds / (24 * 3600)
        return f"{num_days:.1f} days"


def memory_usage():
    """Return max_rss / 1e6 rounded to make it easier to eyeball."""
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6, 2)
