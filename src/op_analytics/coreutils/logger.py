import logging
import resource

import orjson
import structlog
from structlog.contextvars import bind_contextvars, bound_contextvars, clear_contextvars
from structlog.typing import EventDict
from op_analytics.coreutils.time import now
from op_analytics.coreutils.env.aware import current_environment, is_k8s

CURRENT_ENV = current_environment().name


def add_oplabs_env(logger: logging.Logger, method_name: str, event_dict: EventDict) -> EventDict:
    if CURRENT_ENV != "UNDEFINED":
        event_dict["env"] = CURRENT_ENV

    return event_dict


def pass_through(logger: logging.Logger, method_name: str, event_dict: EventDict) -> EventDict:
    return event_dict


# If needed for debugging adding this processor will provide filename and line for the log.
CALLSITE_PARAMETERS = structlog.processors.CallsiteParameterAdder(
    [
        structlog.processors.CallsiteParameter.FILENAME,
        structlog.processors.CallsiteParameter.LINENO,
    ]
)


def configuration():
    if is_k8s():
        return dict(
            processors=[
                # CALLSITE_PARAMETERS,
                structlog.contextvars.merge_contextvars,
                structlog.processors.add_log_level,
                structlog.dev.set_exc_info,
                structlog.processors.TimeStamper(fmt="iso", utc=True),
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
                structlog.dev.ConsoleRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(),
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


class ProgressTracker:
    """
    Tracks progress and ETA for a batch of tasks.
    Instead of logging a separate "Task progress" line, we bind context variables
    so that when the wrapped function logs, the ETA and progress fields are included
    in that log line.
    """

    def __init__(self, total_tasks: int):
        self.total_tasks = total_tasks
        self.completed_tasks = 0
        self.start_time = now()

    def wrap(self, func, current_index: int):
        def wrapper(target_item):
            self.completed_tasks += 1
            elapsed = (now() - self.start_time).total_seconds()
            avg_time_per_task = elapsed / self.completed_tasks
            remaining_tasks = self.total_tasks - self.completed_tasks
            eta = remaining_tasks * avg_time_per_task

            # Bind progress and ETA as context vars, so when func logs, these fields appear
            with bound_contextvars(
                target_id=f"{current_index:03d}/{self.total_tasks:03d}",
                completed=self.completed_tasks,
                total=self.total_tasks,
                elapsed=f"{elapsed:.1f}s",
                eta=f"{eta:.1f}s",
            ):
                return func(target_item)

        return wrapper
