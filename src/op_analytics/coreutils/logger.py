import logging
import os
import resource

import orjson
import structlog
from structlog.contextvars import bind_contextvars, bound_contextvars, clear_contextvars
from structlog.typing import EventDict
from op_analytics.coreutils.env.aware import current_environment, is_k8s

CURRENT_ENV = current_environment().name

DAGSTER_RUN_JOB_NAME = os.environ.get("DAGSTER_RUN_JOB_NAME")


def add_oplabs_env(logger: logging.Logger, method_name: str, event_dict: EventDict) -> EventDict:
    if CURRENT_ENV != "UNDEFINED":
        event_dict["env"] = CURRENT_ENV

    return event_dict


def add_dagster_job(logger: logging.Logger, method_name: str, event_dict: EventDict) -> EventDict:
    if DAGSTER_RUN_JOB_NAME is not None:
        event_dict["dagster_job"] = DAGSTER_RUN_JOB_NAME

    return event_dict


def pass_through(logger: logging.Logger, method_name: str, event_dict: EventDict) -> EventDict:
    return event_dict


# If needed for debugging adding this processor will provide filename and line for the log.
CALLSITE_PARAMETERS = structlog.processors.CallsiteParameterAdder(
    [
        structlog.processors.CallsiteParameter.FILENAME,
        structlog.processors.CallsiteParameter.LINENO,
        structlog.processors.CallsiteParameter.PROCESS,
    ]
)


def configuration():
    if is_k8s():
        plain_logs = os.environ.get("PLAIN_LOGS")
        if plain_logs == "true":
            renderer = structlog.dev.ConsoleRenderer(
                exception_formatter=structlog.dev.plain_traceback,
            )
            factory = structlog.WriteLoggerFactory()
        else:
            renderer = structlog.processors.JSONRenderer(
                serializer=orjson.dumps,
            )  # type: ignore
            factory = structlog.BytesLoggerFactory()  # type: ignore

        return dict(
            processors=[
                CALLSITE_PARAMETERS,
                structlog.contextvars.merge_contextvars,
                add_dagster_job,
                structlog.processors.add_log_level,
                structlog.dev.set_exc_info,
                structlog.processors.TimeStamper(
                    fmt="iso",
                    utc=True,
                ),
                renderer,
            ],
            wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
            context_class=dict,
            logger_factory=factory,
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
                structlog.processors.TimeStamper(
                    fmt="%Y-%m-%d %H:%M:%S",
                    utc=False,
                ),
                structlog.dev.ConsoleRenderer(
                    exception_formatter=structlog.dev.plain_traceback,
                ),
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
        return f"{num_seconds:.0f} secs"
    elif num_seconds < 7200:
        num_minutes = num_seconds / 60
        return f"{num_minutes:.1f}m"
    elif num_seconds < 24 * 3600 * 3:
        num_hours = num_seconds / 3600
        return f"{num_hours:.1f}h"
    else:
        num_days = num_seconds / (24 * 3600)
        return f"{num_days:.1f}d"


def memory_usage():
    """Return max_rss / 1e6 rounded to make it easier to eyeball."""
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6, 2)
