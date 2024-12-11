from op_analytics.coreutils.logger import structlog, bound_contextvars

from .modelexecute import PythonModel
from .querybuilder import TemplatedSQLQuery

log = structlog.get_logger()


def register_model(
    input_datasets: list[str],
    expected_outputs: list[str],
    auxiliary_views: list[TemplatedSQLQuery],
):
    def decorator(func):
        model_name = func.__name__
        with bound_contextvars(model=model_name):
            PythonModel(
                name=model_name,
                input_datasets=input_datasets,
                expected_output_datasets=expected_outputs,
                auxiliary_views=auxiliary_views,
                model_func=func,
            )
        return func

    return decorator
