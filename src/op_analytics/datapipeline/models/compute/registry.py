from typing import Any, Callable

from op_analytics.coreutils.logger import bound_contextvars, structlog

from .model import ModelFunction, ModelPath, PythonModel

log = structlog.get_logger()

# All model functions should be defined in modules under this prefix:
MODULE_PREFIX = "op_analytics.datapipeline.models.code."


def register_model(
    input_datasets: list[str],
    expected_outputs: list[str],
    auxiliary_views: list[str],
) -> Callable[[ModelFunction], Any]:
    def decorator(func):
        function_name = func.__name__

        with bound_contextvars(model=function_name):
            # Instantiating the model registers it on the PythonModel
            # instance registry.
            assert str(func.__module__).startswith(MODULE_PREFIX)

            PythonModel(
                path=ModelPath(
                    module=str(func.__module__).removeprefix(MODULE_PREFIX),
                    function_name=function_name,
                ),
                input_datasets=input_datasets,
                expected_output_datasets=expected_outputs,
                auxiliary_views=auxiliary_views,
                model_func=func,
            )
        return func

    return decorator
