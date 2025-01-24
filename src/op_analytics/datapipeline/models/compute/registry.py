from typing import Any, Callable

from op_analytics.coreutils.logger import bound_contextvars, structlog

from .model import ModelFunction, ModelPath, PythonModel

log = structlog.get_logger()

# All model functions should be defined in modules under this prefix:
PACKAGE_PREFIX = "op_analytics.datapipeline.models.code."


def register_model(
    input_datasets: list[str],
    expected_outputs: list[str],
    auxiliary_templates: list[str],
) -> Callable[[ModelFunction], Any]:
    def decorator(func):
        function_name = func.__name__

        with bound_contextvars(model=function_name):
            # Ensure all models are defined under the "code" package.
            assert str(func.__module__).startswith(PACKAGE_PREFIX)

            # Instantiating the model registers it on the PythonModel
            # instance registry.
            PythonModel(
                path=ModelPath(
                    module=str(func.__module__).removeprefix(PACKAGE_PREFIX),
                    function_name=function_name,
                ),
                input_datasets=input_datasets,
                expected_output_datasets=expected_outputs,
                auxiliary_templates=auxiliary_templates,
                model_func=func,
            )
        return func

    return decorator
