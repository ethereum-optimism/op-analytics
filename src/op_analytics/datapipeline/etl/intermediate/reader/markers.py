from op_analytics.coreutils.logger import structlog
from op_analytics.datapipeline.models.compute.markers import ModelsDataSpec

log = structlog.get_logger()


INTERMEDIATE_MARKERS_TABLE = "intermediate_model_markers"


def make_data_spec(chains: list[str], models: list[str]) -> ModelsDataSpec:
    return ModelsDataSpec(
        chains=chains,
        models=models,
        markers_table=INTERMEDIATE_MARKERS_TABLE,
        root_path_prefix="intermediate",
    )
