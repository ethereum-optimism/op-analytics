from dataclasses import dataclass, field


from op_analytics.coreutils.logger import structlog
from op_analytics.datapipeline.models.compute.model import PythonModel
from op_analytics.datapipeline.etl.ingestion.reader.rootpaths import RootPath

log = structlog.get_logger()


@dataclass
class ModelsDataSpec:
    models: list[str]
    root_path_prefix: str

    input_root_paths: list[RootPath] = field(init=False, default_factory=list)
    output_root_paths: list[RootPath] = field(init=False, default_factory=list)

    def __post_init__(self):
        for _ in self.models:
            model: PythonModel = PythonModel.get(_)

            for output_dataset in model.expected_output_datasets:
                self.output_root_paths.append(
                    RootPath.of(root_path=f"{self.root_path_prefix}/{model.name}/{output_dataset}")
                )

            for input_dataset in model.input_datasets:
                self.input_root_paths.append(RootPath.of(input_dataset))
