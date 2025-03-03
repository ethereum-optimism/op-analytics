from dataclasses import dataclass, field


from op_analytics.coreutils.logger import structlog
from op_analytics.datapipeline.models.compute.model import PythonModel
from op_analytics.datapipeline.etl.ingestion.reader.rootpaths import RootPath

log = structlog.get_logger()


@dataclass
@dataclass
class ModelsDataSpec:
    models: list[str]
    root_path_prefix: str

    input_root_paths: list[RootPath] = field(init=False, default_factory=list)
    output_root_paths: list[RootPath] = field(init=False, default_factory=list)

    model_graph: "ModelGraph" = field(init=False)

    def execution_passes(self) -> list[list[str]]:
        """Use the model_graph to return the models to be executed on each pass."""
        return self.model_graph.passes

    def __post_init__(self):
        nodes: list[ModelNode] = []

        for model_name in self.models:
            model: PythonModel = PythonModel.get(model_name)

            model_outputs = []
            for output_dataset in model.expected_output_datasets:
                model_outputs.append(
                    RootPath.of(
                        root_path=f"{self.root_path_prefix}/{model.name}/{output_dataset}",
                    )
                )
            self.output_root_paths.extend(model_outputs)

            for input_dataset in model.input_datasets:
                self.input_root_paths.append(RootPath.of(input_dataset))

            nodes.append(
                ModelNode(
                    name=model_name,
                    inputs=model.input_datasets,
                    outputs=[_.root_path for _ in model_outputs],
                )
            )

        self.model_graph = ModelGraph.of(nodes)


@dataclass
class ModelNode:
    name: str
    inputs: list[str]
    outputs: list[str]


@dataclass
class ModelGraph:
    # The dependency graph of nodes.
    nodes: dict[str, ModelNode]

    # The topologically sorted nodes.
    sorted_nodes: list[str]

    # The execution passes needed to materialize the nodes.
    passes: list[list[str]]

    @classmethod
    def of(cls, nodes: list[ModelNode]):
        """Create a ModelGraph from a list of ModelNodes.

        This method topologically sorts the nodes based on their dependencies
        and organizes them into execution passes.

        Args:
            nodes: List of ModelNode objects representing the models and their dependencies

        Returns:
            A new ModelGraph instance with the nodes organized into execution passes
        """
        # Create a mapping of output datasets to their producing nodes
        output_to_node = {}
        for node in nodes:
            for output in node.outputs:
                output_to_node[output] = node.name

        # Create a dependency graph
        # node points to its dependencies.
        dependencies: dict[str, set[str]] = {node.name: set() for node in nodes}
        for node in nodes:
            for input_dataset in node.inputs:
                if input_dataset in output_to_node:
                    # This input is produced by another node
                    dependencies[node.name].add(output_to_node[input_dataset])

        # Topologically sort the nodes
        sorted_nodes = []
        visited = set()
        temp_visited = set()

        def visit(node_name):
            if node_name in visited:
                return
            if node_name in temp_visited:
                raise ValueError(f"Cycle detected in model dependencies involving {node_name}")

            temp_visited.add(node_name)
            for dep in dependencies[node_name]:
                visit(dep)

            temp_visited.remove(node_name)
            visited.add(node_name)
            sorted_nodes.append(node_name)

        for node in nodes:
            if node.name not in visited:
                visit(node.name)

        # Organize nodes into execution passes
        passes = []
        remaining_nodes = set(sorted_nodes)

        while remaining_nodes:
            current_pass = []
            for node_name in sorted_nodes:
                if node_name not in remaining_nodes:
                    continue

                # Check if all dependencies are satisfied
                if all(dep not in remaining_nodes for dep in dependencies[node_name]):
                    current_pass.append(node_name)

            if not current_pass:
                raise ValueError("Unable to organize nodes into passes, possible cycle detected")

            passes.append(current_pass)
            remaining_nodes -= set(current_pass)

        # Create and return a new ModelGraph instance
        graph: "ModelGraph" = cls(
            nodes={node.name: node for node in nodes},
            sorted_nodes=sorted_nodes,
            passes=passes,
        )

        return graph
