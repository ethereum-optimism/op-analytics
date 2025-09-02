from datetime import date

from op.core.types.env import EnvProfile
from op.core.types.location import FileLocation
from op.core.types.partition import Partition
from op.core.types.policies import PipelineRunConfig, SkipPolicy, UpstreamPolicy
from op.core.types.product_ref import ProductRef
from op.core.types.registry import ProductRegistry, ProductRegistration
from op.platform.io.memory.client import Client

from op.platform.markers.memory_store import MemoryMarkerStore

from op.core.pipeline.pipeline_binding import PipelineBinding

from op.platform.clients.http.basic import BasicHttpRunner
from op.platform.binders.http import bind_http_source
from op.platform.configs.bindings import HttpSourceConfig


PROTOCOLS_RAW = ProductRef(domain=("raw","defillama"), name="protocols", version="v1")

def main() -> None:
    ENV = EnvProfile("test", vars={"root_prefix": "mem"})
    REG = ProductRegistry(_entries={})
    MARKERS = MemoryMarkerStore()
    IO = Client(ENV, REG, MARKERS)

    REG.register(ProductRegistration(
        product=PROTOCOLS_RAW,
        by_env={"test": FileLocation(
            storage_id="mem:test:bronze",
            uri_template="memory:///{root_prefix}/{domain}/{name}/{version}/dt={dt}/",
            format="parquet",
        )}
    ))

    runner = BasicHttpRunner()
    cfg = HttpSourceConfig(
        product=PROTOCOLS_RAW,
        recipe_pkg="op.catalog.raw.defillama.protocols",
        runner=runner,
        base_url="https://api.llama.fi",
        method="GET",
        path_template_name="path.j2",  # templates/path.j2 under recipe_pkg
        request_spec=None,
        row_type_spec="schema:Record",
        use_mock_data=False,
        mock_data_spec=None,
        defaults={},
        extract_path=None,
        next_url_json_path=None,
        cursor_json_path=None,
        cursor_param_name=None,
        max_pages=1,
        anchor_from_partition_key="dt",
        headers_defaults={},
        timeout_sec=30.0,
    )
    SRC = bind_http_source(cfg)

    pipe = PipelineBinding(
        env=ENV,
        registry=REG,
        marker_store=MARKERS,
        default_io=IO,
        sources=(SRC,),
        nodes=(),
        io_for={},
        run_config=PipelineRunConfig(
            skip=SkipPolicy(location_sensitivity="same_location", allow_copy_from_other_storage=True),
            upstream=UpstreamPolicy(on_missing="run_upstream"),
        ),
    )

    part = Partition({"dt": date.today().isoformat()})
    pipe.run(part)

    out = IO.read(PROTOCOLS_RAW, part)
    print("rows:", len(out.rows))
    print(out.rows[:3])


if __name__ == "__main__":
    main()
