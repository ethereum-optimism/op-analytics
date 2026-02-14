import sys
import types
import pytest

# Stub out heavy dependencies used during import. The real modules pull in
# native dependencies that are not available in the test environment, so we
# provide minimal ``ModuleType`` shims instead of ``SimpleNamespace`` objects to
# keep ``mypy`` satisfied.

duckdb_stub = types.ModuleType("duckdb")
setattr(duckdb_stub, "DuckDBPyConnection", object)
setattr(duckdb_stub, "DuckDBPyRelation", object)
sys.modules.setdefault("duckdb", duckdb_stub)

polars_stub = types.ModuleType("polars")
setattr(polars_stub, "DataFrame", object)
sys.modules.setdefault("polars", polars_stub)

pyarrow_stub = types.ModuleType("pyarrow")
setattr(pyarrow_stub, "Table", object)
sys.modules.setdefault("pyarrow", pyarrow_stub)

overrides_stub = types.ModuleType("overrides")
setattr(overrides_stub, "EnforceOverrides", object)
setattr(overrides_stub, "override", lambda f: f)
sys.modules.setdefault("overrides", overrides_stub)

orjson_stub = types.ModuleType("orjson")
sys.modules.setdefault("orjson", orjson_stub)

decoders_stub = types.ModuleType(
    "op_analytics.datapipeline.models.code.account_abstraction.decoders",
)
setattr(decoders_stub, "register_4337_decoders", lambda ctx: None)
sys.modules.setdefault(
    "op_analytics.datapipeline.models.code.account_abstraction.decoders",
    decoders_stub,
)

compute_model_stub = types.ModuleType(
    "op_analytics.datapipeline.models.compute.model",
)
setattr(compute_model_stub, "AuxiliaryTemplate", type("AuxiliaryTemplate", (), {}))
sys.modules.setdefault("op_analytics.datapipeline.models.compute.model", compute_model_stub)

compute_registry_stub = types.ModuleType(
    "op_analytics.datapipeline.models.compute.registry",
)
setattr(compute_registry_stub, "register_model", lambda *a, **k: (lambda f: f))
sys.modules.setdefault("op_analytics.datapipeline.models.compute.registry", compute_registry_stub)

compute_types_stub = types.ModuleType(
    "op_analytics.datapipeline.models.compute.types",
)
setattr(compute_types_stub, "NamedRelations", dict)
sys.modules.setdefault("op_analytics.datapipeline.models.compute.types", compute_types_stub)

dummy_logger = types.ModuleType("op_analytics.coreutils.logger")
setattr(dummy_logger, "memory_usage", lambda: 0)
setattr(
    dummy_logger,
    "structlog",
    types.SimpleNamespace(
        get_logger=lambda: types.SimpleNamespace(
            info=lambda *a, **k: None, error=lambda *a, **k: None
        )
    ),
)
setattr(dummy_logger, "human_size", lambda *a, **k: "0B")
sys.modules.setdefault("op_analytics.coreutils.logger", dummy_logger)

from op_analytics.datapipeline.models.code.account_abstraction import model


class DummyParquetData:
    def as_subquery(self):
        return "SELECT 1"

    def create_table(self):
        return "dummy_table"


class DummyTemplate:
    def __init__(self, errors=None):
        self.errors = errors or []

    def create_table(self, duckdb_context, template_parameters):
        return "user_ops"

    def create_view(self, duckdb_context, template_parameters):
        return "entrypoint_traces"

    def run_as_data_quality_check(self, duckdb_context):
        return self.errors


def test_data_quality_error_reports_template(monkeypatch):
    # Patch decoder registration to no-op to avoid needing DuckDB setup
    monkeypatch.setattr(model, "register_4337_decoders", lambda ctx: None)

    ctx = object()
    input_datasets = {
        "blockbatch/account_abstraction_prefilter/entrypoint_logs_v1": DummyParquetData(),
        "blockbatch/account_abstraction_prefilter/entrypoint_traces_v1": DummyParquetData(),
    }
    aux_templates = {
        "account_abstraction/useroperationevent_logs": DummyTemplate(),
        "account_abstraction/enriched_entrypoint_traces": DummyTemplate(),
        "account_abstraction/data_quality_check_fail": DummyTemplate(
            errors=[{"error": "bad data"}]
        ),
    }

    with pytest.raises(Exception) as excinfo:
        model.account_abstraction(ctx, input_datasets, aux_templates)

    assert "account_abstraction/data_quality_check_fail" in str(excinfo.value)
