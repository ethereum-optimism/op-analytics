from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional, Any

from op.core.types.product_ref import ProductRef


@dataclass(frozen=True)
class GcsHmac:
    key_id: str
    secret: str


@dataclass(frozen=True)
class DuckDBSessionConfig:
    """
    Session bootstrap:
      - enable_httpfs: required for gs://, s3://, https:// reads/writes
      - gcs: supply HMAC creds if using GCS with CREATE SECRET
      - extra_sql: arbitrary SQL statements (e.g., SETs)
      - udf_modules: python modules that define register_udfs(con)
    """
    enable_httpfs: bool = False
    gcs: Optional[GcsHmac] = None
    extra_sql: list[str] = field(default_factory=list)
    udf_modules: list[str] = field(default_factory=list)

    def validate(self) -> None:
        # no-op currently
        return


@dataclass(frozen=True)
class DuckDBModelConfig:
    """
    Config for bind_duckdb_model():
      Either supply (template_pkg + sql_template_name) OR sql_text (inline).
      Jinja template gets helpers in context:
         - input_path(alias): gs://.../*.parquet
         - read_parquet(alias): read_parquet('gs://.../*.parquet')
         - output_path: destination file or prefix
    """
    name: str
    inputs: Dict[str, ProductRef]            # alias -> ProductRef
    output: ProductRef

    # Template-based
    template_pkg: Optional[str] = None       # e.g., "op.catalog.models.agg_duckdb"
    sql_template_name: Optional[str] = "query.sql.j2"

    # OR inline SQL
    sql_text: Optional[str] = None

    # Output format & options
    format: str = "parquet"                  # parquet|csv|json
    overwrite: bool = True
    output_filename: Optional[str] = None    # if None, COPY will write to the folder (parquet partitioning not managed here)

    template_context: Dict[str, Any] = field(default_factory=dict)

    session: DuckDBSessionConfig = field(default_factory=DuckDBSessionConfig)

    # Optional contract hint
    required_output_fields: Optional[tuple[str, ...]] = None

    def validate(self) -> None:
        if not self.name:
            raise ValueError("[DuckDBModelConfig] name is required")
        if not self.inputs:
            raise ValueError("[DuckDBModelConfig] at least one input is required")
        if not self.output:
            raise ValueError("[DuckDBModelConfig] output is required")
        if self.sql_text is None:
            if not self.template_pkg:
                raise ValueError("[DuckDBModelConfig] need either sql_text or template_pkg")
            # If using template, ensure the template exists
            try:
                import importlib
                mod = importlib.import_module(self.template_pkg)
                pkg_dir = Path(getattr(mod, "__path__")[0])
                tname = self.sql_template_name or "query.sql.j2"
                tfile = pkg_dir / tname
                if not tfile.exists():
                    raise FileNotFoundError(f"[DuckDBModelConfig] template not found: {tfile}")
            except Exception as e:
                raise e
        if self.format not in ("parquet", "csv", "json"):
            raise ValueError("[DuckDBModelConfig] format must be one of parquet|csv|json")