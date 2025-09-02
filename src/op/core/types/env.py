from dataclasses import dataclass, field

@dataclass(frozen=True)
class EnvProfile:
    name: str                   # "local" | "dev" | "staging" | "prod"
    vars: dict[str, str] = field(default_factory=dict)
    # vars can include: "gcs_bucket", "root_prefix", "bq_project", "bq_dataset", "ch_url", ...
