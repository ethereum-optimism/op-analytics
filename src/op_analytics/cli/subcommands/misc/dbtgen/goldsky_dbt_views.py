from op_coreutils.logger import structlog
from op_coreutils.path import repo_path

from op_analytics.cli.subcommands.misc.dbtgen.yamlwriter import AUTOGEN_WARNING_SQL, VIEW_CONFIG

from op_datasets.schemas import resolve_core_dataset

log = structlog.get_logger()


def jinja(val: str):
    return "{{ " + val + " }}"


def generate():
    """Generate dbt view models corresponding to the OP Labs dataset schemas."""

    datasets = ["blocks", "transactions", "logs"]  # "traces",

    for dataset in datasets:
        sql = resolve_core_dataset(dataset).goldsky_sql(
            source_table=jinja(f'source("goldsky_pipelines", "{dataset}")')
        )

        path = repo_path(f"dbt/models/audited_{dataset}.sql")

        with open(path, "w") as fobj:
            fobj.write(AUTOGEN_WARNING_SQL + "\n\n")
            fobj.write(VIEW_CONFIG + "\n\n")
            fobj.write(sql)

        log.info(f"Wrote dbt model to {path}")
