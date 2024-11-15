from op_analytics.cli.subcommands.misc.dbtgen.yamlwriter import AUTOGEN_WARNING_SQL, VIEW_CONFIG
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.path import repo_path
from op_analytics.datapipeline.schemas import ONCHAIN_CURRENT_VERSION

log = structlog.get_logger()


def jinja(val: str):
    return "{{ " + val + " }}"


def generate():
    """Generate dbt view models corresponding to the OP Labs dataset schemas."""

    for name, dataset in ONCHAIN_CURRENT_VERSION.items():
        sql = dataset.goldsky_sql(source_table=jinja(f'source("goldsky_pipelines", "{name}")'))

        path = repo_path(f"dbt/models/audited_{name}.sql")
        assert path is not None

        with open(path, "w") as fobj:
            fobj.write(AUTOGEN_WARNING_SQL + "\n\n")
            fobj.write(VIEW_CONFIG + "\n\n")
            fobj.write(sql)

        log.info(f"Wrote dbt model to {path}")
