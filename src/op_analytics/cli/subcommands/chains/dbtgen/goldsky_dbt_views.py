from op_coreutils.path import repo_path


from op_datasets.coretables.fromgoldsky import get_sql
from op_coreutils.logger import LOGGER

from op_analytics.cli.subcommands.chains.dbtgen.yamlwriter import AUTOGEN_WARNING_SQL, VIEW_CONFIG

log = LOGGER.get_logger()


def generate():
    """Generate dbt view models corresponding to the OP Labs dataset schemas."""

    # chains = goldsky_chains()
    datasets = ["blocks", "transactions", "traces", "logs"]
    datasets = ["blocks"]
    # tables = [dict(name=f"{chain}_{dataset}") for chain in chains for dataset in datasets]

    for dataset in datasets:
        sql = get_sql("op", "blocks", use_dbt_ref=True)

        path = repo_path(f"dbt/models/audited_{dataset}.sql")

        with open(path, "w") as fobj:
            fobj.write(AUTOGEN_WARNING_SQL + "\n\n")
            fobj.write(VIEW_CONFIG + "\n\n")
            fobj.write(sql)

        log.info(f"Wrote dbt model to {path}")
