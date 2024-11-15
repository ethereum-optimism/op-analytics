import os

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.path import repo_path

log = structlog.get_logger()


def load_env():
    dotenv_path = repo_path(".env")

    if dotenv_path is None:
        log.error("Did not find .env. No env vars will be loaded.")
        return

    with open(dotenv_path, "r") as fobj:
        for line in fobj:
            key, val = line.split("=", maxsplit=1)
            os.environ[key] = val.strip()
            log.info(f"Loaded env var: {key}")
    return
