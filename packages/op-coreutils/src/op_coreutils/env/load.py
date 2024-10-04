import os

from op_coreutils.logger import structlog
from op_coreutils.path import repo_path

log = structlog.get_logger()


def load_env():
    dotenv_path = repo_path(".env")

    if not os.path.exists(dotenv_path):
        raise ValueError(".env file is not present on the repo")

    with open(dotenv_path, "r") as fobj:
        for line in fobj:
            key, val = line.split("=", maxsplit=1)
            os.environ[key] = val.strip()
            log.info(f"Loaded env var: {key}")
