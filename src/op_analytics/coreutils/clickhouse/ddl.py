import glob
import os
from dataclasses import dataclass

from op_analytics.coreutils.logger import structlog


log = structlog.get_logger()

DIRECTORY = os.path.dirname(__file__)


def read_ddl(path: str):
    """Read a .sql DDL file from disk."""

    if not os.path.exists(path):
        raise Exception(f"DDL file not found: {path}")

    with open(path, "r") as f:
        return f.read()


@dataclass
class ClickHouseDDL:
    """Conveninece class to hold a ddl while remembering where it came from."""

    basename: str
    statement: str


def read_ddls(directory: str, globstr: str) -> list[ClickHouseDDL]:
    """Read all the ddls in the specified directory and glob.

    This is used to load a family of DDLs that need to be executed in sequence.
    """
    fullglobstr = os.path.join(directory, globstr)

    glob_results = glob.glob(fullglobstr)

    ddls: list[ClickHouseDDL] = []
    for path in glob_results:
        ddls.append(
            ClickHouseDDL(
                basename=os.path.basename(path),
                statement=read_ddl(path),
            )
        )

    return sorted(ddls, key=lambda x: x.basename)
