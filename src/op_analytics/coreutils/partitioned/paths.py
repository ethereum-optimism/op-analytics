import re

from op_analytics.coreutils.time import date_fromstr


# Regex that maches "KEY=VAL/"
PART_RE = re.compile(r"(\w+)=(\w+)\/")

# Regex that matches "/dt=VAL/" and captures the value.
DATE_PART_RE = re.compile(r"\/dt=(?P<dt>\d{4}-\d{2}-\d{2})\/")


def get_root_path(parquet_paths: list[str]):
    """Use the first path to determine the root_path.

    The root path is the part of the path before the Hive partitions.
    """
    root_path = PART_RE.split(parquet_paths[0])[0]
    assert all(_.startswith(root_path) for _ in parquet_paths)
    return root_path


def get_dt(parquet_paths: list[str]):
    """Determine the date partition shared by all paths.

    All paths should share the same dt=YYYY-MM-DD partition value.
    """
    dates = set()
    for _ in parquet_paths:
        if m := DATE_PART_RE.search(_):
            dates.add(m.groupdict()["dt"])
        else:
            raise ValueError(f"could not determine dt from path: {_}")

    assert len(dates) == 1
    return date_fromstr(list(dates)[0])
