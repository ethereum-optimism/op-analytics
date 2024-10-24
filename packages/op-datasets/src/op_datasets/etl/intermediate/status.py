from datetime import date

import polars as pl

from op_coreutils.logger import structlog


log = structlog.get_logger()


def are_inputs_ready(paths_by_dataset_df: pl.DataFrame, dateval: date) -> bool:
    """Decide if we the input data for a given date is complete."""
    return False
