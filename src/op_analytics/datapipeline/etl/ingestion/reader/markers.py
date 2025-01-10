from dataclasses import dataclass


from op_analytics.coreutils.logger import structlog


log = structlog.get_logger()


@dataclass
class IngestionData:
    # True if the data is complete and ready to be consumed.
    is_complete: bool

    # Physical parquet paths (values) that will be read for each logical root path (keys).
    data_paths: dict[str, list[str]] | None
