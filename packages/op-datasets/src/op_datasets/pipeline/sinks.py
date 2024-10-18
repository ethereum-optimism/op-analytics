import json
import os

from dataclasses import dataclass
from typing import Any, NewType

import polars as pl
from overrides import EnforceOverrides, override

from op_coreutils.logger import structlog
from op_coreutils.storage.gcs_parquet import (
    gcs_upload_partitioned_parquet,
    write_partitioned_parquet,
    PartitionOutput,
)
from fsspec.implementations.local import LocalFileSystem


log = structlog.get_logger()


# Root path for a partitioned dataframe output.
SinkOutputRootPath = NewType("SinkOutputRootPath", str)

# A single object path in a partitioned dataframe output (includes the base bath).
SinkOutputPath = NewType("SinkOutputPath", str)

# A single object path for a sink marker. Markers are light objects that are used to
# indicate a sink output has been written successfully.
SinkMarkerPath = NewType("SinkMarkerPath", str)


@dataclass(kw_only=True)
class DataSink(EnforceOverrides):
    sink_spec: str
    completion_status_cache: dict[SinkMarkerPath, bool]

    @classmethod
    def from_spec(cls, sink_spec: str) -> "DataSink":
        if sink_spec == "gcs":
            return GCSSink(
                sink_spec=sink_spec,
                completion_status_cache=dict(),
            )

        if sink_spec.startswith("file://"):
            return LocalFileSink(
                sink_spec=sink_spec,
                completion_status_cache=dict(),
                basepath=sink_spec.removeprefix("file://"),
            )

        raise NotImplementedError()

    def write_output(
        self,
        dataframe: pl.DataFrame,
        root_path: SinkOutputRootPath,
        basename: str,
        partition_cols: list[str],
    ) -> list[PartitionOutput]:
        raise NotImplementedError()

    def write_marker(
        self,
        content: Any,
        marker_path: SinkMarkerPath,
    ):
        raise NotImplementedError()

    def is_complete(self, marker_path: SinkMarkerPath) -> bool:
        raise NotImplementedError


@dataclass(kw_only=True)
class GCSSink(DataSink):
    @override
    def write_output(
        self,
        dataframe: pl.DataFrame,
        root_path: SinkOutputRootPath,
        basename: str,
        partition_cols: list[str],
    ) -> list[PartitionOutput]:
        return gcs_upload_partitioned_parquet(
            root_path=root_path,
            basename=basename,
            df=dataframe,
            partition_cols=partition_cols,
        )

    @override
    def write_marker(
        self,
        content: Any,
        marker_path: SinkMarkerPath,
    ):
        """Markers for GCS output are writen to Clickhouse.

        Having markers in clickhouse allows us to quickly perform analytics
        on marker data.
        """
        raise NotImplementedError()
        # Write markers for the data written to the sink
        # append_df("oplabs_monitor", "core_datasets", out.to_polars())
        # self.completion_status_cache[location] = True

    @override
    def is_complete(self, marker_path: SinkMarkerPath) -> bool:
        raise NotImplementedError


@dataclass(kw_only=True)
class LocalFileSink(DataSink):
    basepath: str

    def full_path(self, path: str):
        return os.path.join(self.basepath, path)

    @override
    def write_output(
        self,
        dataframe: pl.DataFrame,
        root_path: SinkOutputRootPath,
        basename: str,
        partition_cols: list[str],
    ) -> list[PartitionOutput]:
        fs = LocalFileSystem(auto_mkdir=True)
        return write_partitioned_parquet(
            filesystem=fs,
            root_path=self.full_path(root_path),
            basename=basename,
            df=dataframe,
            partition_cols=partition_cols,
        )

    @override
    def write_marker(
        self,
        content: Any,
        marker_path: SinkMarkerPath,
    ):
        fs = LocalFileSystem(auto_mkdir=True)
        with fs.open(self.full_path(marker_path), "w") as fobj:
            fobj.write(json.dumps(content, indent=2))

    @override
    def is_complete(self, marker_path: SinkMarkerPath) -> bool:
        if os.path.isfile(self.full_path(marker_path)):
            return True
        else:
            return False


def all_outputs_complete(sinks: list[DataSink], markers: list[SinkMarkerPath]) -> bool:
    result = True
    for sink in sinks:
        complete = []
        incomplete = []
        for marker in markers:
            if sink.is_complete(marker):
                complete.append(marker)
            else:
                incomplete.append(marker)

        log.info(
            f"{len(complete)} complete, {len(incomplete)} incomplete locations out of {len(markers)} expected at {sink.sink_spec}"
        )

        if incomplete:
            log.info(f"Showing the first 5 incomplete locations at {sink.sink_spec}")
            for location in sorted(incomplete)[:5]:
                log.info(f"DataSink {sink.sink_spec!r} is incomplete at {location!r}")
            result = False

    return result
