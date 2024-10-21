import json
import os

from dataclasses import dataclass, asdict
from typing import NewType

import pyarrow as pa
import polars as pl
from overrides import EnforceOverrides, override

from op_coreutils.clickhouse.client import insert_dataframe, run_oplabs_query
from op_coreutils.logger import structlog
from op_coreutils.storage.gcs_parquet import (
    gcs_upload_partitioned_parquet,
    local_write_partitioned_parquet,
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
        content: list[PartitionOutput],
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
        content: list[PartitionOutput],
        marker_path: SinkMarkerPath,
    ):
        """Markers for GCS output are writen to Clickhouse.

        Having markers in clickhouse allows us to quickly perform analytics
        and query marker data over time.
        """
        dts: list[str] = []
        full_paths: list[str] = []
        partition_cols: list[list[dict[str, str]]] = []
        row_counts: list[int] = []
        total_rows: int = 0
        output: PartitionOutput
        for output in content:
            dts.append(output.dt_value())
            full_paths.append(output.path)
            partition_cols.append(output.partition_key_value_list())
            row_counts.append(output.row_count)
            total_rows += output.row_count
        dt = min(dts)

        row = {
            "dt": dt,
            "marker_path": marker_path,
            "total_rows": total_rows,
            "outputs.full_path": full_paths,
            "outputs.partition_cols": partition_cols,
            "outputs.row_count": row_counts,
        }

        schema = pa.schema(
            [
                pa.field("dt", pa.string()),
                pa.field("marker_path", pa.string()),
                pa.field("total_rows", pa.int64()),
                pa.field("outputs.full_path", pa.large_list(pa.string())),
                pa.field(
                    "outputs.partition_cols", pa.large_list(pa.map_(pa.string(), pa.string()))
                ),
                pa.field("outputs.row_count", pa.large_list(pa.int64())),
            ]
        )

        table = pa.Table.from_pylist(
            [row],
            schema=schema,
        )

        insert_dataframe("OPLABS", "etl_monitor", "gcs_parquet_markers", table)

    @override
    def is_complete(self, marker_path: SinkMarkerPath) -> bool:
        result = run_oplabs_query(
            "SELECT marker_path FROM etl_monitor.gcs_parquet_markers WHERE marker_path = {search_value:String}",
            parameters={"search_value": marker_path},
        )

        return len(result) > 0


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
        return local_write_partitioned_parquet(
            root_path=self.full_path(root_path),
            basename=basename,
            df=dataframe,
            partition_cols=partition_cols,
        )

    @override
    def write_marker(
        self,
        content: list[PartitionOutput],
        marker_path: SinkMarkerPath,
    ):
        fs = LocalFileSystem(auto_mkdir=True)
        with fs.open(self.full_path(marker_path), "w") as fobj:
            fobj.write(json.dumps([asdict(_) for _ in content], indent=2))

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
