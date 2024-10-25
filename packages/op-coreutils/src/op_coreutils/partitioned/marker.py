import socket
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from op_coreutils.time import now

from .output import WrittenParquetPath
from .types import SinkMarkerPath


@dataclass
class Marker:
    """Represent a marker for a collection of objects written to storage."""

    marker_path: SinkMarkerPath
    dataset_name: str
    data_paths: list[WrittenParquetPath]
    chain: str
    process_name: str

    def to_rows(self) -> list[dict[str, Any]]:
        current_time = now()
        hostname = socket.gethostname()
        rows = []
        for parquet_out in self.data_paths:
            rows.append(
                {
                    "updated_at": current_time,
                    "marker_path": self.marker_path,
                    "dataset_name": self.dataset_name,
                    "data_path": parquet_out.full_path,
                    "row_count": parquet_out.row_count,
                    "chain": self.chain,
                    "dt": parquet_out.safe_dt_value(),
                    "process_name": self.process_name,
                    "writer_name": hostname,
                }
            )
        return rows

    def arrow_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("updated_at", pa.timestamp(unit="us", tz=None)),
                pa.field("marker_path", pa.string()),
                pa.field("dataset_name", pa.string()),
                pa.field("data_path", pa.string()),
                pa.field("row_count", pa.int64()),
                pa.field("chain", pa.string()),
                pa.field("dt", pa.date32()),
                pa.field("process_name", pa.string()),
                pa.field("writer_name", pa.string()),
            ]
        )
