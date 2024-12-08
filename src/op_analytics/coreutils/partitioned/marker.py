import socket
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from op_analytics.coreutils.time import date_fromstr, now

from .output import ExpectedOutput
from .partition import WrittenParts


@dataclass
class Marker:
    """Marks that data has been processed and written out.

    A marker is associated with a single ExpectedOutput and one or more
    parquet files written to storage.
    """

    expected_output: ExpectedOutput

    written_parts: WrittenParts

    @property
    def marker_path(self):
        return self.expected_output.marker_path

    def arrow_schema(self, extra_marker_columns_schema: list[pa.Field]) -> pa.Schema:
        return pa.schema(
            [
                pa.field("updated_at", pa.timestamp(unit="us", tz=None)),
                pa.field("marker_path", pa.string()),
                pa.field("dataset_name", pa.string()),
                pa.field("root_path", pa.string()),
                pa.field("num_parts", pa.int32()),
                pa.field("data_path", pa.string()),
                pa.field("row_count", pa.int64()),
                pa.field("process_name", pa.string()),
                pa.field("writer_name", pa.string()),
            ]
            + extra_marker_columns_schema
        )

    def to_pyarrow_table(
        self,
        extra_marker_columns: dict[str, Any],
        extra_marker_columns_schema: list[pa.Field],
    ) -> pa.Table:
        schema = self.arrow_schema(extra_marker_columns_schema)

        current_time = now()
        hostname = socket.gethostname()
        rows = []
        for partition, partition_metadata in self.written_parts.items():
            parquet_out_row = {
                "updated_at": current_time,
                "marker_path": self.expected_output.marker_path,
                "root_path": self.expected_output.root_path,
                "num_parts": len(self.written_parts),
                "dataset_name": "",
                "data_path": partition.full_path(
                    root_path=self.expected_output.root_path,
                    file_name=self.expected_output.file_name,
                ),
                "row_count": partition_metadata.row_count,
                "process_name": self.expected_output.process_name,
                "writer_name": hostname,
            }

            for col in partition:
                sch = schema.field(col.name)
                if sch.type == pa.date32():
                    if isinstance(col.value, str):
                        parquet_out_row[col.name] = date_fromstr(col.value)
                    else:
                        raise NotImplementedError(
                            f"unsupported value for date partition: {col.value}"
                        )
                else:
                    parquet_out_row[col.name] = col.value

            rows.append(parquet_out_row)

        for row in rows:
            for name, value in extra_marker_columns.items():
                row[name] = value

        return pa.Table.from_pylist(rows, schema=schema)
