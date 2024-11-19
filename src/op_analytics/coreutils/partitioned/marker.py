import socket
from dataclasses import dataclass

import pyarrow as pa

from op_analytics.coreutils.time import date_fromstr, now

from .output import OutputPartMeta, ExpectedOutput


@dataclass
class Marker:
    """Represent a marker for a collection of objects written to storage."""

    expected_output: ExpectedOutput

    written_parts: list[OutputPartMeta]

    @property
    def marker_path(self):
        return self.expected_output.marker_path

    def arrow_schema(self) -> pa.Schema:
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
            + self.expected_output.additional_columns_schema
        )

    def to_pyarrow_table(self) -> pa.Table:
        schema = self.arrow_schema()

        current_time = now()
        hostname = socket.gethostname()
        rows = []
        for parquet_out in self.written_parts:
            parquet_out_row = {
                "updated_at": current_time,
                "marker_path": self.expected_output.marker_path,
                "root_path": self.expected_output.root_path,
                "num_parts": len(self.written_parts),
                "dataset_name": self.expected_output.dataset_name,
                "data_path": parquet_out.full_path(
                    self.expected_output.root_path, self.expected_output.file_name
                ),
                "row_count": parquet_out.row_count,
                "process_name": self.expected_output.process_name,
                "writer_name": hostname,
            }

            for partition in parquet_out.partitions:
                sch = schema.field(partition.key)
                if sch.type == pa.date32():
                    if isinstance(partition.value, str):
                        parquet_out_row[partition.key] = date_fromstr(partition.value)
                    else:
                        raise NotImplementedError(
                            f"unsupported value for date partition: {partition.value}"
                        )
                else:
                    parquet_out_row[partition.key] = partition.value

            rows.append(parquet_out_row)

        for row in rows:
            for name, value in self.expected_output.additional_columns.items():
                row[name] = value

        return pa.Table.from_pylist(rows, schema=schema)
