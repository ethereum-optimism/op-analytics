from dataclasses import dataclass

import pyarrow as pa
from op_coreutils.logger import structlog
from op_coreutils.storage.paths import Marker
from op_coreutils.time import now

log = structlog.get_logger()


@dataclass
class IngestionCompletionMarker:
    process_name: str
    chain: str
    marker: Marker

    @property
    def path(self):
        return self.marker.marker_path

    def dt_value(self):
        return min(_.path.dt_value() for _ in self.marker.outputs)

    def to_clickhouse_pyarrow_table(self) -> pa.Table:
        row: dict = self.marker.to_clickhouse_row()
        row["process_name"] = self.process_name
        row["chain"] = self.chain
        row["dt"] = self.dt_value()

        marker_schema = self.marker.clickhouse_schema()
        schema = pa.schema(
            [
                marker_schema.field("marker_path"),
                marker_schema.field("total_rows"),
                marker_schema.field("outputs.full_path"),
                marker_schema.field("outputs.partition_cols"),
                marker_schema.field("outputs.row_count"),
                pa.field("process_name", pa.string()),
                pa.field("chain", pa.string()),
                pa.field("dt", pa.string()),
            ]
        )

        return pa.Table.from_pylist(
            [row],
            schema=schema,
        )

    def to_duckdb_pyarrow_table(self) -> pa.Table:
        row: dict = self.marker.to_duckdb_row()
        row["updated_at"] = now()
        row["process_name"] = self.process_name
        row["chain"] = self.chain
        row["dt"] = self.dt_value()

        marker_schema = self.marker.duckdb_schema()

        # The order of the columns must match the order as declared in the CREATE TABLE
        # statement for the table.
        schema = pa.schema(
            [
                pa.field("updated_at", pa.timestamp(unit="us", tz=None)),
                marker_schema.field("marker_path"),
                marker_schema.field("total_rows"),
                marker_schema.field("outputs"),
                pa.field("process_name", pa.string()),
                pa.field("chain", pa.string()),
                pa.field("dt", pa.string()),
            ]
        )

        return pa.Table.from_pylist(
            [row],
            schema=schema,
        )
