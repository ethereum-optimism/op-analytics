from dataclasses import dataclass

import pyarrow as pa
from op_coreutils.logger import structlog
from op_coreutils.partitioned import Marker

log = structlog.get_logger()


@dataclass
class IngestionCompletionMarker:
    num_blocks: int
    min_block: int
    max_block: int
    chain: str
    marker: Marker

    def to_pyarrow_table(self) -> pa.Table:
        rows: list[dict] = self.marker.to_rows()

        schema = self.marker.arrow_schema()
        schema_new = (
            schema.append(pa.field("num_blocks", pa.int32()))
            .append(pa.field("min_block", pa.int64()))
            .append(pa.field("max_block", pa.int64()))
        )

        for row in rows:
            row["num_blocks"] = self.num_blocks
            row["min_block"] = self.min_block
            row["max_block"] = self.max_block

        return pa.Table.from_pylist(rows, schema=schema_new)
