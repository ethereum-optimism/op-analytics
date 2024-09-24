from pyiceberg.schema import NestedField
from pyiceberg.types import TimestampType, StructType


INGESTION_METADATA = NestedField(
    field_id=10001,
    name="ingestion_metadata",
    field_type=StructType(
        fields=[
            NestedField(
                field_id=1,
                name="ingestion_time",
                field_type=TimestampType(),
                doc="Time at which data was ingested.",
            )
        ]
    ),
    doc="Internal metadata obtained during indexing.",
)
