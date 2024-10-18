from pyiceberg.schema import NestedField
from pyiceberg.types import (
    IntegerType,
    StringType,
    StructType,
    TimestampType,
)

from op_datasets.schemas.core import Column


METADATA = Column(
    name="_meta",
    field_type=StructType(
        NestedField(
            field_id=1,
            name="ingestion_timestamp",
            field_type=TimestampType(),
            doc="Time at which data was ingested.",
        )
    ),
    required=True,
    doc="Internal metadata obtained during indexing.",
)


CHAIN = Column(
    name="chain",
    field_type=StringType(),
    required=True,
    doc="Name of the chain. Example: 'op'",
    op_analytics_clickhouse_expr="chain",
)


NETWORK = Column(
    name="network",
    field_type=StringType(),
    required=True,
    doc="Network codename. Example: 'mainnet'",
    op_analytics_clickhouse_expr="network",
)


CHAIN_ID = Column(
    name="chain_id",
    field_type=IntegerType(),
    required=True,
    doc="Chain id as specified on the chain's configuration. Example: 8453",
    op_analytics_clickhouse_expr="chain_id",
)
