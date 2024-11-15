import pyarrow as pa

from op_analytics.datapipeline.schemas.core import Column


METADATA = Column(
    name="_meta",
    field_type=pa.struct(
        [
            pa.field(
                "ingestion_timestamp",
                pa.timestamp(unit="us"),
                metadata={"doc": "Time at which data was ingested."},
            )
        ]
    ),
    required=True,
    doc="Internal metadata obtained during indexing.",
)


CHAIN = Column(
    name="chain",
    field_type=pa.large_string(),
    required=True,
    doc="Name of the chain. Example: 'op'",
    op_analytics_clickhouse_expr="chain",
)


NETWORK = Column(
    name="network",
    field_type=pa.large_string(),
    required=True,
    doc="Network codename. Example: 'mainnet'",
    op_analytics_clickhouse_expr="network",
)


CHAIN_ID = Column(
    name="chain_id",
    field_type=pa.int32(),
    required=True,
    doc="Chain id as specified on the chain's configuration. Example: 8453",
    op_analytics_clickhouse_expr="chain_id",
)
