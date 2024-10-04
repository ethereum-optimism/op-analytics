from pyiceberg.schema import NestedField
from pyiceberg.types import (
    IntegerType,
    StringType,
    StructType,
    TimestampType,
)

from op_datasets.schemas.core import Column


def METADATA(field_id) -> Column:
    """Metadata column.

    We use this internally to track data pipeline metadata.
    """
    return Column(
        field_id=field_id,
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


def CHAIN(field_id) -> Column:
    return Column(
        field_id=field_id,
        name="chain",
        field_type=StringType(),
        required=True,
        doc="Name of the chain. Example: 'op'",
    )


def NETWORK(field_id) -> Column:
    return Column(
        field_id=field_id,
        name="network",
        field_type=StringType(),
        required=True,
        doc="Network codename. Example: 'mainnet'",
    )


def CHAIN_ID(field_id) -> Column:
    return Column(
        field_id=field_id,
        name="chain_id",
        field_type=IntegerType(),
        required=True,
        doc="Chain id as specified on the chain's configuration. Example: 8453",
    )
