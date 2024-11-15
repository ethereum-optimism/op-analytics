import pyarrow as pa

from op_analytics.datapipeline.schemas.core import to_bigquery_type


def test_struct_to_bigquery():
    struct_type = pa.struct(
        [
            pa.field(
                "ingestion_timestamp",
                pa.timestamp(unit="us"),
                metadata={"doc": "Time at which data was ingested."},
            )
        ]
    )
    actual = to_bigquery_type(struct_type)
    assert actual == "STRUCT<ingestion_timestamp TIMESTAMP>"
