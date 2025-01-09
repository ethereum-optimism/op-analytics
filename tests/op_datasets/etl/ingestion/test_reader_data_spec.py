from op_analytics.datapipeline.etl.ingestion.reader.markers import IngestionDataSpec
from op_analytics.datapipeline.etl.ingestion.reader.rootpaths import RootPath


def test_query_mixed_networks():
    spec = IngestionDataSpec(
        chains=["op", "op_sepolia"],
        root_paths_to_read=[
            RootPath.of("ingestion/blocks_v1"),
            RootPath.of("ingestion/logs_v1"),
            RootPath.of("ingestion/traces_v1"),
            RootPath.of("ingestion/transactions_v1"),
        ],
    )

    assert spec.physical_root_paths() == [
        "ingestion/blocks_v1",
        "ingestion/logs_v1",
        "ingestion/traces_v1",
        "ingestion/transactions_v1",
        "ingestion_testnets/blocks_v1",
        "ingestion_testnets/logs_v1",
        "ingestion_testnets/traces_v1",
        "ingestion_testnets/transactions_v1",
    ]


def test_query_mainnet():
    spec = IngestionDataSpec(
        chains=["op", "mode"],
        root_paths_to_read=[
            RootPath.of("ingestion/blocks_v1"),
            RootPath.of("ingestion/logs_v1"),
            RootPath.of("ingestion/traces_v1"),
            RootPath.of("ingestion/transactions_v1"),
        ],
    )

    assert spec.physical_root_paths() == [
        "ingestion/blocks_v1",
        "ingestion/logs_v1",
        "ingestion/traces_v1",
        "ingestion/transactions_v1",
    ]


def test_query_testnet():
    spec = IngestionDataSpec(
        chains=["op_sepolia", "unichain_sepolia"],
        root_paths_to_read=[
            RootPath.of("ingestion/blocks_v1"),
            RootPath.of("ingestion/logs_v1"),
            RootPath.of("ingestion/traces_v1"),
            RootPath.of("ingestion/transactions_v1"),
        ],
    )

    assert spec.physical_root_paths() == [
        "ingestion_testnets/blocks_v1",
        "ingestion_testnets/logs_v1",
        "ingestion_testnets/traces_v1",
        "ingestion_testnets/transactions_v1",
    ]
