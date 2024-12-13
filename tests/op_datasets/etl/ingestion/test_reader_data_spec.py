from op_analytics.datapipeline.etl.ingestion.reader.markers import IngestionDataSpec


def test_query_mixed_networks():
    spec = IngestionDataSpec(
        chains=["op", "op_sepolia"],
    )

    assert spec.root_paths_query_filter() == [
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
    )

    assert spec.root_paths_query_filter() == [
        "ingestion/blocks_v1",
        "ingestion/logs_v1",
        "ingestion/traces_v1",
        "ingestion/transactions_v1",
    ]


def test_query_testnet():
    spec = IngestionDataSpec(
        chains=["op_sepolia", "unichain_sepolia"],
    )

    assert spec.root_paths_query_filter() == [
        "ingestion_testnets/blocks_v1",
        "ingestion_testnets/logs_v1",
        "ingestion_testnets/traces_v1",
        "ingestion_testnets/transactions_v1",
    ]
