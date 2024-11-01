from datetime import date

from op_coreutils.partitioned.paths import get_root_path, get_dt


def test_get_root_01():
    paths = [
        "gs://mybucket/ingestion/transactions_v1/chain=base/dt=2024-10-01/000020474000.parquet",
        "gs://mybucket/ingestion/transactions_v1/chain=base/dt=2024-10-01/000020476000.parquet",
        "gs://mybucket/ingestion/transactions_v1/chain=base/dt=2024-10-01/000020478000.parquet",
        "gs://mybucket/ingestion/transactions_v1/chain=base/dt=2024-10-01/000020480000.parquet",
        "gs://mybucket/ingestion/transactions_v1/chain=base/dt=2024-10-01/000020482000.parquet",
        "gs://mybucket/ingestion/transactions_v1/chain=base/dt=2024-10-01/000020484000.parquet",
    ]

    actual = get_root_path(paths)
    assert actual == "gs://mybucket/ingestion/transactions_v1/"


def test_get_root_02():
    paths = [
        "gs://mybucket/ingestion/transactions_v1/mypart=base/dt=2024-10-01/000020474000.parquet",
        "gs://mybucket/ingestion/transactions_v1/mypart=base/dt=2024-10-01/000020476000.parquet",
        "gs://mybucket/ingestion/transactions_v1/mypart=base/dt=2024-10-01/000020478000.parquet",
        "gs://mybucket/ingestion/transactions_v1/mypart=base/dt=2024-10-01/000020480000.parquet",
        "gs://mybucket/ingestion/transactions_v1/mypart=base/dt=2024-10-01/000020482000.parquet",
        "gs://mybucket/ingestion/transactions_v1/mypart=base/dt=2024-10-01/000020484000.parquet",
    ]

    actual = get_root_path(paths)
    assert actual == "gs://mybucket/ingestion/transactions_v1/"


def test_get_dt():
    paths = [
        "gs://mybucket/ingestion/transactions_v1/chain=base/dt=2024-10-01/000020474000.parquet",
        "gs://mybucket/ingestion/transactions_v1/chain=base/dt=2024-10-01/000020476000.parquet",
        "gs://mybucket/ingestion/transactions_v1/chain=base/dt=2024-10-01/000020478000.parquet",
        "gs://mybucket/ingestion/transactions_v1/chain=base/dt=2024-10-01/000020480000.parquet",
        "gs://mybucket/ingestion/transactions_v1/chain=base/dt=2024-10-01/000020482000.parquet",
        "gs://mybucket/ingestion/transactions_v1/chain=base/dt=2024-10-01/000020484000.parquet",
    ]

    actual = get_dt(paths)
    assert actual == date(2024, 10, 1)
