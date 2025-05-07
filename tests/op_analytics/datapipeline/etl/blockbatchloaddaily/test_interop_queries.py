import os

from op_analytics.datapipeline.etl.blockbatchloaddaily.datasets import (
    INTEROP_NTT_FIRST_SEEN,
    INTEROP_OFT_FIRST_SEEN,
    INTEROP_NTT_TRANSFERS,
    INTEROP_OFT_TRANSFERS,
    INTEROP_ERC20_FIRST_SEEN,
)

from op_analytics.datapipeline.etl.blockbatchloaddaily.loadspec_datechain import DateChainBatch


def test_interop_queries():
    datasets = [
        INTEROP_NTT_FIRST_SEEN,
        INTEROP_OFT_FIRST_SEEN,
        INTEROP_NTT_TRANSFERS,
        INTEROP_OFT_TRANSFERS,
        INTEROP_ERC20_FIRST_SEEN,
    ]

    for dataset in datasets:
        ddl = dataset.insert_ddl_template(
            batch=DateChainBatch.of(chain="base", dt="2025-01-01"),
            dry_run=True,
        )

        expectation_file = os.path.join(
            os.path.dirname(__file__), f"rendered/{dataset.output_root_path}.sql"
        )

        if not os.path.exists(expectation_file):
            with open(expectation_file, "w") as f:
                f.write(ddl)

        with open(expectation_file, "r") as f:
            expected_ddl = f.read()

        if ddl != expected_ddl:
            print(ddl)

        assert ddl == expected_ddl
