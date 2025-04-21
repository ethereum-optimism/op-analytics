from op_analytics.datapipeline.etl.ingestion.audits.audits import transaction_count

import polars as pl


def test_transaction_count_audit_OK():
    dataframes = {
        "blocks": pl.DataFrame(
            {
                "number": [1, 2, 3],
                "transaction_count": [2, 2, 2],
            }
        ),
        "transactions": pl.DataFrame(
            {
                "block_number": [1, 1, 2, 2, 3, 3],
            }
        ),
    }
    check = transaction_count("base", dataframes).to_dicts()
    assert check == [
        {
            "audit_name": "block transaction count must match observed transactions",
            "failure_count": 0,
        }
    ]


def test_transaction_count_audit_FAIL():
    dataframes = {
        "blocks": pl.DataFrame(
            {
                "number": [1, 2, 3],
                "transaction_count": [2, 2, 2],
            },
            schema_overrides={
                "number": pl.Int64(),
                "transaction_count": pl.Int64(),
            },
        ),
        "transactions": pl.DataFrame(
            {"block_number": []},
            schema_overrides={"block_number": pl.Int64()},
        ),
    }
    check = transaction_count("base", dataframes).to_dicts()
    assert check == [
        {
            "audit_name": "block transaction count must match observed transactions",
            "failure_count": 3,
        }
    ]
