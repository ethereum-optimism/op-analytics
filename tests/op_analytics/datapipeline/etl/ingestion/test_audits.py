from op_analytics.datapipeline.etl.ingestion.audits.audits import transaction_count

import polars as pl


def test_transaction_count_audit_matching_counts():
    dataframes = {
        "blocks": pl.DataFrame(
            {
                "number": [1, 2, 3],
                "transaction_count": [2, 1, 3],
            }
        ),
        "transactions": pl.DataFrame(
            {
                "block_number": [1, 1, 2, 3, 3, 3],
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

    check = transaction_count("ethereum", dataframes).to_dicts()
    assert check == [
        {
            "audit_name": "block transaction count must match observed transactions",
            "failure_count": 0,
        }
    ]

def test_transaction_count_audit_zero_counts_null_txs():
    dataframes = {
        "blocks": pl.DataFrame(
            {
                "number": [1, 2, 3],
                "transaction_count": [2, 1, 0],
            }
        ),
        "transactions": pl.DataFrame(
            {
                "block_number": [1, 1, 2],
            }
        ),
    }
    check = transaction_count("base", dataframes).to_dicts()
    assert check == [
        {
            "audit_name": "block transaction count must match observed transactions",
            "failure_count": 1,
        }
    ]

    check = transaction_count("ethereum", dataframes).to_dicts()
    assert check == [
        {
            "audit_name": "block transaction count must match observed transactions",
            "failure_count": 0,
        }
    ]

def test_transaction_count_audit_nonzero_counts_null_txs():
    dataframes = {
        "blocks": pl.DataFrame(
            {
                "number": [1, 2, 3],
                "transaction_count": [2, 1, 1],  # Last block claims 1 tx but has none
            },
            schema_overrides={
                "number": pl.Int64(),
                "transaction_count": pl.Int64(),
            },
        ),
        "transactions": pl.DataFrame(
            {
                "block_number": [1, 1, 2],
            },
            schema_overrides={"block_number": pl.Int64()},
        ),
    }
    check = transaction_count("base", dataframes).to_dicts()
    assert check == [
        {
            "audit_name": "block transaction count must match observed transactions",
            "failure_count": 1,  # Only the last block should fail
        }
    ]

def test_transaction_count_audit_mismatching_counts():
    dataframes = {
        "blocks": pl.DataFrame(
            {
                "number": [1, 2, 3],
                "transaction_count": [3, 1, 0],  # First block claims 3 but has 2
            },
            schema_overrides={
                "number": pl.Int64(),
                "transaction_count": pl.Int64(),
            },
        ),
        "transactions": pl.DataFrame(
            {
                "block_number": [1, 1, 2],
            },
            schema_overrides={"block_number": pl.Int64()},
        ),
    }
    check = transaction_count("base", dataframes).to_dicts()
    assert check == [
        {
            "audit_name": "block transaction count must match observed transactions",
            "failure_count": 2,  # Only the first block should fail
        }
    ]

    check = transaction_count("ethereum", dataframes).to_dicts()
    assert check == [
        {
            "audit_name": "block transaction count must match observed transactions",
            "failure_count": 1,  # Only the first block should fail
        }
    ]

def test_transaction_count_audit_empty_transactions():
    dataframes = {
        "blocks": pl.DataFrame(
            {
                "number": [1, 2, 3],
                "transaction_count": [1, 1, 1],  # All blocks claim 1 transaction
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
            "failure_count": 3,  # All blocks should fail
        }
    ]

    check = transaction_count("ethereum", dataframes).to_dicts()
    assert check == [
        {
            "audit_name": "block transaction count must match observed transactions",
            "failure_count": 3,  # All blocks should fail
        }
    ]

def test_transaction_count_audit_empty_blocks():
    dataframes = {
        "blocks": pl.DataFrame(
            {
                "number": [],
                "transaction_count": [],
            },
            schema_overrides={
                "number": pl.Int64(),
                "transaction_count": pl.Int64(),
            },
        ),
        "transactions": pl.DataFrame(
            {
                "block_number": [1, 2, 3],
            },
            schema_overrides={"block_number": pl.Int64()},
        ),
    }
    check = transaction_count("base", dataframes).to_dicts()
    assert check == [
        {
            "audit_name": "block transaction count must match observed transactions",
            "failure_count": 0,
        }
    ]
