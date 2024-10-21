# -*- coding: utf-8 -*-
from op_datasets.logic.transforms.daily_address_summary import (
    daily_address_summary,
    CONDITIONS,
)
import polars as pl
from decimal import Decimal


# transaction link: https://optimistic.etherscan.io/tx/0xc64c1471f38b0444d143011c0261a6c14d76a8b32115d39a3f3bf11af337eb55
df_single_tx = pl.read_parquet("test_daily_address_summary_single_tx.parquet")
# transactions link:
# https://optimistic.etherscan.io/tx/0x29ca6a42c2164ccddd912b227e3547685d34e27f63f0c29504b4420787a4c54f
# https://optimistic.etherscan.io/tx/0xdfe5c1ec2e815e37421e08cf838636e6d1c2ca3b069ca05a698c501e14920af5
# https://optimistic.etherscan.io/tx/0x4fe06c4733a8115872d016e976c60dcc8dba796beb583044acebc5048378a4aa
df_multiple_txs = pl.read_parquet("test_daily_address_summary_multiple_txs.parquet")


def test_single_transaction_output():
    result = daily_address_summary(
        df_single_tx,
        conditions=CONDITIONS,
        address="from_address",
        chain_id="chain_id",
        chain="chain",
        dt="dt",
    )

    assert pl.select(result["min_block_number"]).item() == 126070912
    assert pl.select(result["min_nonce"]).item() == 5176
    assert pl.select(result["total_l2_gas_used"]).item() == 119116
    assert pl.select(result["total_l1_gas_used"]).item() == 2396
    assert pl.select(result["total_gas_fees"]).item() == Decimal(
        str(0.000003121154293157)
    )
    assert (
        pl.select(result["l2_contrib_gas_fees"]).item()
        + pl.select(result["l1_contrib_gas_fees"]).item()
        == pl.select(result["total_gas_fees"]).item()
    )


def test_multiple_transactions_output():
    result = daily_address_summary(
        df_multiple_txs,
        conditions=CONDITIONS,
        address="from_address",
        chain_id="chain_id",
        chain="chain",
        dt="dt",
    )

    assert pl.select(result["total_txs"]).item() == 3
    assert (
        pl.select(result["total_txs_success"]).item()
        + pl.select(result["total_txs_fail"]).item()
        == pl.select(result["total_txs"]).item()
    )
    assert pl.select(result["total_blocks"]).item() == 3
    assert (
        pl.select(result["total_blocks_success"]).item()
        + pl.select(result["total_blocks_fail"]).item()
        == pl.select(result["total_blocks"]).item()
    )
    assert pl.select(result["nonce_interval_active"]).item() == 3
    assert (
        pl.select(result["total_l2_gas_used"]).item()
        == df_multiple_txs["receipt_gas_used"].sum()
    )
    assert (
        pl.select(result["total_l1_gas_used"]).item()
        == df_multiple_txs["receipt_l1_gas_used"].sum()
    )
    assert pl.select(result["total_gas_fees"]).item() == Decimal(
        str(0.0000003351026704660000)
    )
    assert (
        pl.select(result["l2_contrib_gas_fees_base_fee"]).item()
        + pl.select(result["l2_contrib_gas_fees_priority_fee"]).item()
        == pl.select(result["l2_contrib_gas_fees"]).item()
    )
    assert pl.select(result["l1_contrib_gas_fees"]).item() == Decimal(
        str(0.0000001870209604660000)
    )
    # assert (
    #     pl.select(result["l1_blobgas_contrib_gas_fees"]).item()
    #     + pl.select(result["l1_l1gas_contrib_gas_fees"]).item()
    #     == pl.select(result["l1_contrib_gas_fees"]).item()
    # )
