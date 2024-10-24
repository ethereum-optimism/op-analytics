# -*- coding: utf-8 -*-
from op_datasets.etl.intermediate.models.daily_address_summary import (
    daily_address_summary,
)
import polars as pl


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


# def test_multiple_transactions_output():
#     result = daily_address_summary(
#         df_multiple_txs,
#         conditions=CONDITIONS,
#         address="from_address",
#         chain_id="chain_id",
#         chain="chain",
#         dt="dt",
#     )

#     assert pl.select(result["total_txs"]).item() == 3
#     assert (
#         pl.select(result["total_txs_success"]).item()
#         + pl.select(result["total_txs_fail"]).item()
#         == pl.select(result["total_txs"]).item()
#     )
#     assert pl.select(result["total_blocks"]).item() == 3
#     assert (
#         pl.select(result["total_blocks_success"]).item()
#         + pl.select(result["total_blocks_fail"]).item()
#         == pl.select(result["total_blocks"]).item()
#     )
#     assert pl.select(result["nonce_interval_active"]).item() == 3
#     assert (
#         pl.select(result["total_l2_gas_used"]).item()
#         == df_multiple_txs["receipt_gas_used"].sum()
#     )
#     assert (
#         pl.select(result["total_l1_gas_used"]).item()
#         == df_multiple_txs["receipt_l1_gas_used"].sum()
#     )
#     assert pl.select(result["total_gas_fees"]).item() == Decimal(
#         str(0.0000003351026704660000)
#     )
#     assert (
#         pl.select(result["l2_contrib_gas_fees_base_fee"]).item()
#         + pl.select(result["l2_contrib_gas_fees_priority_fee"]).item()
#         == pl.select(result["l2_contrib_gas_fees"]).item()
#     )
#     assert pl.select(result["l1_contrib_gas_fees"]).item() == Decimal(
#         str(0.0000001870209604660000)
#     )
#     # assert (
#     #     pl.select(result["l1_blobgas_contrib_gas_fees"]).item()
#     #     + pl.select(result["l1_l1gas_contrib_gas_fees"]).item()
#     #     == pl.select(result["l1_contrib_gas_fees"]).item()
#     # )
