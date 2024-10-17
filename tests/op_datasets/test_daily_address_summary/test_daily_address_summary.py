# -*- coding: utf-8 -*-
from op_datasets.logic.transforms.daily_address_summary import (
    daily_address_summary,
    CONDITIONS,
)
import polars as pl

# transaction link: https://optimistic.etherscan.io/tx/0xc64c1471f38b0444d143011c0261a6c14d76a8b32115d39a3f3bf11af337eb55
df_single_tx = pl.read_parquet("test_daily_address_summary_single_tx.parquet")


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
