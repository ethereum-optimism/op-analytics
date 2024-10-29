from decimal import Decimal

import polars as pl
from op_coreutils.partitioned import DataLocation
from op_datasets.etl.intermediate.construct import construct_tasks
from op_datasets.etl.intermediate.models.daily_address_summary import (
    daily_address_summary,
)

SYSTEM_ADDRESS = "0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001"
SINGLE_TX_ADDRESS = "0xd666115c3d251bece7896297bf446ea908caf035"
MULTI_TXS_ADDRESS = "0xcef6d40144b0d76617664357a15559ecb145374f"

tasks = construct_tasks(
    chains=["op"],
    models=["dummy"],
    range_spec="@20241001:+1",
    read_from=DataLocation.GCS,
    write_to=[],
)

result = daily_address_summary(tasks[0])
result["daily_address_summary"].to_parquet("output.parquet", compression="zstd")
df = pl.scan_parquet("output.parquet")


def test_system_address_not_included():
    assert df.filter(pl.col("address") == SYSTEM_ADDRESS).collect().shape[0] == 0


def test_uniqueness():
    assert df.collect().n_unique(subset=["address", "chain_id", "dt"]) == df.collect().shape[0]


# tx ref link: https://optimistic.etherscan.io/tx/0x2ab7a335f3ecc0236ac9fc0c4832f4500d299ee40abf99e5609fa16309f82763
def test_single_txs_output():
    df_single_tx = df.filter(pl.col("address") == SINGLE_TX_ADDRESS).collect()

    assert df_single_tx.shape[0] == 1
    assert pl.select(df_single_tx["total_txs"]).item() == 1
    assert pl.select(df_single_tx["total_blocks"]).item() == 1
    assert pl.select(df_single_tx["block_interval_active"]).item() == 1
    assert pl.select(df_single_tx["num_to_addresses"]).item() == 1
    assert pl.select(df_single_tx["num_method_ids"]).item() == 1
    assert pl.select(df_single_tx["total_l2_gas_used"]).item() == 47038
    assert pl.select(df_single_tx["total_l1_gas_used"]).item() == 1600
    assert pl.select(df_single_tx["total_gas_fees"]).item() == Decimal(str(0.00000027230883812))
    assert pl.select(df_single_tx["l2_contrib_gas_fees"]).item() == Decimal(
        str(0.000000025959378478)
    )
    assert pl.select(df_single_tx["l1_contrib_gas_fees"]).item() == Decimal(
        str(0.000000246349459642)
    )
    assert (
        pl.select(df_single_tx["l1_contrib_gas_fees"]).item()
        + pl.select(df_single_tx["l2_contrib_gas_fees"]).item()
        == pl.select(df_single_tx["total_gas_fees"]).item()
    )
    assert (
        pl.select(df_single_tx["l1_contrib_contrib_gas_fees_blobgas"]).item()
        + pl.select(df_single_tx["l1_contrib_gas_fees_l1gas"]).item()
        == pl.select(df_single_tx["l1_contrib_gas_fees"]).item()
    )
    assert (
        pl.select(df_single_tx["l2_contrib_gas_fees_basefee"]).item()
        + pl.select(df_single_tx["l2_contrib_gas_fees_priorityfee"]).item()
        + pl.select(df_single_tx["l2_contrib_gas_fees_legacyfee"]).item()
        == pl.select(df_single_tx["l2_contrib_gas_fees"]).item()
    )
    assert pl.select(df_single_tx["avg_l2_gas_price_gwei"]).item() == Decimal(str(0.000551881))
    assert pl.select(df_single_tx["avg_l2_base_fee_gwei"]).item() == Decimal(str(0.000451881))
    assert pl.select(df_single_tx["avg_l2_priority_fee_gwei"]).item() == Decimal(str(0.0001))
    assert pl.select(df_single_tx["avg_l1_gas_price_gwei"]).item() == Decimal(str(29.456363538))


# https://optimistic.etherscan.io/tx/0x0c8c81cc9a97f4d66f4f78ef2bbb5a64c23358db2c4ba6ad35e338f2d2fa3535
# https://optimistic.etherscan.io/tx/0x8aa91fc3fb1c11cd4aba16130697a1fa52fd74ae7ee9f23627b6b8b42fec0a34
def test_multiple_txs_output():
    df_multi_txs = df.filter(pl.col("address") == MULTI_TXS_ADDRESS).collect()
    assert df_multi_txs.shape[0] == 1
    assert pl.select(df_multi_txs["total_txs"]).item() == 2
    assert pl.select(df_multi_txs["total_blocks"]).item() == 2
    assert (
        pl.select(df_multi_txs["block_interval_active"]).item()
        == pl.select(df_multi_txs["max_block_number"]).item()
        - pl.select(df_multi_txs["min_block_number"]).item()
        + 1
    )
    assert (
        pl.select(df_multi_txs["nonce_interval_active"]).item()
        == pl.select(df_multi_txs["max_nonce"]).item()
        - pl.select(df_multi_txs["min_nonce"]).item()
        + 1
    )
    assert (
        pl.select(df_multi_txs["time_interval_active"]).item()
        == pl.select(df_multi_txs["max_block_timestamp"]).item()
        - pl.select(df_multi_txs["min_block_timestamp"]).item()
    )
    assert pl.select(df_multi_txs["total_l2_gas_used"]).item() == 59852
    assert pl.select(df_multi_txs["total_l1_gas_used"]).item() == 3200
    assert pl.select(df_multi_txs["total_gas_fees"]).item() == Decimal(str(0.0000037408546364060))
    assert pl.select(df_multi_txs["l2_contrib_gas_fees"]).item() == Decimal(
        str(0.0000035911200000000)
    )
    assert pl.select(df_multi_txs["l1_contrib_gas_fees"]).item() == Decimal(
        str(0.0000001497346364060)
    )
