import polars as pl
from op_coreutils.logger import structlog

log = structlog.get_logger()


VALID_HASH = r"^0x[\da-f]{64}$"


def valid_hashes(dataframes: dict[str, pl.DataFrame]):
    # 1. Check that all hashes are valid.
    block_hashes = dataframes["blocks"].select(
        pl.lit("all block hashes are valid").alias("audit_name"),
        (~pl.col("hash").str.contains(VALID_HASH)).sum().alias("failure_count"),
    )

    tx_hashes = dataframes["transactions"].select(
        pl.lit("all tx hashes are valid").alias("audit_name"),
        (~pl.col("hash").str.contains(VALID_HASH)).sum().alias("failure_count"),
    )
    return pl.concat([block_hashes, tx_hashes])


def txs_join_to_blocks(dataframes: dict[str, pl.DataFrame]):
    # Check that each transaction can join back to a block by block number.
    blks = dataframes["blocks"].select("number")
    tx = dataframes["transactions"].select("hash", "block_number")
    joined_df = blks.join(tx, left_on="number", right_on="block_number", how="full")
    joined_txs = joined_df.select(
        pl.lit("txs that dont join to blocks").alias("audit_name"),
        pl.col("block_number").is_null().sum().alias("failure_count"),
    )

    return joined_txs


def monotonically_increasing(dataframes: dict[str, pl.DataFrame]):
    diffs = (
        dataframes["blocks"]
        .sort("number")
        .select(
            "number",
            "timestamp",
            (pl.col("number") - pl.col("number").shift(1)).alias("number_diff"),
            (pl.col("timestamp") - pl.col("timestamp").shift(1)).alias("timestamp_diff"),
        )
        .filter(pl.col("number_diff").is_not_null())  # ignore the first row
    )

    result = pl.concat(
        [
            diffs.select(
                pl.lit("block number is monotonically increasing").alias("audit_name"),
                (pl.col("number_diff") != 1).sum().alias("failure_count"),
            ),
            diffs.select(
                pl.lit("block timestamp is monotonically increasing").alias("audit_name"),
                (pl.col("timestamp_diff") < 0).sum().alias("failure_count"),
            ),
        ]
    )

    return result


def distinct_block_numbers(dataframes: dict[str, pl.DataFrame]):
    ctx = pl.SQLContext(frames=dataframes)
    result = ctx.execute(
        """
        SELECT 
            'distinct_block_numbers' AS audit_name,
            count(*) - count(distinct number) AS failure_count
        FROM blocks
        """
    ).collect()

    return result


def dataset_consistent_block_timestamps(dataframes: dict[str, pl.DataFrame]):
    # TODO: Write an audit to make sure the block_timestamp is correct in all of the
    # non-block datasets.
    pass


def dataset_valid_logs():
    # TODO: Write an audit to make sure that the length of the logs data string is always a
    # multiple of 32: (length(data)-2) % 32 == 0

    # TODO: Write an audit to make sure that the length of the topics data string is always
    # valid.  66, 133, 200, 267
    pass
