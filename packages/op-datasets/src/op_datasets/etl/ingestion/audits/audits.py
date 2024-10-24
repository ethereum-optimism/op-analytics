import polars as pl
from op_coreutils.logger import structlog

log = structlog.get_logger()

REGISTERED_AUDITS = {}


def register(func):
    REGISTERED_AUDITS[func.__name__] = func
    return func


VALID_HASH = r"^0x[\da-f]{64}$"


@register
def valid_hashes(dataframes: dict[str, pl.DataFrame]):
    # 1. Check that all hashes are valid.
    block_hashes = dataframes["blocks"].select(
        pl.lit("all block hashes must be valid").alias("audit_name"),
        (~pl.col("hash").str.contains(VALID_HASH)).sum().alias("failure_count"),
    )

    tx_hashes = dataframes["transactions"].select(
        pl.lit("all tx hashes must be valid").alias("audit_name"),
        (~pl.col("hash").str.contains(VALID_HASH)).sum().alias("failure_count"),
    )
    return pl.concat([block_hashes, tx_hashes])


@register
def txs_join_to_blocks(dataframes: dict[str, pl.DataFrame]):
    # Check that each transaction can join back to a block by block number.
    blks = dataframes["blocks"].select("number")
    tx = dataframes["transactions"].select("hash", "block_number")
    joined_df = blks.join(tx, left_on="number", right_on="block_number", how="full")
    joined_txs = joined_df.select(
        pl.lit("txs should join back to blocks on block number").alias("audit_name"),
        pl.col("block_number").is_null().sum().alias("failure_count"),
    )

    return joined_txs


@register
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
                pl.lit("block number must be monotonically increasing").alias("audit_name"),
                (pl.col("number_diff") != 1).sum().alias("failure_count"),
            ),
            diffs.select(
                pl.lit("block timestamp must be monotonically increasing").alias("audit_name"),
                (pl.col("timestamp_diff") < 0).sum().alias("failure_count"),
            ),
        ]
    )

    return result


@register
def distinct_block_numbers(dataframes: dict[str, pl.DataFrame]):
    ctx = pl.SQLContext(frames=dataframes)
    result = ctx.execute(
        """
        SELECT 
            'block numbers must be distinct' AS audit_name,
            count(*) - count(distinct number) AS failure_count
        FROM blocks
        """
    ).collect()

    return result


@register
def dataset_consistent_block_timestamps(dataframes: dict[str, pl.DataFrame]):
    """Make sure the block_timestamp is correct in the non-block datasets."""

    blocks_df = dataframes["blocks"].select("number", "timestamp", "dt")

    audits = []
    for name, dataframe in dataframes.items():
        if name == "blocks":
            continue

        joined = dataframe.select(
            "block_number", "block_timestamp", pl.col("dt").alias("block_dt")
        ).join(
            blocks_df,
            left_on="block_number",
            right_on="number",
            how="left",
            validate="m:1",
        )
        timestamp_check = joined.select(
            pl.lit(f"{name} block timestamp must agree with blocks dataframe").alias("audit_name"),
            (pl.col("block_timestamp") != pl.col("timestamp")).sum().alias("failure_count"),
        )
        audits.append(timestamp_check)

        dt_check = joined.select(
            pl.lit(f"{name} block dt must agree with blocks dataframe").alias("audit_name"),
            (pl.col("block_dt") != pl.col("dt")).sum().alias("failure_count"),
        )
        audits.append(dt_check)

    return pl.concat(audits)


def dataset_valid_logs():
    # TODO: Write an audit to make sure that the length of the logs data string is always a
    # multiple of 32: (length(data)-2) % 32 == 0

    # TODO: Write an audit to make sure that the length of the topics data string is always
    # valid.  66, 133, 200, 267
    pass
