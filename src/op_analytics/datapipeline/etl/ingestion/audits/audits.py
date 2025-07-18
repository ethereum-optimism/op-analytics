import polars as pl
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()

REGISTERED_AUDITS = {}


def register(func):
    REGISTERED_AUDITS[func.__name__] = func
    return func


VALID_HASH = r"^0x[\da-f]{64}$"


@register
def valid_hashes(chain: str, dataframes: dict[str, pl.DataFrame]):
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
def datasets_join_to_blocks(chain: str, dataframes: dict[str, pl.DataFrame]):
    blocks = dataframes["blocks"].select("number")

    # Check that other datasets can join back to a block by block number.
    results = []
    for dataset in ["transactions", "logs", "traces"]:
        df = dataframes[dataset].select("block_number")
        joined_df = df.join(blocks, left_on="block_number", right_on="number", how="full")

        failure_condition = pl.col("number").is_null()
        failures = joined_df.select(
            pl.lit(f"{dataset} should join back to blocks on block number").alias("audit_name"),
            failure_condition.sum().alias("failure_count"),
        )

        if len(joined_df.filter(failure_condition)) > 0:
            # print to debug failures
            print()
            print(f"dataset = {dataset}")
            print(joined_df.filter(failure_condition))
            print(joined_df.filter(failure_condition)["block_number"].unique().to_list())

        results.append(failures)

    return pl.concat(results)


@register
def monotonically_increasing(chain: str, dataframes: dict[str, pl.DataFrame]):
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

    cond1 = pl.col("number_diff") != 1
    check1 = diffs.select(
        pl.lit("block number must be monotonically increasing").alias("audit_name"),
        cond1.sum().alias("failure_count"),
    )
    if len(diffs.filter(cond1)) > 0:
        print(diffs.filter(cond1))  # helps debug failures

    cond2 = pl.col("timestamp_diff") < 0
    check2 = diffs.select(
        pl.lit("block timestamp must be monotonically increasing").alias("audit_name"),
        cond2.sum().alias("failure_count"),
    )
    if len(diffs.filter(cond2)) > 0:
        print(diffs.filter(cond2))  # helps debug failures

    result = pl.concat([check1, check2])

    return result


@register
def distinct_block_numbers(chain: str, dataframes: dict[str, pl.DataFrame]):
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
def dataset_consistent_block_timestamps(chain: str, dataframes: dict[str, pl.DataFrame]):
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

        missing_timestamp = pl.col("timestamp").is_null()
        mismatching_timestamp = pl.col("block_timestamp") != pl.col("timestamp")
        fail_timestamp = missing_timestamp | mismatching_timestamp

        timestamp_check = joined.select(
            pl.lit(f"{name} block timestamp must agree with blocks dataframe").alias("audit_name"),
            fail_timestamp.sum().alias("failure_count"),
        )
        audits.append(timestamp_check)

        missing_dt = pl.col("dt").is_null()
        mismatching_dt = pl.col("block_dt") != pl.col("dt")
        fail_dt = missing_dt | mismatching_dt

        dt_check = joined.select(
            pl.lit(f"{name} block dt must agree with blocks dataframe").alias("audit_name"),
            fail_dt.sum().alias("failure_count"),
        )
        audits.append(dt_check)

    return pl.concat(audits)


@register
def transaction_count(chain: str, dataframes: dict[str, pl.DataFrame]):
    """Block transaction count should agree with number of transaction records."""
    blocks_df = dataframes["blocks"].select("number", "transaction_count")
    transactions_df = dataframes["transactions"].select("block_number")

    tx_count = transactions_df.group_by("block_number").len(name="txs_df_count")
    joined = blocks_df.join(
        tx_count, left_on="number", right_on="block_number", how="full", validate="1:1"
    )

    # Check that the block tx count agrees with the number of transactions.
    no_txs = pl.col("txs_df_count").is_null()
    mismatching_count = pl.col("transaction_count") != pl.col("txs_df_count")

    if chain == "ethereum":
        failure_condition = (
            (no_txs & (pl.col("transaction_count") != 0)) |
            (mismatching_count & ~no_txs)  # Only check mismatch when txs_df_count is not null
        )
    else:
        failure_condition = no_txs | mismatching_count

    check = joined.select(
        pl.lit("block transaction count must match observed transactions").alias("audit_name"),
        failure_condition.sum().alias("failure_count"),
    )
    if len(joined.filter(failure_condition)) > 0:
        # print to debug failures
        print(joined.filter(failure_condition))
        print(joined.filter(failure_condition)["number"].to_list())

    return check


@register
def logs_count(chain: str, dataframes: dict[str, pl.DataFrame]):
    """Block log max_index should agree with number of log records."""
    logs_df = dataframes["logs"].select("block_number", "log_index")

    agged = logs_df.group_by("block_number").agg(
        (1 + pl.max("log_index")).alias("max_log_idx"), pl.count()
    )

    failure_condition = pl.col("max_log_idx") != pl.col("count")
    check = agged.select(
        pl.lit("log max index must equal log count").alias("audit_name"),
        failure_condition.sum().alias("failure_count"),
    )

    if len(agged.filter(failure_condition)) > 0:
        # print to debug failures
        print(agged.filter(failure_condition))
        print(agged.filter(failure_condition)["block_number"].unique().to_list())

    return check


@register
def traces_count(chain: str, dataframes: dict[str, pl.DataFrame]):
    """There should be at least one trace per transaction."""

    # (pedro - 2025/03/02) We currently don't have traces for Kroma. But we are letting
    # it go through ingestion anyways.
    if chain == "kroma":
        return None

    traces_count = (
        dataframes["traces"].group_by("block_number", "transaction_hash").len("num_traces")
    )
    tx_count = (
        dataframes["transactions"]
        .select(
            pl.col("block_number"),
            pl.col("hash").alias("transaction_hash"),
            pl.lit(1).alias("num_txs"),
        )
        .unique()
    )

    joined = tx_count.join(
        traces_count,
        left_on=["block_number", "transaction_hash"],
        right_on=["block_number", "transaction_hash"],
        how="full",
        validate="1:1",
    )

    no_traces = pl.col("num_traces").is_null()
    missing_traces = pl.col("num_txs") > pl.col("num_traces")

    failure_condition = no_traces | missing_traces
    check = joined.select(
        pl.lit("block has missing traces").alias("audit_name"),
        failure_condition.sum().alias("failure_count"),
    )

    if len(joined.filter(failure_condition)) > 0:
        # print to debug failures
        print(joined.filter(failure_condition))

    return check


def dataset_valid_logs():
    # TODO: Write an audit to make sure that the length of the logs data string is always a
    # multiple of 32: (length(data)-2) % 32 == 0

    # TODO: Write an audit to make sure that the length of the topics data string is always
    # valid.  66, 133, 200, 267
    pass
