from datetime import date

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.coreutils.threads import run_concurrently

from .insert import BlockBatch, ClickHouseBlockBatchETL, InsertTask
from .markers import candidate_markers, existing_markers

log = structlog.get_logger()


def load_to_clickhouse(
    dataset: ClickHouseBlockBatchETL,
    range_spec: str | None = None,
    dry_run: bool = False,
):
    """Insert blockbatch data into Clickhouse.

    Find the blockbatches that have not yet been loaded to Clickhouse and schedule
    one insert task per blockbatch.

    The GCSData objects describe the data to be loaded. The `output_root_path` should
    match the `__CREATE.sql` and `__INSERT.sql` files in the "ddl" subdir.

    These two sql files control the structure of the destination ClickHouse table and
    the query that is used to read data from GCS and transform it before inserting it.

    The `input_root_paths` in the GCSData objects describe which blocktbach models are
    read by the INSERT query.

    When this functionality was first introduced we would load blockbatch models into
    ClickHouse without any filtering or transformations. But later on we evolved to
    supporting more complex SQL queries which can include more than one input blockbatch
    model and produce entirely new data going into ClickHouse.

    CREATE sql files
    ----------------

    The CREATE sql files are located under `ddl/<root_path>__CREATE.sql`. Note
    that `<root_path>` has slashes, so the files will appear nested in the file system.

    To avoid user error in table names the _placeholder_ placeholder is used in the CREATE
    files. This placeholder is replaced with the actual table name which is derived from
    the `output_root_path` in the GCSData object.

    INSERT sql files
    ----------------

    The INSERT sql files are located under `ddl/<root_path>__INSERT.sql`. There
    should be one INSERT sql file per CREATE sql file.

    Input root paths are indicated in the INSERT sql files using the `gcs__<sanitized_root_path>`
    placeholder. The sanitized root path is the root path with slashes replaced by double
    underscores. For example:

      root path: "blockbatch/contract_creation/create_traces_v1"
      sanitized: "blockbatch__contract_creation__create_traces_v1"

    At runtime the placeholder will be replaced with the ClickHouse s3() table function call
    that provides the actual data in GCS for the blockbatch being processed.
    """

    # Operate over recent days.
    date_range = DateRange.from_spec(range_spec or "m7days")

    # Create the output tables if they don't exist.
    if not dry_run:
        dataset.create_table()

    # Candidate markers are for blockbatches produced upstream.
    candidate_markers_df = candidate_markers(
        date_range=date_range,
        root_paths=dataset.input_root_paths,
    )

    # Existing markers that have already been loaded to ClickHouse.
    existing_markers_df = existing_markers(date_range)

    # Loop over datasets and find which ones are pending.
    tasks: list[InsertTask] = []

    # Candidate markers for this dataset.
    candidates = (
        candidate_markers_df.filter(pl.col("root_path").is_in(dataset.input_root_paths))
        .select(
            pl.col("root_path"),
            pl.col("chain"),
            pl.col("dt"),
            pl.col("min_block"),
            # The full data path looks like: "blockbatch/contract_creation/create_traces_v1/chain=automata/dt=2025-03-21/000010664000.parquet"
            # The partitioned path is the part after the root path: "chain=automata/dt=2025-03-21/000010664000.parquet"
            pl.col("data_path")
            .str.split(pl.col("root_path") + "/")
            .list.last()
            .alias("partitioned_path"),
        )
        .unique()
        .group_by("chain", "dt", "min_block", "partitioned_path")
        .all()
    ).to_dicts()

    completed_batches = set(
        [
            (x["chain"], x["dt"], x["min_block"])
            for x in existing_markers_df.filter(pl.col("root_path") == dataset.output_root_path)
            .select("chain", "dt", "min_block")
            .to_dicts()
        ]
    )

    for block_batch in candidates:
        ready = block_batch["root_path"]

        # Skip batches that don't have all the required input root_paths.
        if set(dataset.input_root_paths) != set(ready):
            # If needed for debugging:
            # missing = sorted(set(d.input_root_paths) - set(ready))
            continue

        # Skip batches that have already been processed.
        as_tuple = (block_batch["chain"], block_batch["dt"], block_batch["min_block"])
        if as_tuple in completed_batches:
            continue

        # Collect the pending batches.

        assert isinstance(block_batch["chain"], str)
        assert isinstance(block_batch["dt"], date)
        assert isinstance(block_batch["min_block"], int)
        assert isinstance(block_batch["partitioned_path"], str)

        insert_task = InsertTask(
            dataset=dataset,
            blockbatch=BlockBatch(
                chain=block_batch["chain"],
                dt=block_batch["dt"],
                min_block=block_batch["min_block"],
                partitioned_path=block_batch["partitioned_path"],
            ),
        )
        tasks.append(insert_task)

    log.info(f"Prepared {len(tasks)} insert tasks.")

    # Sort tasks by date (should still be somewhat random by chain)
    tasks.sort(key=lambda x: x.blockbatch.dt)

    if dry_run and tasks:
        tasks[0].dry_run()
        log.warning("DRY RUN: Only the first task is shown.")
        return

    summary = run_concurrently(
        function=lambda x: x.execute(),
        targets={t.key: t for t in tasks},
        max_workers=5,
    )

    return summary
