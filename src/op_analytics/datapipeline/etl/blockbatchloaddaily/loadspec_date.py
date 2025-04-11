from collections import defaultdict
from dataclasses import dataclass, field

from op_analytics.coreutils.logger import structlog
from op_analytics.datapipeline.chains.goldsky_chains import goldsky_mainnet_chains

from .loadspec_datechain import ETLMixin
from .readers import DateChainBatch, construct_batches, ALL_CHAINS_SENTINEL


log = structlog.get_logger()


@dataclass
class ClickHouseDateETL(ETLMixin):
    """Represent a task to load data to ClickHouse by dt."""

    # Output root path determines the ClickHouse table name where data will be loaded.
    output_root_path: str

    # This is the list of blockbatch root paths that are inputs to this load task.
    inputs_blockbatch: list[str] = field(default_factory=list)

    # This is the list of ClickHouse root paths that are inputs to this load task.
    inputs_clickhouse: list[str] = field(default_factory=list)

    def pending_batches(
        self, range_spec: str, chains: list[str] | None = None
    ) -> list[DateChainBatch]:
        if chains is not None:
            raise Exception("Must not provide specific chains when running a date level ETL.")

        chains = goldsky_mainnet_chains()

        ready_datechain_batches: list[DateChainBatch] = construct_batches(
            range_spec=range_spec,
            chains=chains,
            blockbatch_root_paths=self.inputs_blockbatch,
            clickhouse_root_paths=self.inputs_clickhouse,
        )

        # Group the ready batches by date. This is so we can look at a single
        # date and determine if it is ready to be procesed.
        by_date = defaultdict(list)
        for batch in ready_datechain_batches:
            by_date[batch.dt].append(batch.chain)

        # Check each date and see if it is ready to be processed.
        ready_date_batches: list[DateChainBatch] = []
        for date, batches in by_date.items():
            ok = set(batches) == set(chains)
            missing = set(chains) - set(batches)

            if ok:
                ready_date_batches.append(
                    DateChainBatch(
                        dt=date,
                        chain=ALL_CHAINS_SENTINEL,
                    )
                )
            else:
                log.warning(f"Missing chains for date {date}: {sorted(missing)}")

        # Existing markers that have already been loaded to ClickHouse.
        existing_markers = self.existing_markers(
            range_spec=range_spec,
            chains=[ALL_CHAINS_SENTINEL],
        )

        # Loop over batches and find which ones are pending.
        batches: list[DateChainBatch] = []
        for batch in ready_date_batches:
            if batch in existing_markers:
                continue
            batches.append(batch)
        log.info(f"{len(batches)}/{len(ready_date_batches)} pending dt insert tasks.")

        return batches
