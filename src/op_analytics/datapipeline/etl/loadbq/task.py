from collections import defaultdict
from dataclasses import dataclass
from datetime import date

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.time import date_fromstr


log = structlog.get_logger()


@dataclass
class DateLoadTask:
    """Task to load all data for a given date to BigQuery."""

    dateval: date
    dataset_paths: dict[str, list[str]]
    chains_ready: set[str]
    chains_not_ready: set[str]

    @property
    def contextvars(self):
        return {"date": self.dateval.strftime("%Y-%m-%d")}


def consolidate_chains(inputs: list[DataReader]) -> list[DateLoadTask]:
    """Consolidate inputs.

    list[InputData] has separate entries for each chain and date. This function goes over
    it and collects a single DateLoadTask which covers all chains.
    """
    date_tasks: dict[date, DateLoadTask] = {}
    for inputdata in inputs:
        dateval = date_fromstr(inputdata.partition_value("dt"))
        chain = inputdata.partition_value("chain")

        if dateval not in date_tasks:
            date_tasks[dateval] = DateLoadTask(
                dateval=dateval,
                dataset_paths=defaultdict(list),
                chains_ready=set(),
                chains_not_ready=set(),
            )

        task = date_tasks[dateval]
        if not inputdata.inputs_ready:
            task.chains_not_ready.add(chain)
        else:
            task.chains_ready.add(chain)

        for dataset, paths in inputdata.dataset_paths.items():
            task.dataset_paths[dataset].extend(paths)

    result = list(date_tasks.values())
    log.info(f"Consolidated to {len(result)} dateval tasks.")
    return result
