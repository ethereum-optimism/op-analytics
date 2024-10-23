from op_datasets.utils.daterange import DateRange
from op_datasets.utils.blockrange import BlockRange
from op_datasets.schemas import ONCHAIN_CURRENT_VERSION

from op_coreutils.clickhouse import run_goldsky_query


def block_range_for_dates(chain: str, date_spec: str):
    date_range = DateRange.from_spec(date_spec)

    if len(date_range) > 2:
        raise ValueError("We don't recommend processing as much data at once.")

    query = ONCHAIN_CURRENT_VERSION["blocks"].goldsky_sql_find_blocks_for_dates(
        source_table=f"{chain}_blocks",
    )

    params = {
        "mints": date_range.min_ts,
        "maxts": date_range.max_ts,
    }

    result = run_goldsky_query(query=query, parameters=params)
    assert len(result) == 1

    row = result.to_dicts()[0]
    return BlockRange(row["block_min"], row["block_max"])
