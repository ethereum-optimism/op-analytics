import json
from unittest.mock import patch

import polars as pl

from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.etl.blockbatch.construct import construct_tasks


def make_dataframe(path: str):
    with open(InputTestData.at(__file__).path(f"testdata/{path}")) as fobj:
        return pl.DataFrame(
            json.load(fobj),
            schema={
                "dt": pl.UInt16(),
                "chain": pl.String(),
                "num_blocks": pl.Int32(),
                "min_block": pl.Int64(),
                "max_block": pl.Int64(),
                "data_path": pl.String(),
                "root_path": pl.String(),
            },
        )


def test_construct():
    with patch("op_analytics.coreutils.partitioned.dataaccess.run_query_oplabs") as m1:
        m1.return_value = make_dataframe("mainnet_markers.json")
        tasks = construct_tasks(
            chains=["mode"],
            models=["contract_creation"],
            range_spec="@20241201:+1",
            read_from=DataLocation.GCS,
            write_to=DataLocation.GCS,
        )

    assert tasks == []
