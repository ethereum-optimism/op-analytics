import polars as pl
from unittest.mock import patch
import datetime

from op_analytics.coreutils.partitioned.dailydatawrite import construct_marker


def test_construct_marker():
    with patch("op_analytics.coreutils.partitioned.marker.now") as m:
        m.return_value = datetime.datetime(2025, 2, 21, 5, 25)

        marker = construct_marker(
            root_path="my/path",
            datestr="2025-02-10",
            row_count=10,
            process_name="example",
        )
        actual = pl.from_arrow(marker).to_dicts()  # type: ignore
        assert actual == [
            {
                "updated_at": datetime.datetime(2025, 2, 21, 5, 25),
                "marker_path": "2025-02-10/my/path",
                "dataset_name": "",
                "root_path": "my/path",
                "num_parts": 1,
                "data_path": "my/path/dt=2025-02-10/out.parquet",
                "row_count": 10,
                "process_name": "example",
                "writer_name": "pedros-Apple-MacBook-Pro",
                "dt": datetime.date(2025, 2, 10),
            }
        ]
