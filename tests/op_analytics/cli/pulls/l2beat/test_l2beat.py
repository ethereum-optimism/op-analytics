import json
from unittest.mock import patch

import polars as pl
from op_coreutils.testutils.pathmanager import PathManager

from op_analytics.cli.subcommands.pulls import l2beat

TESTCASE = PathManager.at(__file__)


def mock_get_data(session, url):
    if url == l2beat.SUMMARY_ENDPOINT:
        with open(TESTCASE.path("mockdata/l2beat_scaling_summary.json")) as fobj:
            return json.load(fobj)

    if url.startswith("https://l2beat.com/api/scaling/tvl/base"):
        with open(TESTCASE.path("mockdata/l2beat_tvl_base.json")) as fobj:
            return json.load(fobj)

    # Use the taiko data for all other chains so we don't have to have as many mocks.
    if url.startswith("https://l2beat.com/api/scaling/tvl/"):
        with open(TESTCASE.path("mockdata/l2beat_tvl_taiko.json")) as fobj:
            return json.load(fobj)

    raise NotImplementedError({url})


@patch("op_analytics.cli.subcommands.pulls.l2beat.get_data", mock_get_data)
def test_extract():
    actual = l2beat.pull()
    summary_df = actual["summary"]
    tvl_df = actual["tvl"]

    assert len(summary_df) == 105
    assert len(tvl_df) == 3255

    base_tvl = tvl_df.filter(pl.col("dt") == "2024-10-05", pl.col("id") == "base").to_dicts()[0]
    assert base_tvl == {
        "dt": "2024-10-05",
        "timestamp": 1728151200,
        "native": 5370357884.74,
        "canonical": 1644922301.97,
        "external": 79759467.7,
        "ethPrice": 2409.8306,
        "id": "base",
    }
