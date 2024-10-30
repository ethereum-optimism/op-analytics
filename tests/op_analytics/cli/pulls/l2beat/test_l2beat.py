import json
from unittest.mock import patch

import polars as pl
from op_coreutils.testutils.inputdata import InputTestData

from op_analytics.cli.subcommands.pulls import l2beat

TESTDATA = InputTestData.at(__file__)


def mock_get_data(session, url):
    if url == l2beat.SUMMARY_ENDPOINT:
        with open(TESTDATA.path("mockdata/l2beat_scaling_summary.json")) as fobj:
            return json.load(fobj)

    # Mock data for base TVL
    if url.startswith("https://l2beat.com/api/scaling/tvl/base"):
        with open(TESTDATA.path("mockdata/l2beat_tvl_base.json")) as fobj:
            return json.load(fobj)

    # Use the taiko data for all other TVLs so we don't have to have as many mocks.
    if url.startswith("https://l2beat.com/api/scaling/tvl/"):
        with open(TESTDATA.path("mockdata/l2beat_tvl_taiko.json")) as fobj:
            return json.load(fobj)

    # Mock data for base ACTIVITY
    if url.startswith("https://l2beat.com/api/scaling/activity/base"):
        with open(TESTDATA.path("mockdata/l2beat_activity_base.json")) as fobj:
            return json.load(fobj)

    # Use the taiko data for all other ACTIVITYs so we don't have to have as many mocks.
    if url.startswith("https://l2beat.com/api/scaling/activity/"):
        with open(TESTDATA.path("mockdata/l2beat_activity_taiko.json")) as fobj:
            return json.load(fobj)

    raise NotImplementedError({url})


@patch("op_analytics.cli.subcommands.pulls.l2beat.get_data", mock_get_data)
def test_extract():
    actual = l2beat.pull()
    summary_df = actual["summary"]
    assert len(summary_df) == 105

    tvl_df = actual["tvl"]
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
        "slug": "base",
    }

    activity_df = actual["activity"]
    assert len(activity_df) == 3150
    base_activity = activity_df.filter(
        pl.col("dt") >= "2024-10-25", pl.col("id") == "base"
    ).to_dicts()
    assert base_activity == [
        {
            "dt": "2024-10-25",
            "timestamp": 1729814400,
            "transaction_count": 6429517,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-10-26",
            "timestamp": 1729900800,
            "transaction_count": 6514485,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-10-27",
            "timestamp": 1729987200,
            "transaction_count": 5998605,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-10-28",
            "timestamp": 1730073600,
            "transaction_count": 6312506,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-10-29",
            "timestamp": 1730160000,
            "transaction_count": 6905505,
            "id": "base",
            "slug": "base",
        },
    ]
