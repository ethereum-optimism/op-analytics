import json
from unittest.mock import patch

import polars as pl
from op_analytics.coreutils.testutils.inputdata import InputTestData

from op_analytics.datasources import l2beat

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


@patch("op_analytics.datasources.l2beat.get_data", mock_get_data)
def test_extract():
    actual = l2beat.pull_l2beat()
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
            "dt": "2024-11-02",
            "timestamp": 1730505600,
            "transaction_count": 6647620,
            "userops_count": 6891364,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-03",
            "timestamp": 1730592000,
            "transaction_count": 6102698,
            "userops_count": 6363131,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-04",
            "timestamp": 1730678400,
            "transaction_count": 5727220,
            "userops_count": 6001436,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-05",
            "timestamp": 1730764800,
            "transaction_count": 6959092,
            "userops_count": 7204823,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-06",
            "timestamp": 1730851200,
            "transaction_count": 7071391,
            "userops_count": 7264831,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-07",
            "timestamp": 1730937600,
            "transaction_count": 7370575,
            "userops_count": 7493368,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-08",
            "timestamp": 1731024000,
            "transaction_count": 6391504,
            "userops_count": 6486776,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-09",
            "timestamp": 1731110400,
            "transaction_count": 6785461,
            "userops_count": 6864956,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-10",
            "timestamp": 1731196800,
            "transaction_count": 6813213,
            "userops_count": 6923879,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-11",
            "timestamp": 1731283200,
            "transaction_count": 6912303,
            "userops_count": 7015249,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-12",
            "timestamp": 1731369600,
            "transaction_count": 7233770,
            "userops_count": 7328839,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-13",
            "timestamp": 1731456000,
            "transaction_count": 6659240,
            "userops_count": 6741825,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-14",
            "timestamp": 1731542400,
            "transaction_count": 6826029,
            "userops_count": 6886465,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-15",
            "timestamp": 1731628800,
            "transaction_count": 7277509,
            "userops_count": 7342899,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-16",
            "timestamp": 1731715200,
            "transaction_count": 6771846,
            "userops_count": 6885868,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-17",
            "timestamp": 1731801600,
            "transaction_count": 7249096,
            "userops_count": 7349182,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-18",
            "timestamp": 1731888000,
            "transaction_count": 7522561,
            "userops_count": 7612284,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-19",
            "timestamp": 1731974400,
            "transaction_count": 7679495,
            "userops_count": 7769243,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-20",
            "timestamp": 1732060800,
            "transaction_count": 7521649,
            "userops_count": 7670998,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-21",
            "timestamp": 1732147200,
            "transaction_count": 7297746,
            "userops_count": 7364826,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-22",
            "timestamp": 1732233600,
            "transaction_count": 7245066,
            "userops_count": 7384037,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-23",
            "timestamp": 1732320000,
            "transaction_count": 7054176,
            "userops_count": 7202284,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-24",
            "timestamp": 1732406400,
            "transaction_count": 9026805,
            "userops_count": 9181107,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-25",
            "timestamp": 1732492800,
            "transaction_count": 9233385,
            "userops_count": 9311427,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-26",
            "timestamp": 1732579200,
            "transaction_count": 11403883,
            "userops_count": 11448359,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-27",
            "timestamp": 1732665600,
            "transaction_count": 7224871,
            "userops_count": 7264108,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-28",
            "timestamp": 1732752000,
            "transaction_count": 10448610,
            "userops_count": 10486186,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-29",
            "timestamp": 1732838400,
            "transaction_count": 5901253,
            "userops_count": 5932253,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-11-30",
            "timestamp": 1732924800,
            "transaction_count": 6681417,
            "userops_count": 6712262,
            "id": "base",
            "slug": "base",
        },
        {
            "dt": "2024-12-01",
            "timestamp": 1733011200,
            "transaction_count": 8348023,
            "userops_count": 8414288,
            "id": "base",
            "slug": "base",
        },
    ]
