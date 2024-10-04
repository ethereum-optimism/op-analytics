import json
from op_coreutils.testutils.pathmanager import PathManager
from op_analytics.cli.subcommands.pulls.l2beat import tvl


def test_extract():
    testcase = PathManager.at(__file__)

    # Load the raw and cleaned up CSVs from the test path.
    with open(testcase.path("case01/l2beat_scaling_summary.json")) as fobj:
        resp = json.load(fobj)

    actual = tvl.extract(resp)

    breakpoint()
    assert actual == None
