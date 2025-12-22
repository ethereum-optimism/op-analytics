import json
import os

import pytest

from op_analytics.datasources.defillama.protocolstvl.protocol import ProtocolTVL


def test_parse_protocol():
    """Test parsing with malformed JSON response from DefiLlama.

    The response from DefiLlama is malformed. The ["chainTvls"]["Base"]["tvl"] values are present but
    the breakdowns by token are missing.  Both ["chainTvls"]["Base"]["tokensInUsd"] and
    ["chainTvls"]["Base"]["tokens"] are null.
    """
    with open(os.path.join(os.path.dirname(__file__), "aerodrome-slipstream.json"), "r") as f:
        jsondata = json.load(f)

    with pytest.raises(Exception) as ex:
        ProtocolTVL.of(slug="aerodrome-slipstream", data=jsondata)

    assert ex.value.args == (
        "Error processing data for slug=aerodrome-slipstream: Incomplete [chainTvls][Base][tokens] entries for slug=aerodrome-slipstream, chain=Base: 342 tvl entries and 0 tokens entries.",
    )
