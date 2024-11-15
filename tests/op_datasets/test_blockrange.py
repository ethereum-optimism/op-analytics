from op_analytics.datapipeline.utils.blockrange import BlockRange


def test_blockrange():
    br = BlockRange.from_spec("2000:2100")
    assert br == BlockRange(2000, 2100)
    assert len(br) == 100


def test_blockrange_plus():
    br = BlockRange.from_spec("200000:+100")
    assert br == BlockRange(200000, 200100)
    assert len(br) == 100
