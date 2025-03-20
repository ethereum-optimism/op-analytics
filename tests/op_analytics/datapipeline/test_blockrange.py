from op_analytics.coreutils.rangeutils.blockrange import BlockRange


def test_blockrange():
    br = BlockRange.from_spec("2000:2100")
    assert br == BlockRange(2000, 2100)
    assert len(br) == 100


def test_blockrange_plus():
    br = BlockRange.from_spec("200000:+100")
    assert br == BlockRange(200000, 200100)
    assert len(br) == 100


def test_blockrange_overlaps():
    br = BlockRange(10, 20)

    assert not br.overlaps(6, 9)
    assert br.overlaps(5, 10)
    assert br.overlaps(8, 12)
    assert br.overlaps(12, 14)
    assert br.overlaps(18, 23)
    assert not br.overlaps(20, 25)
    assert not br.overlaps(22, 30)
