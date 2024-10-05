from op_datasets.datastores import DataSource, GoldskySource, LocalFileSource


def test_blockrange():
    br = DataSource.from_spec("goldsky")
    assert br == GoldskySource()


def test_blockrange_plus():
    br = DataSource.from_spec("local:/path/to/dir")
    assert br == LocalFileSource(rootpath="/path/to/dir")
