from op_datasets.coretables.read import DataSource, GoldskySource, LocalFileSource


def test_blockrange():
    br = DataSource.from_spec("goldsky")
    assert br == GoldskySource()


def test_blockrange_plus():
    br = DataSource.from_spec("file:///path/to/dir")
    assert br == LocalFileSource(basepath="/path/to/dir")
