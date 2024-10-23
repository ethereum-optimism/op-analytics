from op_datasets.etl.ingestion.sources import CoreDatasetSource, GoldskySource, LocalFileSource


def test_blockrange():
    br = CoreDatasetSource.from_spec("goldsky")
    assert br == GoldskySource()


def test_blockrange_plus():
    br = CoreDatasetSource.from_spec("file:///path/to/dir")
    assert br == LocalFileSource(basepath="/path/to/dir")
