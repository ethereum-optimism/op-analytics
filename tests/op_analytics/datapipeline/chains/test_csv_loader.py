import polars as pl
import pytest
from op_analytics.datapipeline.chains.loaders.csv_loader import CsvChainMetadataLoader
import tempfile
import os

# Mock DataFrame with all required columns as expected by the loader
SAMPLE_DF = pl.DataFrame(
    {
        "chain_id": [1, 2],
        "chain_name": ["TestChain", "AnotherChain"],
        "display_name": ["Test Chain", "Another Chain"],
        "source_name": ["example_source", "example_source"],
        "source_rank": [1, 2],
        "dt_day": ["2024-01-01", "2024-01-02 "],  # Note the trailing space for test
    }
)

SAMPLE_CSV = tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".csv")
SAMPLE_DF.write_csv(SAMPLE_CSV.name)
SAMPLE_CSV.close()


def test_csv_chain_metadata_loader_success():
    loader = CsvChainMetadataLoader(csv_path=SAMPLE_CSV.name)
    df = loader.run()
    required = set(loader.REQUIRED_FIELDS)
    assert required.issubset(set(df.columns)), (
        f"Missing required columns: {required - set(df.columns)}"
    )
    assert df.shape[0] == 2
    assert df["chain_id"].to_list() == [1, 2]
    assert df["chain_name"].to_list() == ["TestChain", "AnotherChain"]
    assert df["display_name"].to_list() == ["Test Chain", "Another Chain"]
    assert df["source_name"].to_list() == ["example_source", "example_source"]
    assert df["source_rank"].to_list() == [1, 2]
    assert df["dt_day"].to_list() == ["2024-01-01", "2024-01-02"]
    os.unlink(SAMPLE_CSV.name)


def test_csv_chain_metadata_loader_missing_required():
    df = pl.DataFrame(
        {
            "chain_id": [1],
            "chain_name": ["TestChain"],
            # "display_name" missing
            "source_name": ["example_source"],
            "source_rank": [1],
        }
    )
    tmp_path = "tests/op_analytics/datapipeline/chains/inputs/tmp_missing_required.csv"
    df.write_csv(tmp_path)
    loader = CsvChainMetadataLoader(csv_path=tmp_path)
    with pytest.raises(ValueError):
        loader.run()
