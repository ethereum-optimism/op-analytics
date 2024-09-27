import polars as pl
from op_coreutils.testutils.pathmanager import PathManager
from op_coreutils.testutils.dataframe import compare_dataframes

from op_analytics.cli.subcommands.chains import chain_metadata


def test_clean():
    testcase = PathManager.at(__file__)

    # Load the raw and cleaned up CSVs from the test path.
    raw_df = pl.read_csv(testcase.path("case01/chain_metadata_raw.csv"))
    expected_clean_df = pl.read_csv(testcase.path("case01/chain_metadata.csv"))

    # Clean the CSV.
    actual_clean_df = chain_metadata._clean(raw_df)

    # Chekc that results are as expected.
    compare_dataframes(actual_clean_df, expected_clean_df)


def test_goldsky_chains():
    testcase = PathManager.at(__file__)

    actual = chain_metadata.goldsky_chains(testcase.path("case01/chain_metadata_raw.csv"))
    expected = [
        "base",
        "bob",
        "cyber",
        "fraxtal",
        "ham",
        "kroma",
        "lisk",
        "lyra",
        "metal",
        "mint",
        "mode",
        "op",
        "orderly",
        "polynomial",
        "race",
        "redstone",
        "swan",
        "xterio",
        "zora",
    ]
    assert actual == expected
