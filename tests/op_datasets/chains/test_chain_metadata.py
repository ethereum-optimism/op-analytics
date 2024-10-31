# -*- coding: utf-8 -*-
import polars as pl
from op_coreutils.testutils.inputdata import InputTestData
from op_coreutils.testutils.dataframe import compare_dataframes
from unittest.mock import patch, MagicMock
from op_datasets.chains import chain_metadata


def test_clean():
    testcase = InputTestData.at(__file__)

    # Load the raw and cleaned up CSVs from the test path.
    raw_df = pl.read_csv(testcase.path("case01/chain_metadata_raw.csv"))
    expected_clean_df = pl.read_csv(testcase.path("case01/chain_metadata.csv"))

    # Clean the CSV.
    actual_clean_df = chain_metadata._clean(raw_df)

    # Chekc that results are as expected.
    compare_dataframes(actual_clean_df, expected_clean_df)


def test_to_pandas():
    testcase = InputTestData.at(__file__)

    # Load the raw and cleaned up CSVs from the test path.
    raw_df = pl.read_csv(testcase.path("case01/chain_metadata_raw.csv"))

    # Clean the CSV.
    actual_clean_df = chain_metadata._clean(raw_df)

    pddf = chain_metadata.to_pandas(actual_clean_df)

    assert pddf["mainnet_chain_id"].to_list() == [
        "10",
        "288",
        "1088",
        "881",
        "2999",
        "5151706",
        "7777777",
        "570",
        "5000",
        "424",
        "8453",
        "255",
        "169",
        "204",
        "291",
        "957",
        "34443",
        "252",
        "5101",
        "888888888",
        "4653",
        "2410",
        "8866",
        "81457",
        "78225",
        "12553",
        "1750",
        "690",
        "60808",
        "1135",
        "56026",
        "185",
        "7560",
        "62050",
        "5112",
        "2702128",
        "254",
        "969",
        "42026",
        "698",
        "6805",
        "48900",
        "8008",
        "33979",
        "2192",
        "NA",
        "480",
        "NA",
        "NA",
        "116",
        "5330",
        "NA",
        "NA",
        "NA",
        "NA",
        "NA",
        "NA",
        "NA",
        "NA",
        "NA",
        "NA",
        "NA",
        "NA",
        "NA",
        "360",
        "NA",
        "NA",
        "NA",
        "NA",
        "NA",
        "NA",
        "328527",
        "624",
        "8853",
        "65536",
    ]


def test_goldsky_chains():
    testcase = InputTestData.at(__file__)
    # Load the raw data you want to return from read_gsheet
    raw_gsheet_data = pl.read_csv(
        testcase.path("case01/chain_metadata_raw.csv")
    ).to_dicts()

    with patch("op_coreutils.gsheets.get_worksheet") as mock_get_worksheet:
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_records.return_value = raw_gsheet_data
        mock_get_worksheet.return_value = mock_worksheet

        actual = chain_metadata.goldsky_chains(
            testcase.path("case01/chain_metadata_raw.csv")
        )
        # Rest of your test code

        actual.sort()
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
            "shape",
            "swan",
            "worldchain",
            "xterio",
            "zora",
        ]
        assert actual == expected
