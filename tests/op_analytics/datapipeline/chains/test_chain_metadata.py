import polars as pl
from op_analytics.coreutils.testutils.inputdata import InputTestData
from unittest.mock import patch, MagicMock
from op_analytics.datapipeline.chains import load, upload, goldsky_chains


def test_clean():
    testcase = InputTestData.at(__file__)

    # Load the raw and cleaned up CSVs from the test path.
    raw_df = pl.read_csv(testcase.path("case01/chain_metadata_raw.csv"))

    # Clean the CSV.
    actual_clean_df = load._clean(raw_df)

    # Check that results are as expected.
    assert actual_clean_df.schema == {
        "chain_name": pl.String(),
        "display_name": pl.String(),
        "mainnet_chain_id": pl.Int64,
        "hex_color": pl.String(),
        "public_mainnet_launch_date": pl.String(),
        "op_based_version": pl.String(),
        "chain_type": pl.String(),
        "op_chain_start": pl.String(),
        "has_mods": pl.String(),
        "raas_deployer": pl.String(),
        "rpc_url": pl.String(),
        "product_website": pl.String(),
        "block_explorer_url": pl.String(),
        "github_url": pl.String(),
        "defillama_slug": pl.String(),
        "l2beat_slug": pl.String(),
        "growthepie_origin_key": pl.String(),
        "gas_token": pl.String(),
        "chain_layer": pl.String(),
        "block_time_sec": pl.Float64,
        "da_layer": pl.String(),
        "output_root_layer": pl.String(),
        "system_config_proxy": pl.String(),
        "oplabs_db_schema": pl.String(),
        "oplabs_testnet_db_schema": pl.String(),
        "goldsky_schema": pl.String(),
        "dune_schema": pl.String(),
        "flipside_schema": pl.String(),
        "oso_schema": pl.String(),
        "batchinbox_from": pl.String(),
        "batchinbox_to": pl.String(),
        "outputoracle_from": pl.String(),
        "outputoracle_to_proxy": pl.String(),
        "l1_standard_bridge": pl.String(),
        "optimism_portal": pl.String(),
        "dispute_game_factory": pl.String(),
        "is_op_chain": pl.Boolean(),
        "alignment": pl.String(),
    }


def test_to_pandas():
    testcase = InputTestData.at(__file__)

    # Load the raw and cleaned up CSVs from the test path.
    raw_df = pl.read_csv(testcase.path("case01/chain_metadata_raw.csv"))

    # Clean the CSV.
    actual_clean_df = load._clean(raw_df)

    pddf = upload.to_pandas(actual_clean_df)

    assert pddf["mainnet_chain_id"].to_list() == [
        "7777777",
        "8453",
        "34443",
        "8866",
        "1135",
        "1750",
        "185",
        "2702128",
        "8008",
        "6805",
        "480",
        "360",
        "2192",
        "NA",
        "NA",
        "NA",
        "10",
        "252",
        "690",
        "7560",
        "255",
        "5112",
        "254",
        "288",
        "957",
        "291",
        "60808",
        "42026",
        "NA",
        "NA",
        "NA",
        "624",
        "65536",
        "33979",
        "NA",
        "1088",
        "881",
        "2999",
        "5151706",
        "570",
        "5000",
        "424",
        "169",
        "204",
        "5101",
        "888888888",
        "4653",
        "2410",
        "81457",
        "78225",
        "12553",
        "56026",
        "62050",
        "969",
        "698",
        "48900",
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
        "NA",
        "NA",
        "328527",
        "8853",
        "130",
        "NA",
        "NA",
        "NA",
    ]


def test_goldsky_chains():
    testcase = InputTestData.at(__file__)
    # Load the raw data you want to return from read_gsheet
    raw_gsheet_data = pl.read_csv(testcase.path("case01/chain_metadata_raw.csv")).to_dicts()

    with (
        patch("op_analytics.coreutils.gsheets.get_worksheet") as mock_get_worksheet,
        # Patch load_chain_metadata() so its cache is not changed by this test.
        patch(
            "op_analytics.datapipeline.chains.goldsky_chains.load_chain_metadata",
            load.load_chain_metadata_impl,
        ),
    ):
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_records.return_value = raw_gsheet_data
        mock_get_worksheet.return_value = mock_worksheet

        actual = goldsky_chains.goldsky_mainnet_chains()

        actual.sort()
        expected = [
            "arenaz",
            "automata",
            "base",
            "bob",
            "celo",
            "cyber",
            # "ethereum", (ggarner - 2025/07/14) Exclude ethereum due to downstream dtype issues
            "fraxtal",
            "ham",
            "ink",
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
            "soneium",
            "swan",
            "swell",
            "unichain",
            "worldchain",
            "zora",
        ]
        assert actual == expected
