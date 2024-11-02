# -*- coding: utf-8 -*-
from op_coreutils.logger import structlog
import pandas as pd

log = structlog.get_logger()

FILE_PATH = "../op_chains_tracking/outputs/chain_metadata.csv"


def get_chain_colors(chain_identifier: str = "chain_name") -> dict[str, str]:
    """
    Create a mapping of chain identifiers to colors. Only chains with a color are included.

    Parameters:
    - chain_identifier (str): The column name to use as keys for the color mapping. Defaults to "chain_name".

    Returns:
    - dict[str, str]: A dictionary mapping each chain identifier to its corresponding uppercase hex color.
    """
    df_color = pd.read_csv(FILE_PATH, usecols=[chain_identifier, "hex_color"]).dropna(
        subset=["hex_color"]
    )
    if df_color.empty:
        log.error("No color data found.")
    else:
        df_color["hex_color"] = df_color["hex_color"].apply(
            lambda x: f"#{x.strip().upper()}"
        )

    return dict(zip(df_color[chain_identifier], df_color["hex_color"]))
