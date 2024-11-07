import polars as pl
from op_coreutils.logger import structlog

from .load import load_chain_metadata

log = structlog.get_logger()


def get_chain_colors(chain_identifier: str = "chain_name") -> dict[str, str]:
    """
    Create a mapping of chain identifiers to colors. Only chains with a color are included.

    Parameters:
    - chain_identifier (str): The column name to use as keys for the color mapping. Defaults to "chain_name".

    Returns:
    - dict[str, str]: A dictionary mapping each chain identifier to its corresponding uppercase hex color.
    """
    df_color = (
        load_chain_metadata()
        .select(chain_identifier, "hex_color")
        .filter(pl.col("hex_color").is_not_null())
    )

    if df_color.is_empty():
        log.error("No color data found.")
    else:
        colors = {
            chain[chain_identifier]: "#" + chain["hex_color"].strip().upper()
            for chain in df_color.to_dicts()
        }

    return colors
