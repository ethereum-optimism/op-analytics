from dataclasses import dataclass
from datetime import date
from typing import Any


import polars as pl
import requests

from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.time import date_tostr


PROTOCOLS_ENDPOINT = "https://api.llama.fi/protocols"


@dataclass
class ProtocolMetadata:
    df: pl.DataFrame

    def slugs(self) -> list[str]:
        return self.df.get_column("protocol_slug").to_list()

    @classmethod
    def fetch(cls, process_dt: date, session: requests.Session | None = None) -> "ProtocolMetadata":
        """Extract metadata from the protocols API response.

        Args:
            protocols: List of protocol dictionaries from the API response.

        Returns:
            Polars DataFrame containing metadata.
        """
        session = session or new_session()
        protocols = get_data(session, PROTOCOLS_ENDPOINT)

        metadata_records = [
            {
                "protocol_name": protocol.get("name"),
                "protocol_slug": protocol.get("slug"),
                "protocol_category": protocol.get("category"),
                "parent_protocol": extract_parent(protocol),
                "wrong_liquidity": protocol.get("wrongLiquidity"),
                "misrepresented_tokens": protocol.get("misrepresentedTokens"),
            }
            for protocol in protocols
            if protocol.get("category")
        ]
        return cls(pl.DataFrame(metadata_records).with_columns(dt=pl.lit(date_tostr(process_dt))))


def extract_parent(protocol: dict[str, Any]) -> str | None:
    if parent_protcol := protocol.get("parentProtocol"):
        assert isinstance(parent_protcol, str)
        return parent_protcol.replace("parent#", "")
    else:
        return protocol.get("slug")
