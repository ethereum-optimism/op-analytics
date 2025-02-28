from dataclasses import dataclass
from typing import Any

import requests

from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.time import dt_fromepoch

CHAINS_TVL_ENDPOINT = "https://api.llama.fi/v2/historicalChainTvl/{slug}"


@dataclass
class Chain:
    rows: list[dict[str, Any]]

    @classmethod
    def fetch(cls, chain: str, session: requests.Session | None = None) -> "Chain":
        session = session or new_session()

        url = CHAINS_TVL_ENDPOINT.format(slug=chain)
        response = get_data(session, url)
        return cls.of(chain=chain, data=response)

    @classmethod
    def of(cls, chain: str, data: list[dict[str, Any]]) -> "Chain":
        return cls(
            rows=[
                {
                    "chain_name": chain,
                    "dt": dt_fromepoch(entry["date"]),
                    "tvl": entry["tvl"],
                }
                for entry in data
            ]
        )
