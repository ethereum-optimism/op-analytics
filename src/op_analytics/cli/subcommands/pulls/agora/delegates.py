from dataclasses import dataclass
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import get_data
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import now_dt
from op_analytics.coreutils.env.vault import env_get
from typing import Any
import requests
import requests.exceptions
import itertools
import time
import pandas as pd
import polars as pl
from op_analytics.cli.subcommands.pulls.agora.dataacess import (
    Agora,
    write,
)

log = structlog.get_logger()

BASE_URL = "https://vote.optimism.io/api/v1"
DELEGATES_ENDPOINT = f"{BASE_URL}/delegates"
API_KEY = env_get("AGORA_API_TOKEN")


@dataclass
class AgoraDelegates:
    """Agora delegates data."""

    delegates_df: pl.DataFrame


@dataclass
class PaginatedResponse:
    has_next: bool
    next_offset: int
    data: Any


class Paginator:
    def __init__(self, url: str, params: dict = None, max_workers: int = 16):
        self.url = url
        self.params = params or {}
        self.max_workers = max_workers
        self.limit = 100
        self.failed_offsets = []

    def request(self, offset):
        session = requests.Session()  # Create a session object
        if self.params is None:
            self.params = {}
        self.params.update({"offset": offset, "limit": self.limit})
        try:
            result = get_data(
                session,
                url=self.url,
                headers={"Authorization": f"Bearer {API_KEY}"},
                params=self.params,
                retry_attempts=5,
            )

            assert "meta" in result
            assert "data" in result

            return PaginatedResponse(
                has_next=result["meta"]["has_next"],
                next_offset=result["meta"]["next_offset"],
                data=result["data"],
            )
        except Exception as e:
            log.error(f"Failed to fetch data for offset: {offset}, URL: {self.url}, Error: {e}")
            self.failed_offsets.append(offset)
            time.sleep(10)
            return self.request(offset)

    def binary_search_max_offset(self):
        lo, hi = 0, self.limit
        lower_bound, upper_bound = set(), set()
        num_iterations, max_iterations = 0, 40

        while True:
            num_iterations += 1
            if num_iterations > max_iterations:
                log.warning(
                    "Exceeded maximum iterations in binary search. Resorting to best estimate."
                )
                if upper_bound:
                    self.max_offset = min(upper_bound)
                elif lower_bound:
                    self.max_offset = max(lower_bound)
                else:
                    break
                return self.max_offset

            resp_lo = self.request(lo)
            resp_hi = self.request(hi)

            if resp_lo is None or resp_hi is None:
                log.warning(f"Failed to fetch data for offsets {lo} or {hi}.")
                break

            print(f"max offset search range {lo} -- {hi}")

            diff = hi - lo
            midpoint = lo + (diff) // 2

            if resp_lo.has_next and resp_hi.has_next:
                lower_bound.add(hi)
                lo = max(lower_bound)
                hi = min(upper_bound) if upper_bound else hi * 10
            elif resp_lo.has_next and not resp_hi.has_next:
                lower_bound.add(lo)
                upper_bound.add(hi)
                if diff < 1000:
                    self.min_offset = lo
                    self.max_offset = hi
                    return hi
                else:
                    hi = midpoint
            else:
                log.warning("Unexpected state encountered in binary search.")
                hi = midpoint

    def concurrent_fetch(self):
        self.max_offset = self.binary_search_max_offset()
        offsets = list(range(0, self.max_offset, self.limit))[:10]
        data = run_concurrently(
            function=lambda offset: self.request(offset).data,
            targets=offsets,
            max_workers=self.max_workers,
        )
        return list(itertools.chain.from_iterable(data.values()))


def pull_delegates():
    """Pull and write agora delegates data."""
    p = Paginator(url=f"{BASE_URL}/delegates", params={"sort": "voting_power"}, max_workers=32)
    delegate_data = p.concurrent_fetch()

    # Clean and transform to Pandas
    normalized_data = []
    for item in delegate_data:
        # Extract fields from the item dictionary
        address = item.get("address", "")
        voting_power = item.get("votingPower", {})
        citizen = item.get("citizen", False)
        statement = item.get("statement", {})

        # Make sure voting_power is a dictionary
        if not isinstance(voting_power, dict):
            continue

        flattened = {
            "address": address,
            "voting_power_total": voting_power.get("total", "0"),
            "voting_power_direct": voting_power.get("direct", "0"),
            "voting_power_advanced": voting_power.get("advanced", "0"),
            "is_citizen": citizen,
        }

        if isinstance(statement, dict):
            flattened.update(
                {
                    "statement_signature": statement.get("signature", ""),
                    "statement_created_at": statement.get("created_at", ""),
                    "statement_updated_at": statement.get("updated_at", ""),
                    "statement_text": statement.get("payload", {}).get("delegateStatement", ""),
                    "twitter": statement.get("payload", {}).get("twitter", ""),
                    "discord": statement.get("payload", {}).get("discord", ""),
                }
            )

        normalized_data.append(flattened)
    df = pd.DataFrame(normalized_data)
    df = df.astype(
        {
            "address": "string",
            "voting_power_total": "Float64",
            "voting_power_direct": "Float64",
            "voting_power_advanced": "Float64",
            "is_citizen": "bool",
            "statement_signature": "string",
            "statement_created_at": "string",
            "statement_updated_at": "string",
            "statement_text": "string",
            "twitter": "string",
            "discord": "string",
        },
        errors="coerce",
    )
    # Check for duplicates
    if df.shape[0] != len(df["address"].drop_duplicates()):
        df = df.drop_duplicates()
        log.warning(
            f"Found duplicates in delegates data. Dropped {df.shape[0] - len(df['address'].drop_duplicates())} rows."
        )

    # Todo: This is to partition by pull date. Figure out if this is the correct way to do this.
    df["dt"] = now_dt()

    # Convert to polars
    df = pl.from_pandas(df)

    # Write to GCS
    write(dataset=Agora.DELEGATES, dataframe=df, sort_by=["voting_power_total"])

    return AgoraDelegates(delegates_df=df)


# Todo: move to app.py
def pull_delegate_data():
    """Pull and write agora data."""
    from op_analytics.cli.subcommands.pulls.agora.delegate_events import (
        fetch_delegate_votes,
        fetch_proposals,
    )

    delegates = pull_delegates()

    delegates_with_voting_power = delegates.delegates_df.filter(pl.col("voting_power_total") > 0)
    delegates_without_voting_power = delegates.delegates_df.filter(
        pl.col("voting_power_total") == 0
    )

    log.info(f"Found {len(delegates_with_voting_power)} delegates with voting power.")
    delegate_addresses = delegates_with_voting_power.address.to_list()

    delegate_votes = fetch_delegate_votes(delegate_addresses)
    log.info(f"Found {len(delegate_votes)} delegate votes.")

    proposals = fetch_proposals()
    log.info(f"Found {len(proposals)} proposals.")
