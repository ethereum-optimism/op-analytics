from dataclasses import dataclass

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import get_data
from op_analytics.coreutils.threads import run_concurrently
import itertools
from typing import Any, List
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests.exceptions
import time
import pandas as pd
from op_analytics.cli.subcommands.pulls.agora.data_access import (
    Agora,
    write,
    _camelcase_to_snakecase,
)
import os

log = structlog.get_logger()

BASE_URL = "https://vote.optimism.io/api/v1"
DELEGATES_ENDPOINT = f"{BASE_URL}/delegates"
API_KEY = os.environ["AGORA_API_KEY"]
session = requests.Session()  # Create a session object


@dataclass
class AgoraDelegates:
    """Agora delegates data."""

    delegates_with_voting_power_df: pl.DataFrame
    delegates_without_voting_power_df: pl.DataFrame


@dataclass
class PaginatedResponse:
    has_next: bool
    next_offset: int
    data: Any


class Paginator:
    url: str
    limit: int = 100
    params: dict = None
    failed_offsets: List[int] = None
    max_workers: int = 16

    def __post_init__(self):
        self.failed_offsets = []

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=20),
        retry=retry_if_exception_type(requests.exceptions.HTTPError),
    )
    def request(self, offset):
        if self.params is None:
            self.params = {}
        self.params.update({"offset": offset, "limit": self.limit})
        try:
            result = get_data(
                session,
                url=self.url,
                headers={"Authorization": f"Bearer {API_KEY}"},
                params=self.params,
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
        offsets = list(range(0, self.max_offset, self.limit))
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
    df = pd.json_normalize(delegate_data, sep="_")
    df.columns = df.columns.map(_camelcase_to_snakecase)
    df = df.convert_dtypes()
    # In order to be able to drop duplicates we cannot have iterables in the dataframe
    df = df.applymap(lambda x: str(x) if isinstance(x, list) else x)
    # Check for duplicates
    if df.shape[0] != len(df["address"].drop_duplicates()):
        df = df.drop_duplicates()
        log.warning(
            f"Found duplicates in delegates data. Dropped {df.shape[0] - len(df['address'].drop_duplicates())} rows."
        )

    # Split to delegates that have voting power and those that don't
    delegates_with_voting_power = df[df["voting_power_total"] > 0]
    delegates_without_voting_power = df[df["voting_power_total"] == 0]

    # Write to GCS
    write(dataset=Agora.DELEGATES, dataframe=df, sort_by=["voting_power_total"])
    # Why do this? Because the sizes are assymetrical and we want to optimize downstream
    # performance.
    write(
        dataset=Agora.DELEGATES_WITH_VOTING_POWER,
        dataframe=delegates_with_voting_power,
        sort_by=["voting_power_total"],
    )
    write(
        dataset=Agora.DELEGATES_WITHOUT_VOTING_POWER,
        dataframe=delegates_without_voting_power,
    )

    return AgoraDelegates(
        delegates_with_voting_power_df=delegates_with_voting_power,
        delegates_without_voting_power_df=delegates_without_voting_power,
    )


# Todo: move to app.py
def pull_delegate_data():
    """Pull and write agora data."""
    from op_analytics.cli.subcommands.pulls.agora.delegate_events import (
        fetch_delegate_votes,
        fetch_proposals,
    )

    delegates_with_voting_power, _ = pull_delegates()

    log.info(f"Found {len(delegates_with_voting_power)} delegates with voting power.")
    delegate_addresses = delegates_with_voting_power.address.to_list()

    delegate_votes = fetch_delegate_votes(delegate_addresses)
    log.info(f"Found {len(delegate_votes)} delegate votes.")

    proposals = fetch_proposals()
    log.info(f"Found {len(proposals)} proposals.")
