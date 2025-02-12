from dataclasses import dataclass
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import get_data
from op_analytics.coreutils.threads import run_concurrently_store_failures
from op_analytics.coreutils.time import now_dt
from op_analytics.coreutils.env.vault import env_get
from typing import Any
import requests
import requests.exceptions
import itertools
import time
import polars as pl

from .dataaccess import Agora

log = structlog.get_logger()

BASE_URL = "https://vote.optimism.io/api/v1"
DELEGATES_ENDPOINT = f"{BASE_URL}/delegates"


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
    def __init__(self, url: str, params: dict = {}, max_workers: int = 16):
        self.url = url
        self.params = params or {}
        self.max_workers = max_workers
        self.limit = 100
        self.failed_offsets: list[int] = []
        self._api_key = env_get("AGORA_API_TOKEN")

    def request(self, offset):
        session = requests.Session()
        if self.params is None:
            self.params = {}
        self.params.update({"offset": offset, "limit": self.limit})
        try:
            result = get_data(
                session,
                url=self.url,
                headers={"Authorization": f"Bearer {self._api_key}"},
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
        lower_bound: set[int] = set()
        upper_bound: set[int] = set()
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
            midpoint = lo + (diff // 2)

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
        results = run_concurrently_store_failures(
            function=lambda offset: self.request(offset).data,
            targets=offsets,
            max_workers=self.max_workers,
        )
        data = results.results.values()
        if results.failures:
            log.warning(f"Failed to fetch data for offsets: {results.failures}")
        return list(itertools.chain.from_iterable(data))


def pull_delegates():
    """Pull and write agora delegates data."""
    p = Paginator(url=f"{BASE_URL}/delegates", params={"sort": "voting_power"}, max_workers=32)
    delegate_data = p.concurrent_fetch()

    normalized_data = []
    for item in delegate_data:
        address = item.get("address", "")
        voting_power = item.get("votingPower") or {}
        citizen = item.get("citizen", False)
        statement = item.get("statement") or {}
        payload = statement.get("payload") or {}

        # Raise an exception if 'voting_power' is not a dict
        if not isinstance(voting_power, dict):
            raise ValueError(
                f"Expected 'votingPower' to be a dict for address {address}, got {type(voting_power)}"
            )

        flattened = {
            "address": address,
            "voting_power_total": voting_power["total"],
            "voting_power_direct": voting_power["direct"],
            "voting_power_advanced": voting_power["advanced"],
            "is_citizen": citizen,
            "statement_signature": statement.get("signature", ""),
            "statement_created_at": statement.get("created_at", ""),
            "statement_updated_at": statement.get("updated_at", ""),
            "statement_text": payload.get("delegateStatement", ""),
            "twitter": payload.get("twitter", ""),
            "discord": payload.get("discord", ""),
        }

        normalized_data.append(flattened)

    schema = {
        "address": pl.Utf8,
        "voting_power_total": pl.Float64,
        "voting_power_direct": pl.Float64,
        "voting_power_advanced": pl.Float64,
        "is_citizen": pl.Boolean,
        "statement_signature": pl.Utf8,
        "statement_created_at": pl.Utf8,
        "statement_updated_at": pl.Utf8,
        "statement_text": pl.Utf8,
        "twitter": pl.Utf8,
        "discord": pl.Utf8,
    }
    df = pl.DataFrame(normalized_data, schema=schema)

    # Deduplicate by address if needed
    initial_count = df.height
    df = df.unique(subset=["address"])
    final_count = df.height
    if final_count < initial_count:
        log.warning(f"Dropped {initial_count - final_count} duplicate rows based on 'address'.")

    # Add a dt column for partitioning
    df = df.with_columns(pl.lit(now_dt()).alias("dt"))

    # Write to Storage using Agora's write
    Agora.DELEGATES.write(dataframe=df, sort_by=["voting_power_total"])

    return AgoraDelegates(delegates_df=df)
