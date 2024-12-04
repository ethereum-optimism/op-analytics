from dataclasses import dataclass
from op_analytics.cli.subcommands.pulls.agora.data_access import (
    Agora,
    write,
    _camelcase_to_snakecase,
    parse_isoformat_with_z,
)
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import get_data
from op_analytics.coreutils.threads import run_concurrently_store_failures

from typing import Any
import requests
import pandas as pd
import os

log = structlog.get_logger()

BASE_URL = "https://vote.optimism.io/api/v1"
DELEGATES_ENDPOINT = f"{BASE_URL}/delegates"
API_KEY = os.environ["AGORA_API_KEY"]


@dataclass
class AgoraDelegateVotes:
    votes_df: pd.DataFrame


@dataclass
class PaginatedResponse:
    has_next: bool
    next_offset: int
    data: Any


@dataclass
class SimplePaginator:
    url: str
    limit: int = 50

    def request(self, offset):
        session = requests.Session()
        result = get_data(
            session,
            url=self.url,
            headers={"Authorization": f"Bearer {API_KEY}"},
            params={"offset": offset, "limit": self.limit},
        )

        assert "meta" in result
        assert "data" in result

        return PaginatedResponse(
            has_next=result["meta"]["has_next"],
            next_offset=result["meta"]["next_offset"],
            data=result["data"],
        )

    def fetch_all(self):
        offset = 0
        all_data = []

        while True:
            response = self.request(offset)
            all_data.extend(response.data)

            if not response.has_next:
                break

            offset = response.next_offset

        return all_data


def fetch_event_data(delegates: list, endpoint: str, workers: int) -> pd.DataFrame:
    def fetch_delegate_data(address: str, endpoint: str) -> list:
        paginator = SimplePaginator(url=f"{BASE_URL}/delegates/{address}/{endpoint}", limit=50)
        try:
            return paginator.fetch_all()
        except requests.exceptions.HTTPError as e:
            log.error(
                f"HTTP Error for address: {address}, URL: {e.response.url}, Status Code: {e.response.status_code}"
            )
            return []

    run_results = run_concurrently_store_failures(
        function=fetch_delegate_data,
        targets=delegates,
        max_workers=workers,
        function_args=(endpoint,),
    )

    all_delegate_data = run_results.results.values()
    failed_addresses = run_results.failures.keys()

    if len(failed_addresses) > 0:
        log.error(f"Failed to fetch data for {len(failed_addresses)} addresses")

    return all_delegate_data


def _flatten_data(data):
    flattened_data = [
        {**value, "address": address}
        for record in data
        for address, values in record.items()
        for value in values
    ]
    return flattened_data


def fetch_delegate_votes(delegates: list, workers: int = 12):
    votes = fetch_event_data(delegates, endpoint="votes", workers=workers)
    votes = pd.DataFrame(_flatten_data(votes))
    votes.rename(columns={"timestamp": "dt"}, inplace=True)
    votes.set_index("dt", inplace=True)
    votes.index = votes.index.map(parse_isoformat_with_z)
    votes.columns = votes.columns.map(_camelcase_to_snakecase)

    write(dataset=Agora.DELEGATE_VOTES, dataframe=votes, sort_by=["dt"])

    return AgoraDelegateVotes(votes_df=votes)


def fetch_delegate_delegators(delegates: list, workers: int = 12):
    """Placeholder until the endpoint is available"""
    pass


def fetch_delegate_delegatees(delegates: list, workers: int = 12):
    """Placeholder until the endpoint is available"""
    pass


def fetch_proposals():
    paginator = SimplePaginator(url=f"{BASE_URL}/proposals", limit=50)
    proposals = paginator.fetch_all()
    df = pd.DataFrame(proposals)
    df.columns = df.columns.map(_camelcase_to_snakecase)
    df.set_index("start_time", inplace=True)
    df.index = df.index.map(parse_isoformat_with_z)

    write(dataset=Agora.PROPOSALS, dataframe=df, sort_by=["start_time"])
