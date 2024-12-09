from dataclasses import dataclass
from typing import Any
import os
import requests
import pandas as pd
import polars as pl

from op_analytics.cli.subcommands.pulls.agora.dataaccess import (
    Agora,
    write,
    _camelcase_to_snakecase,
    parse_isoformat_with_z,
)
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import get_data
from op_analytics.coreutils.threads import run_concurrently_store_failures

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
        # Let exceptions bubble up so run_concurrently_store_failures can detect them
        paginator = SimplePaginator(url=f"{BASE_URL}/delegates/{address}/{endpoint}", limit=50)
        return paginator.fetch_all()

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


def _flatten_vote_data(data):
    rows = []
    for record in data:
        for value in record:
            rows.append(
                {
                    "transactionHash": value["transactionHash"],
                    "address": value["address"],
                    "proposalId": value["proposalId"],
                    "support": value["support"],
                    "weight": value["weight"],
                    "reason": value["reason"],
                    "params": value["params"],
                    "proposalValue": value["proposalValue"],
                    "proposalTitle": value["proposalTitle"],
                    "proposalType": value["proposalType"],
                    "timestamp": value["timestamp"],
                }
            )
    return rows


def fetch_delegate_votes(delegates: list, workers: int = 12):
    votes = fetch_event_data(delegates, endpoint="votes", workers=workers)
    votes = pd.DataFrame(_flatten_vote_data(votes))
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

    def parse_optional_time(time_str):
        return parse_isoformat_with_z(time_str) if time_str else None

    def extract_proposal_data(proposal):
        return {
            "id": proposal["id"],
            "proposer": proposal["proposer"],
            "snapshot_block_number": proposal["snapshotBlockNumber"],
            "created_time": parse_isoformat_with_z(proposal["createdTime"]),
            "start_time": parse_isoformat_with_z(proposal["startTime"]),
            "end_time": parse_optional_time(proposal.get("endTime")),
            "cancelled_time": parse_optional_time(proposal.get("cancelledTime")),
            "executed_time": parse_optional_time(proposal.get("executedTime")),
            "queued_time": parse_optional_time(proposal.get("queuedTime")),
            "markdowntitle": proposal["markdowntitle"],
            "description": proposal["description"],
            "quorum": proposal["quorum"],
            "approval_threshold": proposal["approvalThreshold"],
            "proposal_type": proposal["proposalType"],
            "status": proposal["status"],
            "created_transaction_hash": proposal["createdTransactionHash"],
            "cancelled_transaction_hash": proposal["cancelledTransactionHash"],
            "executed_transaction_hash": proposal["executedTransactionHash"],
            "proposal_results_for": proposal["proposalResults"]["for"],
            "proposal_results_against": proposal["proposalResults"]["against"],
            "proposal_results_abstain": proposal["proposalResults"]["abstain"],
        }

    cleaned_rows = [extract_proposal_data(p) for p in proposals]

    df = pl.DataFrame(cleaned_rows)
    write(dataset=Agora.PROPOSALS, dataframe=df, sort_by=["start_time"])
