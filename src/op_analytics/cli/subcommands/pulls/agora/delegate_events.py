from dataclasses import dataclass
from typing import Any, List
import requests
import polars as pl

from op_analytics.cli.subcommands.pulls.agora.dataaccess import Agora
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import get_data
from op_analytics.coreutils.threads import run_concurrently_store_failures
from op_analytics.cli.subcommands.pulls.agora.delegates import AgoraDelegates
from op_analytics.coreutils.env.vault import env_get

log = structlog.get_logger()

BASE_URL = "https://vote.optimism.io/api/v1"


@dataclass
class AgoraDelegateVotes:
    votes_df: pl.DataFrame


@dataclass
class PaginatedResponse:
    has_next: bool
    next_offset: int
    data: Any


@dataclass
class SimplePaginator:
    url: str
    limit: int = 50

    def __post_init__(self):
        self._api_key = env_get("AGORA_API_TOKEN")

    def request(self, offset: int) -> PaginatedResponse:
        session = requests.Session()
        result = get_data(
            session,
            url=self.url,
            headers={"Authorization": f"Bearer {self._api_key}"},
            params={"offset": offset, "limit": self.limit},
        )

        assert "meta" in result
        assert "data" in result

        return PaginatedResponse(
            has_next=result["meta"]["has_next"],
            next_offset=result["meta"]["next_offset"],
            data=result["data"],
        )

    def fetch_all(self) -> List[Any]:
        offset = 0
        all_data = []
        while True:
            response = self.request(offset)
            all_data.extend(response.data)
            if not response.has_next:
                break
            offset = response.next_offset
        return all_data


def parse_isoformat_with_z(iso_string):
    from datetime import datetime

    # Replace 'Z' with '+00:00' to make it compatible with fromisoformat
    if iso_string.endswith("Z"):
        iso_string = iso_string[:-1] + "+00:00"
    return datetime.fromisoformat(iso_string)


def fetch_event_data(delegates: List[str], endpoint: str, workers: int) -> List[Any]:
    def fetch_delegate_data(address: str) -> List[Any]:
        paginator = SimplePaginator(url=f"{BASE_URL}/delegates/{address}/{endpoint}", limit=50)
        return paginator.fetch_all()

    run_results = run_concurrently_store_failures(
        function=fetch_delegate_data,
        targets=delegates,
        max_workers=workers,
    )

    if run_results.failures:
        log.error(f"Failed to fetch data for {len(run_results.failures)} addresses")

    return list(run_results.results.values())


def _flatten_vote_data(data: List[Any]) -> List[dict]:
    return [
        {
            "transaction_hash": value["transactionHash"],
            "address": value["address"],
            "proposal_id": value["proposalId"],
            "support": value["support"],
            "weight": value["weight"],
            "reason": value["reason"],
            "params": value["params"],
            "proposal_value": value["proposalValue"],
            "proposal_title": value["proposalTitle"],
            "proposal_type": value["proposalType"],
            "dt": parse_isoformat_with_z(value["timestamp"]),
        }
        for record in data
        for value in record
    ]


def fetch_delegate_votes(delegates: AgoraDelegates, workers: int = 12) -> AgoraDelegateVotes:
    """Considers only delegates with voting power."""
    delegates_with_voting_power = delegates.delegates_df.filter(pl.col("voting_power_total") > 0)
    log.info(f"Found {len(delegates_with_voting_power)} delegates with voting power.")
    delegate_addresses = delegates_with_voting_power["address"].to_list()

    votes_data = fetch_event_data(delegate_addresses, endpoint="votes", workers=workers)
    flattened = _flatten_vote_data(votes_data)
    df = pl.DataFrame(flattened).sort("dt")
    Agora.DELEGATE_VOTES.write(dataframe=df, sort_by=["dt"])
    return AgoraDelegateVotes(votes_df=df)


def fetch_proposals() -> pl.DataFrame:
    paginator = SimplePaginator(url=f"{BASE_URL}/proposals", limit=50)
    proposals = paginator.fetch_all()

    def parse_optional_time(time_str: str | None) -> Any:
        return parse_isoformat_with_z(time_str) if time_str else None

    def extract_proposal_data(proposal: dict) -> dict:
        # Extract proposal results with default values
        results = proposal.get("proposalResults", {})

        return {
            "id": proposal["id"],
            "dt": parse_isoformat_with_z(proposal["createdTime"]),
            "proposer": proposal["proposer"],
            "snapshot_block_number": proposal["snapshotBlockNumber"],
            "start_time": parse_isoformat_with_z(proposal["startTime"]),
            "end_time": parse_optional_time(proposal.get("endTime")),
            "cancelled_time": parse_optional_time(proposal.get("cancelledTime")),
            "executed_time": parse_optional_time(proposal.get("executedTime")),
            "queued_time": parse_optional_time(proposal.get("queuedTime")),
            "markdowntitle": proposal["markdowntitle"],
            "description": proposal["description"],
            "quorum": float(proposal["quorum"]) if proposal.get("quorum") else 0.0,
            "proposal_type": proposal["proposalType"],
            "status": proposal["status"],
            "created_transaction_hash": proposal["createdTransactionHash"],
            "cancelled_transaction_hash": proposal.get("cancelledTransactionHash"),
            "executed_transaction_hash": proposal.get("executedTransactionHash"),
            "proposal_results_for": float(results.get("for", 0)),
            "proposal_results_against": float(results.get("against", 0)),
            "proposal_results_abstain": float(results.get("abstain", 0)),
            "proposal_data": proposal.get("proposalData"),
            "unformatted_proposal_data": proposal.get("unformattedProposalData"),
        }

    schema = {
        "id": pl.Utf8,
        "dt": pl.Datetime,
        "proposer": pl.Utf8,
        "snapshot_block_number": pl.UInt64,
        "start_time": pl.Datetime,
        "end_time": pl.Datetime,
        "cancelled_time": pl.Datetime,
        "executed_time": pl.Datetime,
        "queued_time": pl.Datetime,
        "markdowntitle": pl.Utf8,
        "description": pl.Utf8,
        "quorum": pl.Float64,
        "proposal_type": pl.Utf8,
        "status": pl.Utf8,
        "created_transaction_hash": pl.Utf8,
        "cancelled_transaction_hash": pl.Utf8,
        "executed_transaction_hash": pl.Utf8,
        "proposal_results_for": pl.Float64,
        "proposal_results_against": pl.Float64,
        "proposal_results_abstain": pl.Float64,
        "proposal_data": pl.Struct,
        "unformatted_proposal_data": pl.Struct,
    }

    cleaned_rows = [extract_proposal_data(p) for p in proposals]
    df = pl.DataFrame(cleaned_rows, schema=schema).sort("start_time")
    Agora.PROPOSALS.write(dataframe=df, sort_by=["start_time"])
    return df


def _fetch_single_address_data(address: str, endpoint: str) -> List[dict]:
    session = requests.Session()
    url = f"{BASE_URL}/delegates/{address}/{endpoint}"
    api_key = env_get("AGORA_API_TOKEN")
    result = get_data(
        session,
        url=url,
        headers={"Authorization": f"Bearer {api_key}"},
        params={},
    )
    return result.get("votes", []) if endpoint == "delegators" else [result]


def fetch_delegate_data(delegates: List[str], endpoint: str, workers: int) -> pl.DataFrame:
    def fetch_data(address: str) -> List[dict]:
        return _fetch_single_address_data(address, endpoint=endpoint)

    run_results = run_concurrently_store_failures(
        function=fetch_data,
        targets=delegates,
        max_workers=workers,
    )

    all_data = [data for v in run_results.results.values() for record in v for data in record]
    schema = {
        "dt": pl.Datetime,
        "from_address": pl.Utf8,
        "to_address": pl.Utf8,
        "allowance": pl.Float64,
        "type": pl.Utf8,
        "amount": pl.Utf8,
        "transaction_hash": pl.Utf8,
    }

    rows = [
        {
            "dt": parse_isoformat_with_z(record["timestamp"]),
            "from_address": record["from"],
            "to_address": record["to"],
            "allowance": record["allowance"],
            "type": record["type"],
            "amount": record["amount"],
            "transaction_hash": record["transaction_hash"],
        }
        for record in all_data
    ]

    return pl.DataFrame(rows, schema=schema)


def fetch_delegate_delegators(delegates: AgoraDelegates, workers: int = 12) -> pl.DataFrame:
    delegate_addresses = delegates.delegates_df["address"].to_list()
    delegator_data = fetch_delegate_data(delegate_addresses, endpoint="delegators", workers=workers)
    print(delegator_data.to_pandas().head())
    Agora.DELEGATORS.write(dataframe=delegator_data, sort_by=["dt"])
    return delegator_data


def fetch_delegate_delegatees(delegates: AgoraDelegates, workers: int = 12) -> pl.DataFrame:
    delegate_addresses = delegates.delegates_df["address"].to_list()
    delegatee_data = fetch_delegate_data(delegate_addresses, endpoint="delegatees", workers=workers)
    print(delegatee_data.to_pandas().head())
    Agora.DELEGATEES.write(dataframe=delegatee_data, sort_by=["dt"])
    return delegatee_data
