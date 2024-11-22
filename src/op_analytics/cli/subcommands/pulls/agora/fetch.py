from typing import List, Dict
from threading import Lock
import os
import json
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import get_data


log = structlog.get_logger()

BASE_URL = "https://vote.optimism.io/api/v1"
ENDPOINTS = {
    "delegates": f"{BASE_URL}/delegates",
    "proposals": f"{BASE_URL}/proposals",
    "proposal_by_id": f"{BASE_URL}/proposals/{{proposal_id}}",
    "votes_by_proposal": f"{BASE_URL}/proposals/{{proposal_id}}/votes",
    "delegatees": f"{BASE_URL}/delegates/{{addressOrEnsName}}/delegatees",
}

AUTH_TOKEN = os.getenv("AGORA_API_KEY")

if not AUTH_TOKEN:
    raise EnvironmentError("AGORA_API_KEY environment variable is not set.")

HEADERS = {
    "Authorization": f"Bearer {AUTH_TOKEN}",
    "Content-Type": "application/json",
}


# Unused, exists for future use regarding pagination
class CheckpointManager:
    def __init__(self, checkpoint_file="api_checkpoints.json"):
        self.checkpoint_file = checkpoint_file
        self._lock = Lock()
        self._checkpoints = self._load_checkpoints()

    def _load_checkpoints(self) -> Dict[str, int]:
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, "r") as f:
                checkpoints = json.load(f)
                log.info(f"Loaded checkpoints: {checkpoints}")
                return checkpoints
        else:
            return {}

    def get_checkpoint(self, key: str) -> int:
        return self._checkpoints.get(key, 0)

    def update_checkpoint(self, key: str, value: int):
        with self._lock:
            self._checkpoints[key] = value
            self._save_checkpoints()

    def _save_checkpoints(self):
        with open(self.checkpoint_file, "w") as f:
            json.dump(self._checkpoints, f)
            log.info(f"Saved checkpoints: {self._checkpoints}")


log = structlog.get_logger()
checkpoint_manager = CheckpointManager()


# Todo: Transport to coreutils
def fetch_paginated_data_with_offset(
    session, url: str, offset: int = 0, params: dict = {}
) -> List[dict]:
    data = []
    limit = params.get("limit", 100)
    offset = offset

    while True:
        query_params = {"limit": limit, "offset": offset}
        if params:
            query_params.update(params)

        response = get_data(session, url, headers=HEADERS, params=query_params)

        # Check if the response is a list or a dictionary
        if isinstance(response, list):
            batch = response
        else:
            batch = response.get("data", [])

        data.extend(batch)

        # If the response is a list, we assume there's no pagination metadata
        if isinstance(response, list) or not response.get("metadata", {}).get("has_next", False):
            break

        # Update offset for the next batch
        offset = response.get("metadata", {}).get("next_offset", offset + limit)

    return data


def fetch_delegates(session, offset: int = 0) -> List[dict]:
    url = ENDPOINTS["delegates"]
    data = fetch_paginated_data_with_offset(session, url, offset)
    log.info(f"Fetched {len(data)} delegates starting from offset {offset}.")
    return data


def fetch_delegations(session, address_or_ens_name: str, offset: int = 0) -> List[dict]:
    url = ENDPOINTS["delegatees"].format(addressOrEnsName=address_or_ens_name)
    data = fetch_paginated_data_with_offset(session, url, offset)
    log.info(f"Fetched {len(data)} delegations starting from offset {offset}.")
    return data


def fetch_proposals(
    session, limit: int = 10, offset: int = 0, filter: str = "relevant"
) -> List[dict]:
    url = ENDPOINTS["proposals"]
    params = {"limit": limit, "offset": offset, "filter": filter}
    data = fetch_paginated_data_with_offset(session, url, offset=offset, params=params)
    log.info(f"Fetched {len(data)} proposals starting from offset {offset} with filter '{filter}'.")
    return data


def fetch_proposal_by_id(session, proposal_id: str) -> dict:
    """
    Fetch a specific proposal by its ID.
    """
    url = ENDPOINTS["proposal_by_id"].format(proposal_id=proposal_id)
    response = get_data(session, url, headers=HEADERS)

    if response:
        log.info(f"Fetched proposal with ID {proposal_id}.")
        return response
    else:
        log.error(f"Failed to fetch proposal with ID {proposal_id}.")
        return {}


def fetch_votes_by_proposal(
    session, proposal_id: str, limit: int = 10, offset: int = 0, sort: str = None
) -> List[dict]:
    """
    Fetch a paginated list of votes for a specific proposal.
    """
    url = ENDPOINTS["votes_by_proposal"].format(proposal_id=proposal_id)
    params = {"limit": limit, "offset": offset}
    if sort:
        params["sort"] = sort

    data = fetch_paginated_data_with_offset(session, url, offset=offset, params=params)
    log.info(f"Fetched {len(data)} votes for proposal {proposal_id} starting from offset {offset}.")
    return data
