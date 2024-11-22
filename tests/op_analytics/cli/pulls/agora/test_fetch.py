from unittest.mock import patch, MagicMock
import requests
from op_analytics.cli.subcommands.pulls.agora.fetch import (
    fetch_proposals,
)


# Test for pagination
@patch("op_analytics.cli.subcommands.pulls.agora.fetch.get_data")
def test_pagination(mock_get_data):
    # Mock response for paginated proposals
    mock_get_data.side_effect = [
        [{"id": "proposal_1"}],
        [{"id": "proposal_2"}],
        [],  # No more proposals
    ]

    session = requests.Session()
    all_proposals = []
    offset = 0
    limit = 1  # Simulate fetching one proposal at a time

    while True:
        proposals = fetch_proposals(session, limit=limit, offset=offset)
        if not proposals:
            break
        all_proposals.extend(proposals)
        offset += limit

    # Assertions
    assert len(all_proposals) == 2
    assert all_proposals[0]["id"] == "proposal_1"
    assert all_proposals[1]["id"] == "proposal_2"


# Test for checkpointing
@patch("op_analytics.cli.subcommands.pulls.agora.fetch.get_data")
@patch("op_analytics.cli.subcommands.pulls.agora.fetch.checkpoint_manager")
def test_checkpointing(mock_checkpoint_manager, mock_get_data):
    # Mock response for proposals
    mock_get_data.return_value = [{"id": "proposal_1"}]

    # Mock checkpoint manager behavior
    mock_checkpoint_manager.get_checkpoint.return_value = 0
    mock_checkpoint_manager.update_checkpoint = MagicMock()

    session = requests.Session()
    all_proposals = []
    limit = 1

    # Simulate fetching with checkpointing
    offset = mock_checkpoint_manager.get_checkpoint("proposals_offset")
    proposals = fetch_proposals(session, limit=limit, offset=offset)
    all_proposals.extend(proposals)
    offset += limit
    mock_checkpoint_manager.update_checkpoint("proposals_offset", offset)

    # Assertions
    assert len(all_proposals) == 1
    assert all_proposals[0]["id"] == "proposal_1"
    mock_checkpoint_manager.update_checkpoint.assert_called_with("proposals_offset", 1)
