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


# Test for fetch proposals with pause and resume
@patch("op_analytics.cli.subcommands.pulls.agora.fetch.get_data")
@patch("op_analytics.cli.subcommands.pulls.agora.fetch.checkpoint_manager")
def test_fetch_proposals_with_pause_and_resume(mock_checkpoint_manager, mock_get_data):
    def fetch_proposals_with_checkpoints(session, all_proposals, limit=10, max_batches=1):
        checkpoint_key = "proposals_offset"
        # Load the last saved offset from the checkpoint manager
        offset = mock_checkpoint_manager.get_checkpoint(checkpoint_key)
        print(f"Starting fetch with offset: {offset}")

        for _ in range(max_batches):
            try:
                # Fetch a batch of proposals
                proposals = fetch_proposals(session, limit=limit, offset=offset)
                print(f"Fetched {len(proposals)} proposals from offset {offset}")

                if not proposals:
                    print("No more proposals to fetch.")
                    break  # Exit loop if no more proposals are returned

                all_proposals.extend(proposals)
                offset += min(limit, len(proposals))  # Increment offset for the next batch

                # Save the current offset to the checkpoint manager
                mock_checkpoint_manager.update_checkpoint(checkpoint_key, offset)
                print(f"Updated checkpoint to offset: {offset}")

            except Exception as e:
                print(f"Error during fetch_proposals: {e}")
                break

    # Mock response for proposals
    mock_get_data.side_effect = [
        [{"id": "proposal_1"}],
        [{"id": "proposal_2"}],
        [],  # No more proposals
    ]

    # Mock checkpoint manager behavior
    mock_checkpoint_manager.get_checkpoint.return_value = 0
    mock_checkpoint_manager.update_checkpoint = MagicMock()
    # Reset the checkpoint for testing purposes
    mock_checkpoint_manager.update_checkpoint("proposals_offset", 0)

    session = requests.Session()
    all_proposals = []

    # Simulate the first run
    fetch_proposals_with_checkpoints(session, all_proposals, max_batches=2)

    # Simulate a pause
    # Simulate the second run
    fetch_proposals_with_checkpoints(session, all_proposals, max_batches=2)

    # Final fetch to complete the process
    fetch_proposals_with_checkpoints(session, all_proposals, max_batches=10)

    print(all_proposals)
    # Assertions
    assert len(all_proposals) == 2
    assert all_proposals[0]["id"] == "proposal_1"
    assert all_proposals[1]["id"] == "proposal_2"
    mock_checkpoint_manager.update_checkpoint.assert_called_with("proposals_offset", 2)
