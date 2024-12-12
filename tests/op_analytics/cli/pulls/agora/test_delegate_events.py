from unittest.mock import patch
import polars as pl

from op_analytics.cli.subcommands.pulls.agora import delegates, delegate_events

sample_votes_data = [
    [
        {
            "transactionHash": "0xVoteTx1",
            "address": "0xDelegate1",
            "proposalId": "1",
            "support": 1,
            "weight": 1000,
            "reason": "Support proposal",
            "params": {"dummy": "value"},
            "proposalValue": "Proposal Value",
            "proposalTitle": "Proposal Title",
            "proposalType": "Type1",
            "timestamp": "2023-10-05",
        }
    ],
    [
        {
            "transactionHash": "0xVoteTx2",
            "address": "0xDelegate2",
            "proposalId": "2",
            "support": 0,
            "weight": 500,
            "reason": "Against proposal",
            "params": {"dummy": "value"},
            "proposalValue": "Proposal Value",
            "proposalTitle": "Proposal Title",
            "proposalType": "Type2",
            "timestamp": "2023-10-06",
        }
    ],
]

sample_proposals_data = [
    {
        "id": "1",
        "proposer": "0xProposer1",
        "snapshotBlockNumber": 123456,
        "createdTime": "2023-10-01",
        "startTime": "2023-10-02",
        "endTime": "2023-10-03",
        "cancelledTime": None,
        "executedTime": None,
        "queuedTime": None,
        "markdowntitle": "Proposal 1",
        "description": "Description 1",
        "quorum": 1000,
        "approvalThreshold": 0.5,
        "proposalType": "Type1",
        "status": "Active",
        "createdTransactionHash": "0xCreateTx1",
        "cancelledTransactionHash": None,
        "executedTransactionHash": None,
        "proposalResults": {"for": 600, "against": 400, "abstain": 0},
    }
]


def test_fetch_delegate_votes():
    with patch(
        "op_analytics.cli.subcommands.pulls.agora.delegate_events.fetch_event_data"
    ) as mock_fetch:
        # Mock fetch_event_data to return sample votes data
        mock_fetch.return_value = sample_votes_data

        # Create a mock AgoraDelegates object
        delegates_df = pl.DataFrame(
            {
                "address": ["0xDelegate1", "0xDelegate2"],
                "voting_power_total": [1000, 500],
            }
        )
        mock_delegates = delegates.AgoraDelegates(delegates_df=delegates_df)

        # Call the function under test
        result = delegate_events.fetch_delegate_votes(mock_delegates)

        # Assertions
        assert isinstance(result, delegate_events.AgoraDelegateVotes)
        assert len(result.votes_df) == 2
        assert set(result.votes_df["address"]) == {"0xDelegate1", "0xDelegate2"}


def test_fetch_proposals():
    with patch(
        "op_analytics.cli.subcommands.pulls.agora.delegate_events.SimplePaginator.fetch_all"
    ) as mock_fetch_all:
        # Mock fetch_all to return sample proposals data
        mock_fetch_all.return_value = sample_proposals_data

        # Mock the write method to prevent actual file I/O
        with patch(
            "op_analytics.cli.subcommands.pulls.agora.dataaccess.Agora.PROPOSALS.write"
        ) as mock_write:
            # Call the function under test
            delegate_events.fetch_proposals()

            # Assertions
            assert mock_write.called
            args, kwargs = mock_write.call_args
            df_written = kwargs["dataframe"]
            assert len(df_written) == 1
            assert df_written["id"][0] == "1"


def test_fetch_delegate_delegators():
    with patch(
        "op_analytics.cli.subcommands.pulls.agora.delegate_events.fetch_delegate_data"
    ) as mock_fetch_data:
        # Sample data for delegators
        sample_delegators = pl.DataFrame(
            {
                "dt": ["2023-10-05"],
                "from_address": ["0xDelegator1"],
                "to_address": ["0xDelegate1"],
                "allowance": [100],
                "type": ["type1"],
                "amount": [1000],
                "transaction_hash": ["0xTx1"],
            }
        )
        mock_fetch_data.return_value = sample_delegators

        # Create a mock AgoraDelegates object
        delegates_df = pl.DataFrame(
            {
                "address": ["0xDelegate1"],
                "voting_power_total": [1000],
            }
        )
        mock_delegates = delegates.AgoraDelegates(delegates_df=delegates_df)

        # Call the function under test
        result = delegate_events.fetch_delegate_delegators(mock_delegates)

        # Assertions
        assert len(result) == 1
        assert result["from_address"][0] == "0xDelegator1"


def test_fetch_delegate_delegatees():
    with patch(
        "op_analytics.cli.subcommands.pulls.agora.delegate_events.fetch_delegate_data"
    ) as mock_fetch_data:
        # Sample data for delegatees
        sample_delegatees = pl.DataFrame(
            {
                "dt": ["2023-10-06"],
                "from_address": ["0xDelegate1"],
                "to_address": ["0xDelegatee1"],
                "allowance": [200],
                "type": ["type2"],
                "amount": [500],
                "transaction_hash": ["0xTx2"],
            }
        )
        mock_fetch_data.return_value = sample_delegatees

        # Create a mock AgoraDelegates object
        delegates_df = pl.DataFrame(
            {
                "address": ["0xDelegate1"],
                "voting_power_total": [1000],
            }
        )
        mock_delegates = delegates.AgoraDelegates(delegates_df=delegates_df)

        # Call the function under test
        result = delegate_events.fetch_delegate_delegatees(mock_delegates)

        # Assertions
        assert len(result) == 1
        assert result["to_address"][0] == "0xDelegatee1"
