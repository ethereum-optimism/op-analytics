from unittest.mock import patch

from op_analytics.datasources.agora import delegates, delegate_events
import polars as pl

sample_delegates_data = [
    {
        "address": "0xDelegate1",
        "votingPower": {"total": 1000, "direct": 500, "advanced": 500},
        "citizen": True,
        "statement": {
            "signature": "signature1",
            "created_at": "2023-10-01T00:00:00Z",
            "updated_at": "2023-10-02T00:00:00Z",
            "payload": {
                "delegateStatement": "Statement1",
                "twitter": "@delegate1",
                "discord": "delegate1#1234",
            },
        },
    },
    {
        "address": "0xDelegate2",
        "votingPower": {"total": 500, "direct": 300, "advanced": 200},
        "citizen": False,
        "statement": {
            "signature": "signature2",
            "created_at": "2023-10-03T00:00:00Z",
            "updated_at": "2023-10-04T00:00:00Z",
            "payload": {
                "delegateStatement": "Statement2",
                "twitter": "@delegate2",
                "discord": "delegate2#5678",
            },
        },
    },
]

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
            "timestamp": "2023-10-05T00:00:00Z",
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
            "timestamp": "2023-10-06T00:00:00Z",
        }
    ],
]

sample_proposals_data = [
    {
        "id": "1",
        "proposer": "0xProposer1",
        "snapshotBlockNumber": 123456,
        "createdTime": "2023-10-01T00:00:00Z",
        "startTime": "2023-10-02T00:00:00Z",
        "endTime": "2023-10-03T00:00:00Z",
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
        "proposalData": {},  # Added empty dict to prevent empty Struct error
        "unformattedProposalData": {},  # Added empty dict
    }
]


def test_pull_delegates():
    with patch.object(delegates.Paginator, "request") as mock_request:
        mock_response = delegates.PaginatedResponse(
            has_next=False, next_offset=0, data=sample_delegates_data
        )
        mock_request.return_value = mock_response

        # Corrected the patching of binary_search_max_offset
        with patch.object(delegates.Paginator, "binary_search_max_offset", return_value=100):
            # Mock the write method to prevent actual file I/O
            with patch.object(delegates.Agora.DELEGATES, "write"):
                result = delegates.pull_delegates()

                # Assertions
                assert isinstance(result, delegates.AgoraDelegates)
                assert len(result.delegates_df) == 2
                assert set(result.delegates_df["address"]) == {"0xDelegate1", "0xDelegate2"}
                assert "voting_power_total" in result.delegates_df.columns


def test_fetch_delegate_votes():
    with patch("op_analytics.datasources.agora.delegate_events.fetch_event_data") as mock_fetch:
        mock_fetch.return_value = sample_votes_data

        delegates_df = pl.DataFrame(
            {
                "address": ["0xDelegate1", "0xDelegate2"],
                "voting_power_total": [1000, 500],
            }
        )
        mock_delegates = delegates.AgoraDelegates(delegates_df=delegates_df)

        # Mock the write method to prevent actual file I/O
        with patch.object(delegates.Agora.DELEGATE_VOTES, "write"):
            result = delegate_events.fetch_delegate_votes(mock_delegates)

            # Assertions
            assert isinstance(result, delegate_events.AgoraDelegateVotes)
            assert len(result.votes_df) == 2
            assert set(result.votes_df["address"]) == {"0xDelegate1", "0xDelegate2"}


def test_fetch_proposals():
    with patch(
        "op_analytics.datasources.agora.delegate_events.SimplePaginator.fetch_all"
    ) as mock_fetch_all:
        mock_fetch_all.return_value = sample_proposals_data

        # Mock the write method to prevent actual file I/O
        with patch.object(delegates.Agora.PROPOSALS, "write") as mock_write:
            result = delegate_events.fetch_proposals()

            # Assertions
            assert type(result) is pl.DataFrame
            mock_write.assert_called_once()
            args, kwargs = mock_write.call_args
            df_written = kwargs["dataframe"]
            assert len(df_written) == 1
            assert df_written["id"][0] == "1"


def test_fetch_delegate_delegators():
    with patch(
        "op_analytics.datasources.agora.delegate_events.fetch_delegate_data"
    ) as mock_fetch_data:
        sample_delegators = pl.DataFrame(
            {
                "dt": ["2023-10-05"],  # Corrected date format
                "from_address": ["0xDelegator1"],
                "to_address": ["0xDelegate1"],
                "allowance": [100],
                "type": ["type1"],
                "amount": [1000],
                "transaction_hash": ["0xTx1"],
            }
        )
        mock_fetch_data.return_value = sample_delegators

        delegates_df = pl.DataFrame(
            {
                "address": ["0xDelegate1"],
                "voting_power_total": [1000],
            }
        )
        mock_delegates = delegates.AgoraDelegates(delegates_df=delegates_df)

        # Mock the write method to prevent actual file I/O
        with patch.object(delegates.Agora.DELEGATORS, "write"):
            result = delegate_events.fetch_delegate_delegators(mock_delegates)

            # Assertions
            assert len(result) == 1
            assert result["from_address"][0] == "0xDelegator1"


def test_fetch_delegate_delegatees():
    with patch(
        "op_analytics.datasources.agora.delegate_events.fetch_delegate_data"
    ) as mock_fetch_data:
        sample_delegatees = pl.DataFrame(
            {
                "dt": ["2023-10-06"],  # Corrected date format
                "from_address": ["0xDelegate1"],
                "to_address": ["0xDelegatee1"],
                "allowance": [200],
                "type": ["type2"],
                "amount": [500],
                "transaction_hash": ["0xTx2"],
            }
        )
        mock_fetch_data.return_value = sample_delegatees

        delegates_df = pl.DataFrame(
            {
                "address": ["0xDelegate1"],
                "voting_power_total": [1000],
            }
        )
        mock_delegates = delegates.AgoraDelegates(delegates_df=delegates_df)

        # Mock the write method to prevent actual file I/O
        with patch.object(delegates.Agora.DELEGATEES, "write"):
            result = delegate_events.fetch_delegate_delegatees(mock_delegates)

            # Assertions
            assert len(result) == 1
            assert result["to_address"][0] == "0xDelegatee1"
