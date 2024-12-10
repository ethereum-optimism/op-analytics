from unittest.mock import patch

from op_analytics.cli.subcommands.pulls.agora import delegates
from op_analytics.cli.subcommands.pulls.agora.delegates import Paginator, PaginatedResponse

sample_delegates_data = [
    {
        "address": "0xDelegate1",
        "votingPower": {"total": 1000, "direct": 500, "advanced": 500},
        "citizen": True,
        "statement": {
            "signature": "signature1",
            "created_at": "2023-10-01",
            "updated_at": "2023-10-02",
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
            "created_at": "2023-10-03",
            "updated_at": "2023-10-04",
            "payload": {
                "delegateStatement": "Statement2",
                "twitter": "@delegate2",
                "discord": "delegate2#5678",
            },
        },
    },
]


def test_pull_delegates():
    with patch(
        "op_analytics.cli.subcommands.pulls.agora.delegates.Paginator.request"
    ) as mock_request:
        # Mock the paginator's request method to return sample data
        mock_response = delegates.PaginatedResponse(
            has_next=False, next_offset=0, data=sample_delegates_data
        )
        mock_request.return_value = mock_response

        # Mock the binary search to return a valid max_offset
        with patch(
            "op_analytics.cli.subcommands.pulls.agora.delegates.Paginator.binary_search_max_offset",
            return_value=100,
        ):
            result = delegates.pull_delegates()

            # Assertions
            assert isinstance(result, delegates.AgoraDelegates)
            assert len(result.delegates_df) == 2
            assert set(result.delegates_df["address"]) == {"0xDelegate1", "0xDelegate2"}
            assert "voting_power_total" in result.delegates_df.columns


def test_binary_search_max_offset():
    def mock_request(offset):
        if offset < 100:
            return PaginatedResponse(has_next=True, next_offset=offset + 10, data=[])
        else:
            return PaginatedResponse(has_next=False, next_offset=offset, data=[])

    with patch.object(Paginator, "request", side_effect=mock_request):
        paginator = Paginator(url="mock_url", params={})
        max_offset = paginator.binary_search_max_offset()

        # Assertions
        assert max_offset == 100, f"Expected max_offset to be 100, but got {max_offset}"
