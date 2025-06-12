from op_analytics.datasources.chainsmeta.systemconfig.systemconfig import (
    decode_address,
    decode,
    decode_string_from_abi,
)
import pytest


# --- Decode helpers ---
def test_decode_address():
    assert (
        decode_address("0x000000000000000000000000fff0000000000000000000000000000000000288")
        == "0xfff0000000000000000000000000000000000288"
    )


def test_decode():
    assert (
        decode("uint256", "0x0000000000000000000000000000000000000000000000000000000000000834")
        == 2100
    )
    # Test with different types
    assert (
        decode("uint32", "0x0000000000000000000000000000000000000000000000000000000000000001") == 1
    )
    assert (
        decode("uint64", "0x0000000000000000000000000000000000000000000000000000000000000002") == 2
    )
    # Test with invalid input
    with pytest.raises(Exception):
        decode("invalid_type", "0x00")


def test_decode_string_from_abi():
    assert (
        decode_string_from_abi(
            "0x00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000005322e352e30000000000000000000000000000000000000000000000000000000"
        )
        == "2.5.0"
    )
    # Test with invalid input
    with pytest.raises(Exception):
        decode_string_from_abi("invalid_hex")
