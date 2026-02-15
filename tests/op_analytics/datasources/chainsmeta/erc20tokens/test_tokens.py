from op_analytics.datasources.chainsmeta.erc20tokens.tokens import (
    Token,
    TokenMetadata,
    decode_string,
)


def test_decode_string():
    data = "0x000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000054265744d65000000000000000000000000000000000000000000000000000000"
    actual = decode_string(data)
    assert actual == "BetMe"


def test_decode_response():
    response: list[dict] = [
        {
            "jsonrpc": "2.0",
            "result": {
                "number": "0x1934a94",
                "timestamp": "0x67b0f20b",
            },
            "id": "block",
        },
        {
            "jsonrpc": "2.0",
            "result": "0x0000000000000000000000000000000000000000000000000000000000000012",
            "id": "decimals",
        },
        {
            "jsonrpc": "2.0",
            "result": "0x000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000054265744d65000000000000000000000000000000000000000000000000000000",
            "id": "symbol",
        },
        {
            "jsonrpc": "2.0",
            "result": "0x000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000054265744d65000000000000000000000000000000000000000000000000000000",
            "id": "name",
        },
        {
            "jsonrpc": "2.0",
            "result": "0x0000000000000000000000000000000000000000033b2e3c9fd0803ce8000000",
            "id": "totalSupply",
        },
    ]

    token = Token(chain="dummy", chain_id=0, contract_address="0x000")
    actual = TokenMetadata.of(token, response)
    assert actual == TokenMetadata(
        chain="dummy",
        chain_id=0,
        contract_address="0x000",
        block_number=26430100,
        block_timestamp=1739649547,
        decimals=18,
        symbol="BetMe",
        name="BetMe",
        total_supply=1000000000000000000000000000,
    )

    assert actual.to_dict() == {
        "chain": "dummy",
        "chain_id": 0,
        "contract_address": "0x000",
        "block_number": 26430100,
        "block_timestamp": 1739649547,
        "decimals": 18,
        "symbol": "BetMe",
        "name": "BetMe",
        "total_supply": 1000000000000000000000000000,
    }


def test_error_handling_missing_functions():
    """Test that tokens with missing functions are handled gracefully."""
    # Test with error code 3 (contract doesn't have fallback/receive functions)
    response_with_error_3: list[dict] = [
        {
            "jsonrpc": "2.0",
            "result": {
                "number": "0x1934a94",
                "timestamp": "0x67b0f20b",
            },
            "id": "block",
        },
        {
            "jsonrpc": "2.0",
            "error": {
                "code": 3,
                "message": "execution reverted: Contract does not have fallback nor receive functions",
                "data": "0x08c379a000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000035436f6e747261637420646f6573206e6f7420686176652066616c6c6261636b206e6f7220726563656976652066756e6374696f6e730000000000000000000000",
            },
            "id": "decimals",
        },
        {
            "jsonrpc": "2.0",
            "error": {
                "code": 3,
                "message": "execution reverted: Contract does not have fallback nor receive functions",
            },
            "id": "symbol",
        },
        {
            "jsonrpc": "2.0",
            "error": {
                "code": 3,
                "message": "execution reverted: Contract does not have fallback nor receive functions",
            },
            "id": "name",
        },
        {
            "jsonrpc": "2.0",
            "error": {
                "code": 3,
                "message": "execution reverted: Contract does not have fallback nor receive functions",
            },
            "id": "totalSupply",
        },
    ]

    token = Token(
        chain="base", chain_id=8453, contract_address="0xece616e29f54d124542e8d7ae5296f73640f8002"
    )
    result = TokenMetadata.of(token, response_with_error_3)

    # Should return None because all required methods failed
    assert result is None

    # Test with error code -32000 (execution reverted)
    response_with_error_32000: list[dict] = [
        {
            "jsonrpc": "2.0",
            "result": {
                "number": "0x1934a94",
                "timestamp": "0x67b0f20b",
            },
            "id": "block",
        },
        {
            "jsonrpc": "2.0",
            "error": {"code": -32000, "message": "execution reverted"},
            "id": "decimals",
        },
        {
            "jsonrpc": "2.0",
            "result": "0x000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000054265744d65000000000000000000000000000000000000000000000000000000",
            "id": "symbol",
        },
        {
            "jsonrpc": "2.0",
            "result": "0x000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000054265744d65000000000000000000000000000000000000000000000000000000",
            "id": "name",
        },
        {
            "jsonrpc": "2.0",
            "result": "0x0000000000000000000000000000000000000000033b2e3c9fd0803ce8000000",
            "id": "totalSupply",
        },
    ]

    result = TokenMetadata.of(token, response_with_error_32000)

    # Should return None because decimals method failed (required)
    assert result is None

    # Test with partial success (some methods work, some fail)
    response_partial_success: list[dict] = [
        {
            "jsonrpc": "2.0",
            "result": {
                "number": "0x1934a94",
                "timestamp": "0x67b0f20b",
            },
            "id": "block",
        },
        {
            "jsonrpc": "2.0",
            "result": "0x0000000000000000000000000000000000000000000000000000000000000012",
            "id": "decimals",
        },
        {
            "jsonrpc": "2.0",
            "result": "0x000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000054265744d65000000000000000000000000000000000000000000000000000000",
            "id": "symbol",
        },
        {
            "jsonrpc": "2.0",
            "error": {
                "code": 3,
                "message": "execution reverted: Contract does not have fallback nor receive functions",
            },
            "id": "name",
        },
        {
            "jsonrpc": "2.0",
            "result": "0x0000000000000000000000000000000000000000033b2e3c9fd0803ce8000000",
            "id": "totalSupply",
        },
    ]

    result = TokenMetadata.of(token, response_partial_success)

    # Should return None because name method failed (required)
    assert result is None


def test_invalid_strings():
    """Some tokens have invalid utf-8 on their symbol or name."""

    example = {
        "token": Token(
            chain="base",
            chain_id=8453,
            contract_address="0x13506932a68f87cc3a9b319628dbe63621318d2a",
        ),
        "asbytes": b"\x13Pi2\xa6\x8f\x87\xcc:\x9b1\x96(\xdb\xe66!1\x8d*",
        "data": {
            "block_number": 26467597,
            "block_timestamp": 1739724541,
            "decimals": "0x0000000000000000000000000000000000000000000000000000000000000012",
            "symbol": "0x000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000034641550000000000000000000000000000000000000000000000000000000000",
            "name": "0x0000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000001413506932a68f87cc3a9b319628dbe63621318d2a000000000000000000000000",
            "totalSupply": "0x000000000000000000000000000000000000000000084595161401484a000000",
        },
    }

    actual = decode_string(example["data"]["name"])  # type: ignore
    assert actual == "\x13Pi2����:�1�(��6!1�*"
