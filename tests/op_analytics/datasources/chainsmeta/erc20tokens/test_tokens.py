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
