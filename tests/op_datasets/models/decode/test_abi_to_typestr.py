import pytest

from op_analytics.datapipeline.models.decode.abi_to_typestr import abi_inputs_to_typestr


def test_empty_abi():
    abi: dict = {}
    with pytest.raises(KeyError):
        abi_inputs_to_typestr(abi)


def test_simple_types():
    abi = {
        "inputs": [
            {"type": "uint256", "name": "a"},
            {"type": "address", "name": "b"},
            {"type": "bool", "name": "c"},
        ]
    }
    assert abi_inputs_to_typestr(abi) == ["uint256", "address", "bool"]


def test_array_types():
    abi = {
        "inputs": [
            {"type": "uint256[]", "name": "a"},
            {"type": "address[]", "name": "b"},
            {"type": "bool[]", "name": "c"},
        ]
    }
    assert abi_inputs_to_typestr(abi) == ["uint256[]", "address[]", "bool[]"]


def test_struct_type():
    abi = {
        "inputs": [
            {
                "type": "tuple",
                "components": [
                    {"type": "uint256", "name": "b"},
                    {"type": "address", "name": "c"},
                ],
                "name": "a",
            },
        ]
    }
    assert abi_inputs_to_typestr(abi) == ["(uint256,address)"]


def test_array_of_structs():
    abi = {
        "inputs": [
            {
                "type": "tuple[]",
                "components": [
                    {"type": "uint256", "name": "b"},
                    {"type": "address", "name": "c"},
                ],
                "name": "a",
            },
        ]
    }
    assert abi_inputs_to_typestr(abi) == ["(uint256,address)[]"]


def test_complex_nested():
    abi = {
        "inputs": [
            {"type": "uint256", "name": "a"},
            {
                "type": "tuple",
                "components": [
                    {"type": "address", "name": "c"},
                    {
                        "type": "tuple[]",
                        "components": [
                            {"type": "bool", "name": "e"},
                            {"type": "uint256", "name": "f"},
                        ],
                        "name": "d",
                    },
                ],
                "name": "b",
            },
        ]
    }
    assert abi_inputs_to_typestr(abi) == ["uint256", "(address,(bool,uint256)[])"]


def test_handle_ops_abi():
    abi = {
        "inputs": [
            {
                "components": [
                    {"internalType": "address", "name": "sender", "type": "address"},
                    {"internalType": "uint256", "name": "nonce", "type": "uint256"},
                    {"internalType": "bytes", "name": "initCode", "type": "bytes"},
                    {"internalType": "bytes", "name": "callData", "type": "bytes"},
                    {"internalType": "uint256", "name": "callGasLimit", "type": "uint256"},
                    {"internalType": "uint256", "name": "verificationGasLimit", "type": "uint256"},
                    {"internalType": "uint256", "name": "preVerificationGas", "type": "uint256"},
                    {"internalType": "uint256", "name": "maxFeePerGas", "type": "uint256"},
                    {"internalType": "uint256", "name": "maxPriorityFeePerGas", "type": "uint256"},
                    {"internalType": "bytes", "name": "paymasterAndData", "type": "bytes"},
                    {"internalType": "bytes", "name": "signature", "type": "bytes"},
                ],
                "internalType": "struct UserOperation[]",
                "name": "ops",
                "type": "tuple[]",
            },
            {"internalType": "address payable", "name": "beneficiary", "type": "address"},
        ],
        "name": "handleOps",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }

    assert abi_inputs_to_typestr(abi) == [
        "(address,uint256,bytes,bytes,uint256,uint256,uint256,uint256,uint256,bytes,bytes)[]",
        "address",
    ]


def test_user_operation_event_abi():
    abi = {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "bytes32", "name": "userOpHash", "type": "bytes32"},
            {"indexed": True, "internalType": "address", "name": "sender", "type": "address"},
            {"indexed": True, "internalType": "address", "name": "paymaster", "type": "address"},
            {"indexed": False, "internalType": "uint256", "name": "nonce", "type": "uint256"},
            {"indexed": False, "internalType": "bool", "name": "success", "type": "bool"},
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "actualGasCost",
                "type": "uint256",
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "actualGasUsed",
                "type": "uint256",
            },
        ],
        "name": "UserOperationEvent",
        "type": "event",
    }

    assert abi_inputs_to_typestr(abi) == ["uint256", "bool", "uint256", "uint256"]
