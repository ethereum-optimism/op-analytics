import pytest

from op_analytics.datapipeline.models.decode.abi_to_typestr import abi_inputs_to_typestr


def test_empty_abi():
    abi: dict = {}
    with pytest.raises(KeyError):
        abi_inputs_to_typestr(abi)


def test_simple_types():
    abi = {"inputs": [{"type": "uint256"}, {"type": "address"}, {"type": "bool"}]}
    assert abi_inputs_to_typestr(abi) == ["uint256", "address", "bool"]


def test_array_types():
    abi = {"inputs": [{"type": "uint256[]"}, {"type": "address[]"}, {"type": "bool[]"}]}
    assert abi_inputs_to_typestr(abi) == ["uint256[]", "address[]", "bool[]"]


def test_struct_type():
    abi = {"inputs": [{"type": "tuple", "components": [{"type": "uint256"}, {"type": "address"}]}]}
    assert abi_inputs_to_typestr(abi) == ["(uint256,address)"]


def test_array_of_structs():
    abi = {
        "inputs": [{"type": "tuple[]", "components": [{"type": "uint256"}, {"type": "address"}]}]
    }
    assert abi_inputs_to_typestr(abi) == ["(uint256,address)[]"]


def test_complex_nested():
    abi = {
        "inputs": [
            {"type": "uint256"},
            {
                "type": "tuple",
                "components": [
                    {"type": "address"},
                    {"type": "tuple[]", "components": [{"type": "bool"}, {"type": "uint256"}]},
                ],
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
