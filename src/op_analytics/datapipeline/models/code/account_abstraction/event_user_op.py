"""
UserOperationEvent ABI

{
    "anonymous": False,
    "inputs": [
        {"indexed": True, "internalType": "bytes32", "name": "userOpHash", "type": "bytes32"},
        {"indexed": True, "internalType": "address", "name": "sender", "type": "address"},
        {"indexed": True, "internalType": "address", "name": "paymaster", "type": "address"},
        {"indexed": False, "internalType": "uint256", "name": "nonce", "type": "uint256"},
        {"indexed": False, "internalType": "bool", "name": "success", "type": "bool"},
        {"indexed": False, "internalType": "uint256", "name": "actualGasCost", "type": "uint256"},
        {"indexed": False, "internalType": "uint256", "name": "actualGasUsed", "type": "uint256"},
    ],
    "name": "UserOperationEvent",
    "type": "event",
}
"""

from typing import Any

from duckdb.functional import PythonUDFType
from eth_abi_lite.decoding import (
    BooleanDecoder,
    ContextFramesBytesIO,
    TupleDecoder,
    decode_uint_256,
)

from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext

USER_OPS_TYPES = [
    "uint256",
    "bool",
    "uint256",
    "uint256",
]

USER_OPS_NAMES = [
    "nonce",
    "success",
    "actual_gas_cost",
    "actual_gas_used",
]

USER_OPS_DECODER = TupleDecoder(
    decoders=[
        decode_uint_256,
        BooleanDecoder(),
        decode_uint_256,
        decode_uint_256,
    ]
)


def decode_user_ops(data: str) -> dict:
    stream = ContextFramesBytesIO(bytearray.fromhex(data[2:]))

    ans: dict[str, Any] = {}
    for internal_type, name, val in zip(
        USER_OPS_TYPES,
        USER_OPS_NAMES,
        USER_OPS_DECODER.decode(stream),
    ):
        if internal_type == "uint256":
            ans[name + "_lossless"] = str(val)

            if val < 18446744073709551615:
                ans[name] = val
            else:
                ans[name] = None
        else:
            ans[name] = val

    return ans


def unregister_decode_user_ops(ctx: DuckDBContext):
    ctx.client.remove_function("decode_user_ops")


def register_decode_user_ops(ctx: DuckDBContext):
    # NOTE: DuckDB does not support converting from python to UHUGEINT
    # https://github.com/duckdb/duckdb/blob/8e68a3e34aa526a342ae91e1b14b764bb3075a12/tools/pythonpkg/src/native/python_conversion.cpp#L325
    ctx.client.create_function(
        "decode_user_ops",
        decode_user_ops,
        type=PythonUDFType.NATIVE,
        parameters=["VARCHAR"],  # type: ignore
        return_type="""
            STRUCT(
                nonce_lossless VARCHAR, 
                nonce UBIGINT, 
                success BOOL, 
                actual_gas_cost_lossless VARCHAR, 
                actual_gas_cost UBIGINT, 
                actual_gas_used_lossless VARCHAR,
                actual_gas_used UBIGINT
            )
            """,  # type: ignore
    )
