from eth_utils_lite.hexadecimal import encode_hex

from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext
from op_analytics.datapipeline.models.decode.abi_to_decoder import abi_inputs_to_decoder
from op_analytics.datapipeline.models.decode.conversion import safe_uint256
from op_analytics.datapipeline.models.decode.register import register_decoder
from op_analytics.datapipeline.models.decode.method_decoder import (
    MultiMethodDecoder,
    SingleMethodDecoder,
)

from .abis import (
    HANDLE_OPS_FUNCTION_ABI_v0_6_0,
    HANDLE_OPS_FUNCTION_ABI_v0_7_0,
    HANDLE_OPS_FUNCTION_METHOD_ID_v0_6_0,
    HANDLE_OPS_FUNCTION_METHOD_ID_v0_7_0,
    INNER_HANDLE_OP_FUNCTION_ABI_v0_6_0,
    INNER_HANDLE_OP_FUNCTION_ABI_v0_7_0,
    INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_6_0,
    INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_7_0,
)


def register_4337_function_decoders(ctx: DuckDBContext):
    """Register decode functions in DuckDB.


    Supports "innerHandleOp" and "handleOps" methods in Entrypoint v0_6_0 and v0_7_0.
    """
    # innerHandleOp
    register_decoder(
        ctx=ctx,
        duckdb_function_name="decode_inner_handle_op",
        decoder=inner_handle_op_decoder(),
        parameters=["VARCHAR"],
        return_type="""
            STRUCT(

                sender VARCHAR,

                decoding_status VARCHAR,

                method_id VARCHAR,
                
                contract_name VARCHAR
            )
            """,
    )

    # handleOps
    register_decoder(
        ctx=ctx,
        duckdb_function_name="decode_handle_ops",
        decoder=handle_ops_decoder(),
        parameters=["VARCHAR"],
        return_type="""
            STRUCT(

                user_ops STRUCT(
                    sender VARCHAR,
                    call_gas_limit UBIGINT,
                    pre_verification_gas UBIGINT,
                    max_fee_per_gas UBIGINT,
                    max_priority_fee_per_gas UBIGINT,
                    account_gas_limits VARCHAR,
                    gas_fees VARCHAR,
                    beneficiary VARCHAR
                    )[],

                decoding_status VARCHAR,

                method_id VARCHAR,
                
                contract_name VARCHAR
            )
            """,
    )


def inner_handle_op_decoder():
    """Decoder for the innerHandleOp method."""

    v0_6_0 = SingleMethodDecoder(
        method_id=INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_6_0,
        decoder=abi_inputs_to_decoder(INNER_HANDLE_OP_FUNCTION_ABI_v0_6_0),
        to_dict=lambda result: {
            # [1][0][0] -> opInfo.mUserOp.sender
            "sender": result[1][0][0],
            "contract_name": "v0_6_0",
        },
    )

    v0_7_0 = SingleMethodDecoder(
        method_id=INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_7_0,
        decoder=abi_inputs_to_decoder(INNER_HANDLE_OP_FUNCTION_ABI_v0_7_0),
        to_dict=lambda result: {
            # [1][0][0] -> opInfo.mUserOp.sender
            "sender": result[1][0][0],
            "contract_name": "v0_7_0",
        },
    )

    return MultiMethodDecoder.of(
        decoders=[v0_6_0, v0_7_0],
        default_result={"sender": None, "contract_name": None},
    )


def handle_ops_decoder():
    """Decoder for the handleOps method."""

    v0_6_0 = SingleMethodDecoder(
        method_id=HANDLE_OPS_FUNCTION_METHOD_ID_v0_6_0,
        decoder=abi_inputs_to_decoder(HANDLE_OPS_FUNCTION_ABI_v0_6_0),
        to_dict=lambda result: {
            "user_ops": [
                {
                    "sender": user_op[0],
                    "call_gas_limit": safe_uint256(user_op[4]),
                    "pre_verification_gas": safe_uint256(user_op[5]),
                    "max_fee_per_gas": safe_uint256(user_op[6]),
                    "max_priority_fee_per_gas": safe_uint256(user_op[7]),
                    "account_gas_limits": None,  # v0.7.0 field
                    "gas_fees": None,  # v0.7.0 field
                    "beneficiary": result[1],
                }
                for user_op in result[0]
            ],
            "contract_name": "v0_6_0",
        },
    )

    v0_7_0 = SingleMethodDecoder(
        method_id=HANDLE_OPS_FUNCTION_METHOD_ID_v0_7_0,
        decoder=abi_inputs_to_decoder(HANDLE_OPS_FUNCTION_ABI_v0_7_0),
        to_dict=lambda result: {
            "user_ops": [
                {
                    "sender": user_op[0],
                    "call_gas_limit": None,  # v0.6.0 field
                    "pre_verification_gas": safe_uint256(user_op[5]),
                    "max_fee_per_gas": None,  # v0.6.0 field
                    "max_priority_fee_per_gas": None,  # v0.6.0 field
                    "account_gas_limits": encode_hex(user_op[4]),
                    "gas_fees": encode_hex(user_op[6]),
                    "beneficiary": result[1],
                }
                for user_op in result[0]
            ],
            "contract_name": "v0_7_0",
        },
    )

    return MultiMethodDecoder.of(
        decoders=[v0_6_0, v0_7_0],
        default_result={"user_ops": None, "contract_name": "v0_7_0"},
    )
