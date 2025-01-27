from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext
from op_analytics.datapipeline.models.decode.abi_to_decoder import abi_inputs_to_decoder
from op_analytics.datapipeline.models.decode.register import register_decoder
from op_analytics.datapipeline.models.decode.method_decoder import (
    MultiMethodDecoder,
    SingleMethodDecoder,
)
from op_analytics.datapipeline.models.decode.abi_to_structmaker import make_struct, make_duckdb_type

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
    """Register decode functions in DuckDB."""
    # innerHandleOp

    v6_type = make_duckdb_type(INNER_HANDLE_OP_FUNCTION_ABI_v0_6_0)
    v7_type = make_duckdb_type(INNER_HANDLE_OP_FUNCTION_ABI_v0_7_0)

    register_decoder(
        ctx=ctx,
        duckdb_function_name="decode_inner_handle_op",
        decoder=inner_handle_op_decoder(),
        parameters=["VARCHAR"],
        return_type=f"""
            STRUCT(
                v0_6_0 {v6_type},
                v0_7_0 {v7_type},
                decoding_status VARCHAR,
                method_id VARCHAR
            )
            """,
    )


def inner_handle_op_decoder():
    """Decoder for the innerHandleOp method."""

    v0_6_0 = SingleMethodDecoder(
        method_id=INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_6_0,
        decoder=abi_inputs_to_decoder(INNER_HANDLE_OP_FUNCTION_ABI_v0_6_0),
        to_dict=lambda result: {
            "v0_6_0": make_struct(
                abi_entry=INNER_HANDLE_OP_FUNCTION_ABI_v0_6_0,
                decoded_result=result,
            ),
            "v0_7_0": None,
        },
    )

    v0_7_0 = SingleMethodDecoder(
        method_id=INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_7_0,
        decoder=abi_inputs_to_decoder(INNER_HANDLE_OP_FUNCTION_ABI_v0_7_0),
        to_dict=lambda result: {
            "v0_6_0": None,
            "v0_7_0": make_struct(
                abi_entry=INNER_HANDLE_OP_FUNCTION_ABI_v0_7_0,
                decoded_result=result,
            ),
        },
    )

    return MultiMethodDecoder.of(
        decoders=[v0_6_0, v0_7_0],
        default_result={"v0_6_0": None, "v0_7_0": None},
    )


def handle_ops_decoder():
    """Decoder for the handleOps method."""

    v0_6_0 = SingleMethodDecoder(
        method_id=HANDLE_OPS_FUNCTION_METHOD_ID_v0_6_0,
        decoder=abi_inputs_to_decoder(HANDLE_OPS_FUNCTION_ABI_v0_6_0),
        to_dict=lambda result: {
            "v0_6_0": make_struct(
                abi_entry=HANDLE_OPS_FUNCTION_ABI_v0_6_0,
                decoded_result=result,
            ),
            "v0_7_0": None,
        },
    )

    v0_7_0 = SingleMethodDecoder(
        method_id=HANDLE_OPS_FUNCTION_METHOD_ID_v0_7_0,
        decoder=abi_inputs_to_decoder(HANDLE_OPS_FUNCTION_ABI_v0_7_0),
        to_dict=lambda result: {
            "v0_6_0": None,
            "v0_7_0": make_struct(
                abi_entry=HANDLE_OPS_FUNCTION_ABI_v0_7_0,
                decoded_result=result,
            ),
        },
    )

    return MultiMethodDecoder.of(
        decoders=[v0_6_0, v0_7_0],
        default_result={"user_ops": None, "contract_name": "v0_7_0"},
    )
