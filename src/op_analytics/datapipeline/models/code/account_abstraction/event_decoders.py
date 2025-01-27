from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext
from op_analytics.datapipeline.models.decode.abi_to_decoder import abi_inputs_to_decoder
from op_analytics.datapipeline.models.decode.conversion import safe_uint256
from op_analytics.datapipeline.models.decode.log_decoder import LogDecoder
from op_analytics.datapipeline.models.decode.register import register_decoder

from .abis import (
    USER_OP_EVENT_ABI_v0_6_0,
    USER_OP_EVENT_ABI_v0_7_0,
)


def register_4337_event_decoders(ctx: DuckDBContext):
    """Register decoders in DuckDB."""
    # UserOperationEvent
    register_decoder(
        ctx=ctx,
        duckdb_function_name="decode_user_ops",
        decoder=user_op_event_decoder(),
        parameters=["VARCHAR"],
        return_type="""
             STRUCT(
                nonce BIGINT, 
                nonce_lossless VARCHAR, 
                success BOOL, 
                actual_gas_cost BIGINT, 
                actual_gas_cost_lossless VARCHAR, 
                actual_gas_used BIGINT,
                actual_gas_used_lossless VARCHAR
            )
            """,
    )


def user_op_event_decoder():
    """Decoder for the UserOperationEvent log."""

    # The ABI is the same for both versions of the Entrypoint contract.
    assert USER_OP_EVENT_ABI_v0_6_0 == USER_OP_EVENT_ABI_v0_7_0

    return LogDecoder(
        decoder=abi_inputs_to_decoder(USER_OP_EVENT_ABI_v0_6_0),
        to_dict=lambda result: dict(
            nonce=safe_uint256(result[0]),  # uint256
            nonce_lossless=str(result[0]),
            success=result[1],
            actual_gas_cost=safe_uint256(result[2]),  # uint256
            actual_gas_cost_lossless=str(result[2]),
            actual_gas_used=safe_uint256(result[3]),  # uint256
            actual_gas_used_lossless=str(result[3]),
        ),
    )
