from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext
from op_analytics.datapipeline.models.decode.abi_to_dictdecoder import DictionaryDecoder
from op_analytics.datapipeline.models.decode.log_decoder import LogDecoder
from op_analytics.datapipeline.models.decode.method_decoder import (
    MultiMethodDecoder,
    SingleMethodDecoder,
)
from op_analytics.datapipeline.models.decode.register import register_decoder

from .abis import (
    INNER_HANDLE_OP_FUNCTION_ABI_v0_6_0,
    INNER_HANDLE_OP_FUNCTION_ABI_v0_7_0,
    INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_6_0,
    INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_7_0,
    USER_OP_EVENT_ABI_v0_6_0,
    USER_OP_EVENT_ABI_v0_7_0,
)


def register_4337_decoders(ctx: DuckDBContext):
    """Register decoders in DuckDB.

    This function is cached to prevent "function already created" errors in DuckDB.
    """

    # UserOperationEvent
    register_decoder(
        ctx=ctx,
        duckdb_function_name="decode_user_op",
        decoder=user_op_event_decoder(),
        parameters=["VARCHAR"],
        return_type="VARCHAR",
    )

    # innerHandleOp
    register_decoder(
        ctx=ctx,
        duckdb_function_name="decode_inner_handle_op",
        decoder=inner_handle_op_decoder(),
        parameters=["VARCHAR"],
        return_type="""
            STRUCT(
                method_id VARCHAR,
                decoded_json VARCHAR,
                error VARCHAR
            )
            """,
    )


def user_op_event_decoder():
    """Decoder for the UserOperationEvent log."""

    # The ABI is the same for both versions of the Entrypoint contract.
    assert USER_OP_EVENT_ABI_v0_6_0 == USER_OP_EVENT_ABI_v0_7_0
    return LogDecoder(decoder=DictionaryDecoder.of(USER_OP_EVENT_ABI_v0_6_0))


def inner_handle_op_decoder():
    """Decoder for the innerHandleOp method."""

    return MultiMethodDecoder.of(
        decoders=[
            # v0_6_0
            SingleMethodDecoder(
                method_id=INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_6_0,
                decoder=DictionaryDecoder.of(INNER_HANDLE_OP_FUNCTION_ABI_v0_6_0),
            ),
            # v0_7_0
            SingleMethodDecoder(
                method_id=INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_7_0,
                decoder=DictionaryDecoder.of(INNER_HANDLE_OP_FUNCTION_ABI_v0_7_0),
            ),
        ]
    )
