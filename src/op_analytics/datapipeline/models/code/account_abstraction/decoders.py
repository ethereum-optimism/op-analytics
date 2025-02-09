import json
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
        # TODO:
        # Get the callData out of the json because it is too big. Also context, get it out. Only keep opInfo.
        # Get all top level fields out of the json
        # We separate callData and context because they are too big and not likely to get used,
        # It helps not to have them in the json field so that json udfs don't have to parse them
        # unnecessarily.
        return_type="""
            STRUCT(
                decodeerror VARCHAR,
                opinfo_sender VARCHAR,
                opinfo_paymaster VARCHAR,
                opinfo_userophash VARCHAR,
                opinfo VARCHAR,
                calldata VARCHAR,
                context VARCHAR
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

    def _adapt(x):
        decoded = x["decoded"]

        if decoded is None:
            return dict(
                decode_error=x["decode_error"],
                opinfo_sender=None,
                opinfo_paymaster=None,
                opinfo_userophash=None,
                opinfo=None,
                calldata=None,
                context=None,
            )
        else:
            return dict(
                decode_error=x["decode_error"],
                opinfo_sender=decoded["opInfo"]["mUserOp"].pop("sender"),
                opinfo_paymaster=decoded["opInfo"]["mUserOp"].pop("paymaster"),
                opinfo_userophash=decoded["opInfo"].pop("userOpHash"),
                opinfo=json.dumps(decoded["opInfo"]),
                calldata=decoded["callData"],
                context=decoded["context"],
            )

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
        ],
        as_json=False,
        adapter=_adapt,
    )
