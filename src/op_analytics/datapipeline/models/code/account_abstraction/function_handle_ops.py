from duckdb.functional import PythonUDFType
from eth_abi_lite.decoding import ContextFramesBytesIO, TupleDecoder
from eth_utils_lite.hexadecimal import encode_hex

from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext
from op_analytics.datapipeline.models.decode.abi_to_decoder import abi_inputs_to_decoder
from op_analytics.datapipeline.models.decode.conversion import safe_uint256

from .abis import (
    HANDLE_OPS_FUNCTION_ABI_v0_6_0,
    HANDLE_OPS_FUNCTION_ABI_v0_7_0,
    HANDLE_OPS_FUNCTION_METHOD_ID_v0_6_0,
    HANDLE_OPS_FUNCTION_METHOD_ID_v0_7_0,
)

DUCKDB_FUNCTION_NAME = "decode_handle_ops_input"


# TODO: Make sure that all uint256 values are lossless
def structure_v0_6_0(user_op, beneficiary):
    return {
        "sender": user_op[0],
        "call_gas_limit": safe_uint256(user_op[4]),
        "pre_verification_gas": safe_uint256(user_op[5]),
        "max_fee_per_gas": safe_uint256(user_op[6]),
        "max_priority_fee_per_gas": safe_uint256(user_op[7]),
        "account_gas_limits": None,  # v0.7.0 field
        "gas_fees": None,  # v0.7.0 field
        "beneficiary": beneficiary,
    }


def structure_v0_7_0(user_op, beneficiary):
    return {
        "sender": user_op[0],
        "call_gas_limit": None,  # v0.6.0 field
        "pre_verification_gas": safe_uint256(user_op[5]),
        "max_fee_per_gas": None,  # v0.6.0 field
        "max_priority_fee_per_gas": None,  # v0.6.0 field
        "account_gas_limits": encode_hex(user_op[4]),
        "gas_fees": encode_hex(user_op[6]),
        "beneficiary": beneficiary,
    }


def unregister_decode_handle_ops_input(ctx: DuckDBContext):
    ctx.client.remove_function(DUCKDB_FUNCTION_NAME)


def register_decode_handle_ops_input(ctx: DuckDBContext):
    decoder_v0_6_0: TupleDecoder = abi_inputs_to_decoder(HANDLE_OPS_FUNCTION_ABI_v0_6_0)
    decoder_v0_7_0: TupleDecoder = abi_inputs_to_decoder(HANDLE_OPS_FUNCTION_ABI_v0_7_0)

    def _decode(data: str):
        try:
            method_id = data[:10]
        except Exception as ex:
            return {
                "user_ops": None,
                "decoding_status": str(ex) + "\n" + data,
                "method_id": None,
                "contract_name": None,
            }

        try:
            # skip 2 for "0x"
            # skip 8 for method id
            stream = ContextFramesBytesIO(bytearray.fromhex(data[10:]))

            # We select the decoder and structure implementations based on the contract version.

            if method_id == HANDLE_OPS_FUNCTION_METHOD_ID_v0_6_0:
                result = decoder_v0_6_0.decode(stream)
                return {
                    "user_ops": [structure_v0_6_0(user_op, result[1]) for user_op in result[0]],
                    "decoding_status": "ok",
                    "method_id": method_id,
                    "contract_name": "v0_6_0",
                }

            elif method_id == HANDLE_OPS_FUNCTION_METHOD_ID_v0_7_0:
                result = decoder_v0_7_0.decode(stream)
                return {
                    "user_ops": [structure_v0_7_0(user_op, result[1]) for user_op in result[0]],
                    "decoding_status": "ok",
                    "method_id": method_id,
                    "contract_name": "v0_7_0",
                }

            else:
                # Unsupported method_id
                return {
                    "user_ops": None,
                    "decoding_status": "unsupported",
                    "method_id": method_id,
                    "contract_name": None,
                }

        except Exception as ex:
            {
                "user_ops": None,
                "decoding_status": str(ex) + "\n" + data,
                "method_id": method_id,
                "contract_name": None,
            }

    # NOTE: DuckDB does not support converting from python to UHUGEINT
    # https://github.com/duckdb/duckdb/blob/8e68a3e34aa526a342ae91e1b14b764bb3075a12/tools/pythonpkg/src/native/python_conversion.cpp#L325
    ctx.client.create_function(
        DUCKDB_FUNCTION_NAME,
        _decode,
        type=PythonUDFType.NATIVE,
        parameters=["VARCHAR"],  # type: ignore
        # return_type="VARCHAR",
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
            """,  # type: ignore
    )
