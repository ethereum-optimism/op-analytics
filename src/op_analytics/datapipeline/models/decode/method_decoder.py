from dataclasses import dataclass
from typing import Any, Callable, NewType

from duckdb.typing import DuckDBPyType
from duckdb.functional import PythonUDFType
from eth_abi_lite.decoding import ContextFramesBytesIO, TupleDecoder

from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext


DUCKDB_FUNCTION_NAME = "decode_inner_handle_op"


MethodId = NewType("MethodId", str)


@dataclass
class SingleMethodDecoder:
    """Decode data for a single method."""

    # Method ID associated with this decoder:
    method_id: int

    # Decoder instance. Converts bytestream to python objects.
    decoder: TupleDecoder

    # Result converter. Extracts specific fields from the result
    # produced by the decoder.
    to_dict: Callable[[str, Any], dict]

    def decode(self, stream: ContextFramesBytesIO):
        result = self.decoder.decode(stream)
        return self.to_dict(result)


@dataclass
class MultiMethodDecoder:
    """Dispatch to a SingleMethodDecoder based on method_id."""

    decoders: dict[MethodId, SingleMethodDecoder]

    default_result: dict

    @classmethod
    def of(cls, decoders: list[SingleMethodDecoder], default_result: dict):
        """Instantiate using a list of decoders.

        The list is converted to a mapping by method_id.
        """
        return cls(
            decoders={_.method_id: _ for _ in decoders},
            default_result=default_result,
        )

    def error(self, ex: Exception, data: str):
        return dict(
            self.default_result,
            decoding_status=str(ex) + "\n" + data,
            method_id=None,
        )

    def decode(self, data: str):
        try:
            method_id = MethodId(data[:10])
        except Exception as ex:
            return self.error(ex, data)

        # skip 2 for "0x"
        # skip 8 for method id
        stream = ContextFramesBytesIO(bytearray.fromhex(data[10:]))

        if method_id in self.decoders:
            return dict(
                decoding_status="ok",
                method_id=method_id,
                **self.decoders[method_id].decode(stream),
            )

        else:
            return dict(
                self.default_result,
                decoding_status="unsupported",
                method_id=method_id,
            )


def register_multi_method_decoder(
    ctx: DuckDBContext,
    duckdb_function_name: str,
    decoder: MultiMethodDecoder,
    parameters: list[str],
    return_type: str,
):
    """Register a DuckDB function to decode data from multiple method_ids"""

    def _decode(data: str):
        return decoder.decode(data)

    # NOTE: DuckDB does not support converting from python to UHUGEINT
    # https://github.com/duckdb/duckdb/blob/8e68a3e34aa526a342ae91e1b14b764bb3075a12/tools/pythonpkg/src/native/python_conversion.cpp#L325
    ctx.client.create_function(
        duckdb_function_name,
        _decode,
        type=PythonUDFType.NATIVE,
        parameters=[DuckDBPyType(_) for _ in parameters],
        return_type=DuckDBPyType(return_type),
    )
