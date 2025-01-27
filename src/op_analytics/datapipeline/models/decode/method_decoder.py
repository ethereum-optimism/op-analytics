from dataclasses import dataclass
from typing import Callable, NewType

from eth_abi_lite.decoding import ContextFramesBytesIO, TupleDecoder


MethodId = NewType("MethodId", str)


@dataclass
class SingleMethodDecoder:
    """Decode data for a single method."""

    # Method ID associated with this decoder (hex sring).
    method_id: str

    # Decoder instance. Converts bytestream to python objects.
    decoder: TupleDecoder

    # Result converter. Extracts specific fields from the result
    # produced by the decoder.
    to_dict: Callable[[str], dict]

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
