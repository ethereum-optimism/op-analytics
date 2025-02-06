from dataclasses import dataclass
from typing import NewType, TypedDict

from .abi_to_dictdecoder import DictionaryDecoder


MethodId = NewType("MethodId", str)


class DecodingResult(TypedDict):
    method_id: str
    decoded_json: str
    error: str


@dataclass
class SingleMethodDecoder:
    """Decode data for a single method."""

    # Method ID associated with this decoder (hex sring).
    method_id: str

    # Decoder instance. Converts bytestream to python ditionary.
    decoder: DictionaryDecoder

    def decode_as_json(self, hexstr: str):
        return self.decoder.decode_function_as_json(hexstr)


@dataclass
class MultiMethodDecoder:
    """Dispatch to a SingleMethodDecoder based on method_id."""

    decoders: dict[MethodId, SingleMethodDecoder]

    @classmethod
    def of(cls, decoders: list[SingleMethodDecoder]):
        """Instantiate using a list of decoders.

        The list is converted to a mapping by method_id.
        """
        return cls(
            decoders={_.method_id: _ for _ in decoders},
        )

    def decode(self, data: str) -> DecodingResult:
        try:
            method_id = MethodId(data[:10])
        except Exception as ex:
            return dict(
                method_id=None,
                decoded_json=None,
                error=str(ex) + "\n" + data,
            )

        if method_id in self.decoders:
            return dict(
                method_id=method_id,
                decoded_json=self.decoders[method_id].decode_as_json(data),
                error=None,
            )

        else:
            return dict(
                method_id=method_id,
                decoded_json=None,
                error=None,
            )
