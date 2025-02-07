from dataclasses import dataclass
from typing import Any, NewType, TypedDict, Callable

from .abi_to_dictdecoder import DictionaryDecoder


MethodId = NewType("MethodId", str)


class DecodingResult(TypedDict):
    method_id: str | None
    decoded: dict | None
    decode_error: str | None


@dataclass
class SingleMethodDecoder:
    """Decode data for a single method."""

    # Method ID associated with this decoder (hex sring).
    method_id: str

    # Decoder instance. Converts bytestream to python ditionary.
    decoder: DictionaryDecoder

    def decode(self, hexstr: str, as_json: bool):
        if as_json:
            return self.decoder.decode_function_as_json(hexstr)
        return self.decoder.decode_function(hexstr)


@dataclass
class MultiMethodDecoder:
    """Dispatch to a SingleMethodDecoder based on method_id."""

    decoders: dict[MethodId, SingleMethodDecoder]
    as_json: bool
    adapter: Callable[[DecodingResult], Any] | None

    @classmethod
    def of(
        cls,
        decoders: list[SingleMethodDecoder],
        as_json: bool,
        adapter: Callable[[DecodingResult], Any] | None = None,
    ):
        """Instantiate using a list of decoders.

        The list is converted to a mapping by method_id.
        """
        return cls(
            decoders={_.method_id: _ for _ in decoders},
            as_json=as_json,
            adapter=adapter,
        )

    def decode(self, data: str) -> Any:
        result = self._decode(data)
        if self.adapter is None:
            return result
        return self.adapter(result)

    def _decode(self, data: str) -> DecodingResult:
        try:
            method_id = MethodId(data[:10])
        except Exception as ex:
            return dict(
                method_id=None,
                decoded=None,
                decode_error=str(ex) + "\n" + data,
            )

        if method_id in self.decoders:
            return dict(
                method_id=method_id,
                decoded=self.decoders[method_id].decode(data, self.as_json),
                decode_error=None,
            )

        else:
            return dict(
                method_id=method_id,
                decoded=None,
                decode_error=None,
            )
