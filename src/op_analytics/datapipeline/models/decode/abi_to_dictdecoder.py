from dataclasses import dataclass
from typing import Any

import orjson

from eth_abi_lite.decoding import (
    TupleDecoder,
    BaseArrayDecoder,
    BaseDecoder,
    ContextFramesBytesIO,
    DynamicArrayDecoder,
)


@dataclass(frozen=True, slots=True)
class DictionaryDecoder:
    """Decode binary data to a python dict."""

    decoder: TupleDecoder

    def decode_event(self, hexstr: str):
        # skip 2 for "0x"
        stream = ContextFramesBytesIO(bytearray.fromhex(hexstr[2:]))
        return self.decode(stream)

    def decode_event_as_json(self, hexstr: str):
        return orjson.dumps(self.decode_event(hexstr)).decode()

    def decode_function(self, hexstr: str):
        # skip 2 for "0x"
        # skip 8 for method id
        stream = ContextFramesBytesIO(bytearray.fromhex(hexstr[10:]))
        return self.decode(stream)

    def decode_function_as_json(self, hexstr: str) -> str:
        return orjson.dumps(self.decode_function(hexstr)).decode()

    def decode(self, stream):
        return dict(decoder(stream) for decoder in self.decoder.decoders)  # type: ignore

    @classmethod
    def of(cls, abi_entry: dict) -> "DictionaryDecoder":
        """Create a decoder instance for the provided ABI entry."""

        is_log = abi_entry.get("type") == "event"

        decoders = []
        for param in abi_entry["inputs"]:
            if is_log and param["indexed"]:
                continue
            decoder = abi_param_to_decoder(param)
            decoders.append(decoder)

        return cls(TupleDecoder(decoders=decoders))


@dataclass
class NamedDecoder:
    """Wraps a decoder keeping track of the associated field name."""

    # The path is not used at the moment, but could be useful if we
    # wanted to flatten out nested structs on a decoded output.
    field_path: list[str]

    field_name: str | None
    decoder: BaseDecoder

    def decode(self, stream: ContextFramesBytesIO) -> Any:
        if isinstance(self.decoder, TupleDecoder):
            return self.field_name, dict(_.decode(stream) for _ in self.decoder.decoders)  # type: ignore

        if isinstance(self.decoder, BaseArrayDecoder):
            # Ignore the dummy name that is used for the array item type.
            return self.field_name, [_[1] for _ in self.decoder.decode(stream)]

        return self.field_name, self.decoder.decode(stream)

    @property
    def is_dynamic(self) -> bool:
        return getattr(self.decoder, "is_dynamic", False)

    def validate(self):
        self.decoder.validate()

    def __call__(self, stream: ContextFramesBytesIO) -> Any:
        return self.decode(stream)

    @classmethod
    def from_type_str(cls, abi_type, registry):
        raise NotImplementedError("NamedDecoder must be instantiated directly")


def abi_param_to_decoder(param: dict, path: list[str] | None = None):
    """Convert a single parameter definition to its canonical type string representation."""

    path = path or []
    new_path = path + [param["name"]]

    def make_named(_dec):
        return NamedDecoder(
            field_path=new_path,
            field_name=param["name"],
            decoder=_dec,
        )

    # Handle structs (tuples in ABI)
    if param["type"] == "tuple":
        return make_named(
            TupleDecoder(
                decoders=[
                    abi_param_to_decoder(
                        param=comp,
                        path=new_path,
                    )
                    for comp in param.get("components", [])
                ]
            )
        )

    # Handle dynamic arrays of primitive types or structs
    elif param["type"].endswith("[]"):
        base_type = param["type"][:-2]

        item_decoder = abi_param_to_decoder(
            param=dict(param, type=base_type, name="ARRAY_ELEMENT"),
            path=new_path,
        )

        return make_named(DynamicArrayDecoder(item_decoder=item_decoder))

    # Handle primitive types
    else:
        return make_named(get_decoder(param["type"]))


def get_decoder(typestr):
    registry = custom_registry()
    return registry.get_decoder(typestr)


def custom_registry():
    """Custom decode classes.

    These classes convert interger and bytes values to strings to ensure the
    resulting types are lossless and supported on any database (BigQuery/DuckDB).
    """
    from eth_abi_lite import encoding, decoding
    from eth_abi_lite.registry import ABIRegistry, BaseEquals, has_arrlist, is_base_tuple

    class CustomUnsignedIntegerDecoder(decoding.UnsignedIntegerDecoder):
        def decoder_fn(self, data) -> str:  # type: ignore
            return str(super().decoder_fn(data))  # type: ignore

    class CustomSignedIntegerDecoder(decoding.SignedIntegerDecoder):
        def decoder_fn(self, data) -> str:  # type: ignore
            return str(super().decoder_fn(data))  # type: ignore

    class CustomBytesDecoder(decoding.BytesDecoder):
        @staticmethod
        def decoder_fn(data):
            return "0x" + data.hex()

    class CustomByteStringDecoder(decoding.ByteStringDecoder):
        @staticmethod
        def decoder_fn(data):
            return "0x" + data.hex()

    registry = ABIRegistry()

    registry.register(
        BaseEquals("uint"),
        encoding.UnsignedIntegerEncoder,
        CustomUnsignedIntegerDecoder,  # CUSTOM
        label="uint",
    )
    registry.register(
        BaseEquals("int"),
        encoding.SignedIntegerEncoder,
        CustomSignedIntegerDecoder,  # CUSTOM
        label="int",
    )
    registry.register(
        BaseEquals("address"),
        encoding.AddressEncoder,
        decoding.AddressDecoder,
        label="address",
    )
    registry.register(
        BaseEquals("bool"),
        encoding.BooleanEncoder,
        decoding.BooleanDecoder,
        label="bool",
    )
    registry.register(
        BaseEquals("ufixed"),
        encoding.UnsignedFixedEncoder,
        decoding.UnsignedFixedDecoder,
        label="ufixed",
    )
    registry.register(
        BaseEquals("fixed"),
        encoding.SignedFixedEncoder,
        decoding.SignedFixedDecoder,
        label="fixed",
    )
    registry.register(
        BaseEquals("bytes", with_sub=True),
        encoding.BytesEncoder,
        CustomBytesDecoder,  # CUSTOM
        label="bytes<M>",
    )
    registry.register(
        BaseEquals("bytes", with_sub=False),
        encoding.ByteStringEncoder,
        CustomByteStringDecoder,  # CUSTOM
        label="bytes",
    )
    registry.register(
        BaseEquals("function"),
        encoding.BytesEncoder,
        decoding.BytesDecoder,
        label="function",
    )
    registry.register(
        BaseEquals("string"),
        encoding.TextStringEncoder,
        decoding.StringDecoder,
        label="string",
    )
    registry.register(
        has_arrlist,
        encoding.BaseArrayEncoder,
        decoding.BaseArrayDecoder,
        label="has_arrlist",
    )
    registry.register(
        is_base_tuple,
        encoding.TupleEncoder,
        decoding.TupleDecoder,
        label="is_base_tuple",
    )

    return registry
