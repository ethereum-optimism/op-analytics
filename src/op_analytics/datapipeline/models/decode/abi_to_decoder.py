from eth_abi_lite.decoding import TupleDecoder
from eth_abi_lite.abi import default_codec

from .abi_to_typestr import abi_entry_to_typestr


def abi_inputs_to_decoder(abi_entry: dict) -> TupleDecoder:
    """Create a decoder instance for the provided ABI entry."""

    decoders = []
    for typestr in abi_entry_to_typestr(abi_entry):
        decoders.append(default_codec._registry.get_decoder(typestr))

    return TupleDecoder(decoders=decoders)
