from dataclasses import dataclass
from typing import Callable

from eth_abi_lite.decoding import ContextFramesBytesIO, TupleDecoder


@dataclass
class LogDecoder:
    """Decode data for a log."""

    # Decoder instance. Converts bytestream to python objects.
    decoder: TupleDecoder

    # Result converter. Extracts specific fields from the result
    # produced by the decoder.
    to_dict: Callable[[str], dict]

    def decode(self, data: str):
        # skip 2 for "0x"
        stream = ContextFramesBytesIO(bytearray.fromhex(data[2:]))
        result = self.decoder.decode(stream)
        return self.to_dict(result)
