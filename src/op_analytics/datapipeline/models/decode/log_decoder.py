from dataclasses import dataclass

from .abi_to_dictdecoder import DictionaryDecoder


@dataclass
class LogDecoder:
    """Decode data for a log."""

    # Decoder instance. Converts bytestream to python objects.
    decoder: DictionaryDecoder

    def decode(self, data: str):
        # NOTE: We only support as_json for now, but if more flexibility is needed
        # we can make "as_json" a parameter and have an optional "adapter" callable
        # to manipulate the result, as we do in method_decoder.py.
        return self.decoder.decode_event_as_json(data)
