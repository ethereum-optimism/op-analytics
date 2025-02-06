from dataclasses import dataclass

from .abi_to_dictdecoder import DictionaryDecoder


@dataclass
class LogDecoder:
    """Decode data for a log."""

    # Decoder instance. Converts bytestream to python objects.
    decoder: DictionaryDecoder

    def decode(self, data: str):
        return self.decoder.decode_event_as_json(data)
