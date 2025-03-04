from dataclasses import dataclass

from .abi_to_dictdecoder import DictionaryDecoder


@dataclass
class LogDecoder:
    """Decode data for a log."""

    # Decoder instance. Converts bytestream to python objects.
    decoder: DictionaryDecoder

    # Whether to decode as json string or python dict.
    as_json: bool

    def decode(self, hexstr: str):
        if self.as_json:
            return self.decoder.decode_event_as_json(hexstr)

        return self.decoder.decode_event(hexstr)
