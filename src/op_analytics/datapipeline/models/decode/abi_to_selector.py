from dataclasses import dataclass

from .abi_to_typestr import abi_entry_to_typestr


@dataclass(frozen=True, eq=True, order=True)
class EventSignature:
    signature: str
    keccak_hash: str

    @classmethod
    def of(cls, signature: str):
        from eth_utils_lite import keccak

        return cls(
            signature=signature,
            keccak_hash="0x" + keccak(text=signature).hex(),
        )

    @classmethod
    def from_event_abi(cls, abi_entry: dict) -> "EventSignature":
        item_type = abi_entry["type"]
        if item_type == "event":
            event_name = abi_entry["name"]
            event_params = ",".join(abi_entry_to_typestr(abi_entry, include_indexed=True))
            return cls.of(signature=f"{event_name}({event_params})")

        raise ValueError(f"abi_item is not an event: {item_type}")

    @classmethod
    def from_abi(cls, abi: list, event_name: str) -> "EventSignature":
        for item in abi:
            if item["type"] == "event" and item["name"] == event_name:
                return cls.from_event_abi(item)
        raise ValueError(f"Event {event_name} not found in ABI")


def get_all_log_selectors(abi_json):
    selectors = {}
    for item in abi_json:
        if item["type"] == "event":
            event_name = item["name"]
            selectors[event_name] = EventSignature.from_event_abi(item)

    if not selectors:
        return Exception("no selectors found in ABI")

    return selectors


@dataclass(frozen=True, eq=True, order=True)
class FunctionSignature:
    signature: str
    keccak_hash: str
    method_id: str

    @classmethod
    def from_abi(cls, abi: list, function_name: str) -> "FunctionSignature":
        from eth_utils_lite import keccak

        for entry in abi:
            if entry["type"] == "function" and entry["name"] == function_name:
                function_inputs = ",".join(abi_entry_to_typestr(entry))

                function_signature = f"{function_name}({function_inputs})"
                keccak_hash = "0x" + keccak(text=function_signature).hex()

                return cls(
                    signature=function_signature,
                    keccak_hash=keccak_hash,
                    method_id=keccak_hash[:10],
                )

        raise ValueError(f"Function {function_name} not found in ABI")
