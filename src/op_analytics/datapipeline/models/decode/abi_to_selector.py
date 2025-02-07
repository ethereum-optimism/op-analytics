from dataclasses import dataclass

from .abi_to_typestr import abi_inputs_to_typestr


def get_function_selector(abi_json, function_name):
    from eth_utils_lite import keccak

    for item in abi_json:
        if item["type"] == "function" and item["name"] == function_name:
            function_inputs = ",".join(abi_inputs_to_typestr(item))

            function_signature = f"{function_name}({function_inputs})"

            return "0x" + keccak(text=function_signature).hex()[:8]

    raise ValueError(f"Function {function_name} not found in ABI")


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
    def from_abi(cls, abi_item: dict):
        item_type = abi_item["type"]
        if item_type == "event":
            event_name = abi_item["name"]
            event_params = ",".join(abi_inputs_to_typestr(abi_item, include_indexed=True))
            return cls.of(signature=f"{event_name}({event_params})")

        raise ValueError(f"abi_item is not an event: {item_type}")


def get_log_selector(abi_json, event_name):
    for item in abi_json:
        if item["type"] == "event" and item["name"] == event_name:
            return EventSignature.from_abi(item)
    raise ValueError(f"Event {event_name} not found in ABI")


def get_all_log_selectors(abi_json):
    selectors = {}
    for item in abi_json:
        if item["type"] == "event":
            event_name = item["name"]
            selectors[event_name] = EventSignature.from_abi(item)

    if not selectors:
        return Exception("no selectors found in ABI")

    return selectors
