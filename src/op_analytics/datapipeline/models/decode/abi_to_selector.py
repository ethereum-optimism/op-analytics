from .abi_to_typestr import abi_inputs_to_typestr


def get_function_selector(abi_json, function_name):
    from eth_utils_lite import keccak

    for item in abi_json:
        if item["type"] == "function" and item["name"] == function_name:
            function_inputs = ",".join(abi_inputs_to_typestr(item))

            function_signature = f"{function_name}({function_inputs})"

            return "0x" + keccak(text=function_signature).hex()[:8]

    raise ValueError(f"Function {function_name} not found in ABI")


def get_log_selector(abi_json, event_name):
    from eth_utils_lite import keccak

    for item in abi_json:
        if item["type"] == "event" and item["name"] == event_name:
            event_params = ",".join(abi_inputs_to_typestr(item, include_indexed=True))
            event_signature = f"{event_name}({event_params})"
            return "0x" + keccak(text=event_signature).hex()

    raise ValueError(f"Event {event_name} not found in ABI")


def get_all_log_selectors(abi_json):
    from eth_utils_lite import keccak

    selectors = {}
    for item in abi_json:
        if item["type"] == "event":
            event_name = item["name"]
            event_params = ",".join(abi_inputs_to_typestr(item, include_indexed=True))
            event_signature = f"{event_name}({event_params})"
            selectors[event_name] = "0x" + keccak(text=event_signature).hex()

    if not selectors:
        return Exception("no selectors found in ABI")

    return selectors
